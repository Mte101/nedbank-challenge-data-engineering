"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""

from pipeline.utils import get_connection, load_config, parquet_expr, profile_stage, read_delta, write_delta


def _count_delta(con, path: str) -> int:
    expr = parquet_expr(path)
    return int(con.execute(f"SELECT COUNT(*) FROM {expr}").fetchone()[0])


def run_provisioning() -> dict:
    config = load_config()
    silver = config["output"]["silver_path"]
    gold = config["output"]["gold_path"]

    con = get_connection()
    gold_counts = {}

    # ── dim_customers (9 fields) ─────────────────────────────────────────────
    # Built first — needed for FK resolution in fact_transactions.
    with profile_stage("provision.dim_customers"):
        silver_customers = read_delta(f"{silver}/customers")
        con.register("silver_customers", silver_customers)

        write_delta(
            con.execute("""
                SELECT
                    CAST((hash(customer_id) & 9223372036854775807) AS BIGINT)      AS customer_sk,
                    customer_id,
                    gender,
                    province,
                    income_band,
                    segment,
                    risk_score,
                    kyc_status,
                    CASE
                        WHEN CAST(DATEDIFF('day', dob, CURRENT_DATE) / 365.25 AS INTEGER) >= 65
                            THEN '65+'
                        WHEN CAST(DATEDIFF('day', dob, CURRENT_DATE) / 365.25 AS INTEGER) >= 56
                            THEN '56-65'
                        WHEN CAST(DATEDIFF('day', dob, CURRENT_DATE) / 365.25 AS INTEGER) >= 46
                            THEN '46-55'
                        WHEN CAST(DATEDIFF('day', dob, CURRENT_DATE) / 365.25 AS INTEGER) >= 36
                            THEN '36-45'
                        WHEN CAST(DATEDIFF('day', dob, CURRENT_DATE) / 365.25 AS INTEGER) >= 26
                            THEN '26-35'
                        WHEN CAST(DATEDIFF('day', dob, CURRENT_DATE) / 365.25 AS INTEGER) >= 18
                            THEN '18-25'
                        ELSE NULL
                    END                                        AS age_band
                FROM silver_customers
            """).fetch_record_batch(300_000),
            f"{gold}/dim_customers",
        )
        con.unregister("silver_customers")
        gold_counts["dim_customers"] = _count_delta(con, f"{gold}/dim_customers")

    # ── dim_accounts (11 fields, customer_id at position 3) ─────────────────
    with profile_stage("provision.dim_accounts"):
        silver_accounts = read_delta(f"{silver}/accounts")
        con.register("silver_accounts", silver_accounts)

        write_delta(
            con.execute("""
                SELECT
                    CAST((hash(account_id) & 9223372036854775807) AS BIGINT)      AS account_sk,
                    account_id,
                    customer_ref                              AS customer_id,
                    account_type,
                    account_status,
                    open_date,
                    product_tier,
                    digital_channel,
                    credit_limit,
                    current_balance,
                    last_activity_date
                FROM silver_accounts
            """).fetch_record_batch(300_000),
            f"{gold}/dim_accounts",
        )
        con.unregister("silver_accounts")
        gold_counts["dim_accounts"] = _count_delta(con, f"{gold}/dim_accounts")

    # ── fact_transactions (15 fields) ────────────────────────────────────────
    with profile_stage("provision.fact_transactions"):
        dim_accounts = read_delta(f"{gold}/dim_accounts")
        con.register("dim_accounts", dim_accounts)

        dim_customers = read_delta(f"{gold}/dim_customers")
        con.register("dim_customers", dim_customers)

        con.execute("""
            CREATE OR REPLACE VIEW account_customer AS
            SELECT a.account_sk, a.account_id, c.customer_sk
            FROM dim_accounts a
            JOIN dim_customers c ON a.customer_id = c.customer_id
        """)

        silver_tx = parquet_expr(f"{silver}/transactions")

        fact_transactions = con.execute(f"""
            SELECT
                CAST((hash(t.transaction_id) & 9223372036854775807) AS BIGINT)      AS transaction_sk,
                t.transaction_id,
                ac.account_sk,
                ac.customer_sk,
                t.transaction_date,
                t.transaction_timestamp,
                t.transaction_type,
                t.merchant_category,
                t.merchant_subcategory,
                t.amount,
                t.currency,
                t.channel,
                t.province,
                t.dq_flag,
                t.ingestion_timestamp
            FROM {silver_tx} t
            LEFT JOIN account_customer ac ON t.account_id = ac.account_id
        """).fetch_record_batch(500_000)

        write_delta(fact_transactions, f"{gold}/fact_transactions")
        con.execute("DROP VIEW IF EXISTS account_customer")
        con.unregister("dim_accounts")
        con.unregister("dim_customers")
        gold_counts["fact_transactions"] = _count_delta(con, f"{gold}/fact_transactions")

    return gold_counts
