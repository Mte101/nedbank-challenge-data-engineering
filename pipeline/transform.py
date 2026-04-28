"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - At Stage 2, load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §8 for the full list of DQ flag values and their
definitions.
"""

from pipeline.utils import get_connection, load_config, profile_stage, read_delta, write_delta


def run_transformation():
    config = load_config()
    bronze = config["output"]["bronze_path"]
    silver = config["output"]["silver_path"]

    con = get_connection()

    # ── Accounts ────────────────────────────────────────────────────────────
    with profile_stage("transform.accounts"):
        bronze_accounts = read_delta(f"{bronze}/accounts")
        con.register("bronze_accounts", bronze_accounts)

        write_delta(
            con.execute("""
                SELECT
                    account_id,
                    customer_ref,
                    account_type,
                    account_status,
                    CAST(open_date AS DATE)              AS open_date,
                    product_tier,
                    mobile_number,
                    digital_channel,
                    CAST(credit_limit AS DECIMAL(18,2))  AS credit_limit,
                    CAST(current_balance AS DECIMAL(18,2)) AS current_balance,
                    CAST(last_activity_date AS DATE)     AS last_activity_date,
                    ingestion_timestamp
                FROM bronze_accounts
                WHERE account_id IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY account_id ORDER BY open_date
                ) = 1
            """).fetch_record_batch(300_000),
            f"{silver}/accounts",
        )
        con.unregister("bronze_accounts")

    # ── Customers ────────────────────────────────────────────────────────────
    with profile_stage("transform.customers"):
        bronze_customers = read_delta(f"{bronze}/customers")
        con.register("bronze_customers", bronze_customers)

        write_delta(
            con.execute("""
                SELECT
                    customer_id,
                    id_number,
                    first_name,
                    last_name,
                    CAST(dob AS DATE)        AS dob,
                    gender,
                    province,
                    income_band,
                    segment,
                    CAST(risk_score AS INTEGER) AS risk_score,
                    kyc_status,
                    product_flags,
                    ingestion_timestamp
                FROM bronze_customers
                WHERE customer_id IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY customer_id ORDER BY dob
                ) = 1
            """).fetch_record_batch(300_000),
            f"{silver}/customers",
        )
        con.unregister("bronze_customers")

    # ── Transactions ─────────────────────────────────────────────────────────
    with profile_stage("transform.transactions"):
        # Read silver_accounts from Delta — no Python Arrow copy in memory
        silver_accounts = read_delta(f"{silver}/accounts")
        con.register("silver_accounts", silver_accounts)

        bronze_transactions = read_delta(f"{bronze}/transactions")
        con.register("bronze_transactions", bronze_transactions)

        # Process in hash buckets to bound peak memory.
        # hash(transaction_id) is deterministic for equal values on a typed
        # Delta column, so all copies of a duplicate always land in the same
        # bucket — the QUALIFY dedup inside each bucket is globally correct.
        N_BUCKETS = 4
        for bucket in range(N_BUCKETS):
            chunk = con.execute(f"""
                WITH bucket AS MATERIALIZED (
                    SELECT *
                    FROM bronze_transactions
                    WHERE hash(transaction_id) % {N_BUCKETS} = {bucket}
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY transaction_id
                        ORDER BY transaction_date, transaction_time
                    ) = 1
                )
                SELECT
                    t.transaction_id,
                    t.account_id,
                    CAST(t.transaction_date AS DATE)                         AS transaction_date,
                    t.transaction_time,
                    STRPTIME(
                        STRFTIME(CAST(t.transaction_date AS DATE), '%Y-%m-%d')
                        || ' ' || t.transaction_time,
                        '%Y-%m-%d %H:%M:%S'
                    )                                                        AS transaction_timestamp,
                    t.transaction_type,
                    t.merchant_category,
                    CAST(t.amount AS DECIMAL(18,2))                          AS amount,
                    CASE
                        WHEN UPPER(CAST(t.currency AS VARCHAR))
                             IN ('ZAR', 'R', 'RANDS', '710')
                        THEN 'ZAR'
                        ELSE CAST(t.currency AS VARCHAR)
                    END                                                       AS currency,
                    t.channel,
                    t.location.province                                       AS province,
                    CASE
                        WHEN t.transaction_id IS NULL
                          OR t.account_id IS NULL     THEN 'NULL_REQUIRED'
                        WHEN a.account_id IS NULL      THEN 'ORPHANED_ACCOUNT'
                        WHEN UPPER(CAST(t.currency AS VARCHAR))
                             NOT IN ('ZAR', 'R', 'RANDS', '710')
                         AND t.currency IS NOT NULL    THEN 'CURRENCY_VARIANT'
                        ELSE NULL
                    END                                                       AS dq_flag,
                    t.ingestion_timestamp
                FROM bucket t
                LEFT JOIN silver_accounts a ON t.account_id = a.account_id
            """).fetch_record_batch(300_000)

            write_delta(chunk, f"{silver}/transactions",
                        mode="overwrite" if bucket == 0 else "append")
        con.unregister("bronze_transactions")
        con.unregister("silver_accounts")
