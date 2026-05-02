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

import gc
import logging

import yaml
from deltalake import DeltaTable

from pipeline.utils import get_connection, load_config, parquet_expr, profile_stage, read_delta, write_delta

_log = logging.getLogger(__name__)

# Multi-format date parse expression (accounts/customers — plain string, not f-string).
# Handles YYYY-MM-DD, DD/MM/YYYY, and Unix epoch seconds (including negative for pre-1970).
_DATE_PARSE_SQL = """
    CASE
        WHEN regexp_matches(CAST({col} AS VARCHAR), '^\d{{4}}-\d{{2}}-\d{{2}}$')
            THEN CAST(CAST({col} AS VARCHAR) AS DATE)
        WHEN regexp_matches(CAST({col} AS VARCHAR), '^\d{{2}}/\d{{2}}/\d{{4}}$')
            THEN STRPTIME(CAST({col} AS VARCHAR), '%d/%m/%Y')::DATE
        WHEN regexp_matches(CAST({col} AS VARCHAR), '^-?\d+$')
            THEN epoch_ms(CAST(CAST({col} AS VARCHAR) AS BIGINT) * 1000)::DATE
        ELSE NULL
    END
"""


def _date_parse(col: str) -> str:
    return _DATE_PARSE_SQL.format(col=col)


def _load_dq_rules(config: dict) -> list:
    rules_path = config.get("dq", {}).get("rules_path")
    if not rules_path:
        return []
    try:
        with open(rules_path) as f:
            return yaml.safe_load(f).get("rules", [])
    except FileNotFoundError:
        _log.warning("dq_rules.yaml not found at %s; continuing with SQL-embedded rules", rules_path)
        return []


def run_transformation() -> dict:
    config = load_config()
    bronze = config["output"]["bronze_path"]
    silver = config["output"]["silver_path"]

    dq_rules = _load_dq_rules(config)
    _log.info("Loaded %d DQ rules from config", len(dq_rules))

    con = get_connection()

    # ── Accounts ────────────────────────────────────────────────────────────
    with profile_stage("transform.accounts"):
        bronze_accounts = read_delta(f"{bronze}/accounts")
        con.register("bronze_accounts", bronze_accounts)

        acc_pre = con.execute("""
            SELECT
                COUNT(*)                                                              AS total_raw,
                SUM(CASE WHEN account_id IS NULL OR account_id = '' THEN 1 ELSE 0 END) AS null_pk_count,
                SUM(CASE WHEN open_date IS NOT NULL
                          AND NOT regexp_matches(CAST(open_date AS VARCHAR),
                                                 '^\d{4}-\d{2}-\d{2}$')
                         THEN 1 ELSE 0 END)                                           AS date_format_count
            FROM bronze_accounts
        """).fetchone()
        accounts_raw, null_pk_count, acc_date_format_count = (
            int(acc_pre[0]), int(acc_pre[1]), int(acc_pre[2])
        )

        write_delta(
            con.execute(f"""
                WITH raw AS (
                    SELECT *,
                        {_date_parse('open_date')}            AS _parsed_open_date,
                        {_date_parse('last_activity_date')}   AS _parsed_last_activity
                    FROM bronze_accounts
                    WHERE account_id IS NOT NULL AND account_id != ''
                )
                SELECT
                    account_id,
                    customer_ref,
                    account_type,
                    account_status,
                    _parsed_open_date                         AS open_date,
                    product_tier,
                    mobile_number,
                    digital_channel,
                    CAST(credit_limit AS DECIMAL(18,2))       AS credit_limit,
                    CAST(current_balance AS DECIMAL(18,2))    AS current_balance,
                    _parsed_last_activity                     AS last_activity_date,
                    ingestion_timestamp
                FROM raw
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY account_id ORDER BY _parsed_open_date
                ) = 1
            """).fetch_record_batch(300_000),
            f"{silver}/accounts",
        )
        con.unregister("bronze_accounts")

    # ── Customers ────────────────────────────────────────────────────────────
    with profile_stage("transform.customers"):
        bronze_customers = read_delta(f"{bronze}/customers")
        con.register("bronze_customers", bronze_customers)

        cust_pre = con.execute("""
            SELECT
                COUNT(*)                                                              AS total_raw,
                SUM(CASE WHEN dob IS NOT NULL
                          AND NOT regexp_matches(CAST(dob AS VARCHAR),
                                                 '^\d{4}-\d{2}-\d{2}$')
                         THEN 1 ELSE 0 END)                                           AS date_format_count
            FROM bronze_customers
        """).fetchone()
        customers_raw, cust_date_format_count = int(cust_pre[0]), int(cust_pre[1])

        write_delta(
            con.execute(f"""
                WITH raw AS (
                    SELECT *,
                        {_date_parse('dob')} AS _parsed_dob
                    FROM bronze_customers
                    WHERE customer_id IS NOT NULL
                )
                SELECT
                    customer_id,
                    id_number,
                    first_name,
                    last_name,
                    _parsed_dob                        AS dob,
                    gender,
                    province,
                    income_band,
                    segment,
                    CAST(risk_score AS INTEGER)        AS risk_score,
                    kyc_status,
                    product_flags,
                    ingestion_timestamp
                FROM raw
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY customer_id ORDER BY _parsed_dob
                ) = 1
            """).fetch_record_batch(300_000),
            f"{silver}/customers",
        )
        con.unregister("bronze_customers")

    # Release DuckDB buffers accumulated during accounts/customers before the
    # much larger transactions stage — prevents OOM on the 2 GB container.
    con.close()
    gc.collect()
    con = get_connection()

    # ── Transactions ─────────────────────────────────────────────────────────
    with profile_stage("transform.transactions"):
        # Build native read_parquet() expressions — avoids con.register() which
        # materialises the full dataset into DuckDB's buffer pool before any query.
        bronze_tx  = parquet_expr(f"{bronze}/transactions")
        silver_acc = parquet_expr(f"{silver}/accounts")

        # Stage 1/2 compatibility: read column names from Delta metadata (no data scan)
        bronze_col_names = DeltaTable(f"{bronze}/transactions").schema().to_pyarrow().names
        merch_sub = (
            "t.merchant_subcategory"
            if "merchant_subcategory" in bronze_col_names
            else "CAST(NULL AS VARCHAR)"
        )

        # Pre-compute DQ stats, bucketed on the same hash partition used for
        # transforms. COUNT(DISTINCT) per bucket hashes ~total/N_BUCKETS IDs
        # (~14 MB each) instead of the full ~113 MB in one pass; exact count.
        N_BUCKETS = 8
        tx_total_raw         = 0
        tx_unique            = 0
        type_mismatch_raw    = 0
        date_format_raw      = 0
        currency_variant_raw = 0
        for _b in range(N_BUCKETS):
            _pre = con.execute(f"""
                SELECT
                    COUNT(*)                                                              AS total_raw,
                    COUNT(DISTINCT transaction_id)                                        AS unique_count,
                    SUM(CASE WHEN _amount_was_string THEN 1 ELSE 0 END)                   AS type_mismatch_raw,
                    SUM(CASE WHEN transaction_date IS NOT NULL
                              AND NOT regexp_matches(CAST(transaction_date AS VARCHAR),
                                                     '^\d{{4}}-\d{{2}}-\d{{2}}$')
                             THEN 1 ELSE 0 END)                                           AS date_format_raw,
                    SUM(CASE WHEN UPPER(CAST(currency AS VARCHAR)) IN ('ZAR','R','RANDS','710')
                              AND CAST(currency AS VARCHAR) != 'ZAR'
                             THEN 1 ELSE 0 END)                                           AS currency_variant_raw
                FROM {bronze_tx}
                WHERE hash(transaction_id) % {N_BUCKETS} = {_b}
            """).fetchone()
            tx_total_raw         += int(_pre[0])
            tx_unique            += int(_pre[1])
            type_mismatch_raw    += int(_pre[2])
            date_format_raw      += int(_pre[3])
            currency_variant_raw += int(_pre[4])

        for bucket in range(N_BUCKETS):
            chunk = con.execute(f"""
                WITH raw_bucket AS (
                    SELECT *,
                        CASE
                            WHEN regexp_matches(CAST(transaction_date AS VARCHAR),
                                                '^\d{{4}}-\d{{2}}-\d{{2}}$')
                                THEN CAST(CAST(transaction_date AS VARCHAR) AS DATE)
                            WHEN regexp_matches(CAST(transaction_date AS VARCHAR),
                                                '^\d{{2}}/\d{{2}}/\d{{4}}$')
                                THEN STRPTIME(CAST(transaction_date AS VARCHAR), '%d/%m/%Y')::DATE
                            WHEN regexp_matches(CAST(transaction_date AS VARCHAR), '^-?\d+$')
                                THEN epoch_ms(
                                        CAST(CAST(transaction_date AS VARCHAR) AS BIGINT) * 1000
                                     )::DATE
                            ELSE NULL
                        END AS _parsed_date
                    FROM {bronze_tx}
                    WHERE hash(transaction_id) % {N_BUCKETS} = {bucket}
                ),
                bucket AS MATERIALIZED (
                    SELECT * FROM raw_bucket
                    WHERE transaction_id IS NOT NULL AND account_id IS NOT NULL
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY transaction_id
                        ORDER BY _parsed_date, transaction_time
                    ) = 1
                )
                SELECT
                    t.transaction_id,
                    t.account_id,
                    t._parsed_date                                                        AS transaction_date,
                    t.transaction_time,
                    STRPTIME(
                        STRFTIME(t._parsed_date, '%Y-%m-%d') || ' ' || t.transaction_time,
                        '%Y-%m-%d %H:%M:%S'
                    )                                                                     AS transaction_timestamp,
                    t.transaction_type,
                    t.merchant_category,
                    {merch_sub}                                                           AS merchant_subcategory,
                    TRY_CAST(
                        regexp_replace(CAST(t.amount AS VARCHAR), '^[^0-9.-]+', '')
                        AS DECIMAL(18,2)
                    )                                                                     AS amount,
                    CASE
                        WHEN UPPER(CAST(t.currency AS VARCHAR)) IN ('ZAR','R','RANDS','710')
                            THEN 'ZAR'
                        ELSE CAST(t.currency AS VARCHAR)
                    END                                                                   AS currency,
                    t.channel,
                    t.location.province                                                   AS province,
                    CASE
                        WHEN t._amount_was_string
                            THEN 'TYPE_MISMATCH'
                        WHEN NOT regexp_matches(CAST(t.transaction_date AS VARCHAR),
                                                '^\d{{4}}-\d{{2}}-\d{{2}}$')
                            THEN 'DATE_FORMAT'
                        WHEN UPPER(CAST(t.currency AS VARCHAR)) IN ('ZAR','R','RANDS','710')
                             AND CAST(t.currency AS VARCHAR) != 'ZAR'
                            THEN 'CURRENCY_VARIANT'
                        ELSE NULL
                    END                                                                   AS dq_flag,
                    t.ingestion_timestamp
                FROM bucket t
                INNER JOIN {silver_acc} a ON t.account_id = a.account_id
            """).fetch_record_batch(300_000)

            write_delta(
                chunk,
                f"{silver}/transactions",
                mode="overwrite" if bucket == 0 else "append",
            )

        # Post-silver counts via native read_parquet (same reason — avoid materialisation)
        silver_tx = parquet_expr(f"{silver}/transactions")
        post = con.execute(f"""
            SELECT
                COUNT(*)                                                              AS total,
                SUM(CASE WHEN dq_flag = 'TYPE_MISMATCH'    THEN 1 ELSE 0 END)        AS type_mismatch_out,
                SUM(CASE WHEN dq_flag = 'DATE_FORMAT'      THEN 1 ELSE 0 END)        AS date_format_out,
                SUM(CASE WHEN dq_flag = 'CURRENCY_VARIANT' THEN 1 ELSE 0 END)        AS currency_variant_out
            FROM {silver_tx}
        """).fetchone()

        silver_tx_total      = int(post[0])
        type_mismatch_out    = int(post[1])
        date_format_out      = int(post[2])
        currency_variant_out = int(post[3])

        # Orphaned = unique deduped valid transactions that failed the INNER JOIN
        orphaned_count  = tx_unique - silver_tx_total
        duplicate_count = tx_total_raw - tx_unique

    return {
        "accounts_raw":           accounts_raw,
        "transactions_raw":       tx_total_raw,
        "customers_raw":          customers_raw,
        "duplicate_count":        duplicate_count,
        "orphaned_count":         orphaned_count,
        "type_mismatch_count":    type_mismatch_raw,
        "date_format_count":      date_format_raw + acc_date_format_count + cust_date_format_count,
        "currency_variant_count": currency_variant_raw,
        "null_pk_count":          null_pk_count,
        "type_mismatch_in_output":    type_mismatch_out,
        "date_format_in_output":      date_format_out,
        "currency_variant_in_output": currency_variant_out,
    }
