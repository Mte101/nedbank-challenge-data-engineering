"""
Writes /data/output/dq_report.json summarising all DQ outcomes for the pipeline run.
"""

import json
import os
from datetime import datetime


def run_dq_report(
    dq_counts: dict,
    gold_counts: dict,
    config: dict,
    run_start: datetime,
    run_end: datetime,
) -> None:
    tx_raw  = dq_counts["transactions_raw"]
    acc_raw = dq_counts["accounts_raw"]

    issues = []

    if dq_counts["duplicate_count"] > 0:
        issues.append({
            "issue_type": "duplicate_transactions",
            "records_affected": dq_counts["duplicate_count"],
            "percentage_of_total": round(dq_counts["duplicate_count"] / tx_raw * 100, 2),
            "handling_action": "DEDUPLICATED_KEEP_FIRST",
            "records_in_output": 0,
        })

    if dq_counts["orphaned_count"] > 0:
        issues.append({
            "issue_type": "orphaned_transactions",
            "records_affected": dq_counts["orphaned_count"],
            "percentage_of_total": round(dq_counts["orphaned_count"] / tx_raw * 100, 2),
            "handling_action": "QUARANTINED",
            "records_in_output": 0,
        })

    if dq_counts["type_mismatch_count"] > 0:
        issues.append({
            "issue_type": "amount_type_mismatch",
            "records_affected": dq_counts["type_mismatch_count"],
            "percentage_of_total": round(dq_counts["type_mismatch_count"] / tx_raw * 100, 2),
            "handling_action": "CAST_TO_DECIMAL",
            "records_in_output": dq_counts["type_mismatch_in_output"],
        })

    if dq_counts["date_format_count"] > 0:
        issues.append({
            "issue_type": "date_format_inconsistency",
            "records_affected": dq_counts["date_format_count"],
            "percentage_of_total": round(dq_counts["date_format_count"] / tx_raw * 100, 2),
            "handling_action": "NORMALISED_DATE",
            "records_in_output": dq_counts["date_format_in_output"],
        })

    if dq_counts["currency_variant_count"] > 0:
        issues.append({
            "issue_type": "currency_variants",
            "records_affected": dq_counts["currency_variant_count"],
            "percentage_of_total": round(dq_counts["currency_variant_count"] / tx_raw * 100, 2),
            "handling_action": "NORMALISED_CURRENCY",
            "records_in_output": dq_counts["currency_variant_in_output"],
        })

    if dq_counts["null_pk_count"] > 0:
        issues.append({
            "issue_type": "null_account_id",
            "records_affected": dq_counts["null_pk_count"],
            "percentage_of_total": round(dq_counts["null_pk_count"] / acc_raw * 100, 2),
            "handling_action": "EXCLUDED_NULL_PK",
            "records_in_output": 0,
        })

    report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": run_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stage": "2",
        "source_record_counts": {
            "accounts_raw":     dq_counts["accounts_raw"],
            "transactions_raw": dq_counts["transactions_raw"],
            "customers_raw":    dq_counts["customers_raw"],
        },
        "dq_issues": issues,
        "gold_layer_record_counts": gold_counts,
        "execution_duration_seconds": int((run_end - run_start).total_seconds()),
    }

    path = config["output"]["dq_report_path"]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
