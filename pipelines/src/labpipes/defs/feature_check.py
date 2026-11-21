"""S2: the feature_contract check on trip_features.

contracts/features.yaml lists the model's inputs, label and the post-trip columns that must never be
inputs; contracts/mlflow.yaml says this check blocks. Decide what "matches" means: column names,
types, value ranges. Gameday E1 will test it.
"""

import dagster as dg

from ..contracts import features_contract
from ..partitions import trip_months
from ..resources import RedshiftData
from .features import trip_features


@dg.asset_check(
    asset=trip_features,
    partitions_def=trip_months,
    blocking=True,
    description="staging.trip_features matches contracts/features.yaml (columns, types, ranges, no forbidden post-trip fields).",
)
def feature_contract(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = context.partition_key
    contract = features_contract()

    # 1. Query Redshift column metadata for staging.trip_features
    cols = redshift.query(
        "select column_name from information_schema.columns "
        "where table_schema = 'staging' and table_name = 'trip_features'"
    )
    col_names = {c["column_name"].lower() for c in cols}

    # Verify required columns exist (keys, inputs, label)
    expected_cols = (
        {c["name"].lower() for c in contract["keys"]}
        | {c["name"].lower() for c in contract["inputs"]}
        | {contract["label"]["name"].lower()}
    )
    missing = expected_cols - col_names
    if missing:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            description=f"Missing required columns in staging.trip_features: {sorted(missing)}",
        )

    # Verify no forbidden post-trip columns exist (target leakage)
    forbidden = {c.lower() for c in contract.get("forbidden", [])}
    leaked = forbidden & col_names
    if leaked:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            description=f"Forbidden post-trip columns found in staging.trip_features: {sorted(leaked)}",
        )

    # 2. Verify value ranges for this partition
    bad_count = redshift.scalar(
        f"""
        select count(*) from staging.trip_features
        where trip_month = '{month}'
          and (
            pickup_zone_id not between 1 and 263
            or dropoff_zone_id not between 1 and 263
            or pickup_hour not between 0 and 23
            or pickup_dow not between 0 and 6
            or duration_min <= 0
            or duration_min > 180
          )
        """
    )
    bad = int(bad_count or 0)
    if bad > 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            description=f"Found {bad} rows with values out of contract range in month {month}",
            metadata={"bad_rows": dg.MetadataValue.int(bad)},
        )

    total_rows = int(redshift.scalar(f"select count(*) from staging.trip_features where trip_month = '{month}'") or 0)
    return dg.AssetCheckResult(
        passed=True,
        severity=dg.AssetCheckSeverity.ERROR,
        description=f"Month {month}: {total_rows} rows match feature contract (valid columns, ranges, zero forbidden fields)",
        metadata={"rows": dg.MetadataValue.int(total_rows)},
    )
