"""S1: the schema_contract check on trip_files.

Compare the landed file's Parquet footer (the DataBucket resource reads it) with
contracts/trips.schema.yaml, before raw_trips copies anything. contracts/platform.yaml says this check
is blocking. Patterns: pipelines/reference/patterns.py.
"""

import dagster as dg

from ..contracts import trips_schema
from ..partitions import trip_months
from ..resources import DataBucket
from .trip_files import trip_files


@dg.asset_check(
    asset=trip_files,
    name="schema_contract",
    partitions_def=trip_months,
    blocking=True,
    description="Compare landed Parquet footer with contracts/trips.schema.yaml before COPY.",
)
def schema_contract(
    context: dg.AssetCheckExecutionContext,
    data_bucket: DataBucket,
) -> dg.AssetCheckResult:
    month = context.partition_key
    footer = data_bucket.footer(month)
    expected = trips_schema()
    actual = footer.columns

    mismatches = []
    if len(actual) != len(expected):
        mismatches.append(f"column count mismatch: expected {len(expected)}, got {len(actual)}")
    else:
        for idx, (exp, act) in enumerate(zip(expected, actual, strict=True)):
            if exp.name.lower() != act.name.lower():
                mismatches.append(f"col {idx}: expected name '{exp.name}', got '{act.name}'")
            if exp.type != act.type:
                mismatches.append(f"col {idx} ('{act.name}'): expected type '{exp.type}', got '{act.type}'")

    passed = len(mismatches) == 0
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description="Schema matches contracts/trips.schema.yaml" if passed else f"Schema mismatch: {'; '.join(mismatches)}",
        metadata={
            "num_columns": dg.MetadataValue.int(len(actual)),
            "mismatches": dg.MetadataValue.json(mismatches),
        },
    )
