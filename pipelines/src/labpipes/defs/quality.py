"""S1: data-quality checks on stg_trips (nulls, ranges, duplicates, row counts).

Decision points: which checks block downstream and which only warn, and every threshold. Base them on
what real months look like and on what gameday's corrupt month would look like.
"""

import dagster as dg

from ..partitions import check_month, trip_months
from ..resources import RedshiftData
from .staging import stg_trips


@dg.asset_check(
    asset=stg_trips,
    name="plausible_trips",
    partitions_def=trip_months,
    blocking=False,
    description="Check nonpositive durations, over 3h durations, null passengers, and unknown zones.",
)
def plausible_trips(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    res = redshift.query(
        "select count(*) as rows, "
        "sum(case when duration_min <= 0 then 1 else 0 end) as nonpositive_duration, "
        "sum(case when duration_min > 180 then 1 else 0 end) as over_3h, "
        "sum(case when passenger_count is null then 1 else 0 end) as null_passengers, "
        "sum(case when pickup_zone_id not between 1 and 263 or dropoff_zone_id not between 1 and 263 then 1 else 0 end) as unknown_zone "
        f"from staging.stg_trips where trip_month = '{month}'"
    )
    r = res[0] if res else {"rows": 0, "nonpositive_duration": 0, "over_3h": 0, "null_passengers": 0, "unknown_zone": 0}
    rows = int(r.get("rows") or 0)
    nonpos = int(r.get("nonpositive_duration") or 0)
    over3h = int(r.get("over_3h") or 0)
    null_p = int(r.get("null_passengers") or 0)
    unknown_z = int(r.get("unknown_zone") or 0)

    bad_durations_rate = (nonpos + over3h) / rows if rows > 0 else 0
    passed = rows > 0 and bad_durations_rate < 0.05

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"Month {month}: {rows} rows. Nonpositive duration: {nonpos}, over 3h: {over3h}, "
            f"null passengers: {null_p}, unknown zone: {unknown_z}"
        ),
        metadata={
            "rows": dg.MetadataValue.int(rows),
            "nonpositive_duration": dg.MetadataValue.int(nonpos),
            "over_3h": dg.MetadataValue.int(over3h),
            "null_passengers": dg.MetadataValue.int(null_p),
            "unknown_zone": dg.MetadataValue.int(unknown_z),
            "bad_duration_rate": dg.MetadataValue.float(float(bad_durations_rate)),
        },
    )


@dg.asset_check(
    asset=stg_trips,
    name="no_duplicate_trips",
    partitions_def=trip_months,
    blocking=False,
    description="Check for duplicate trips in staging.",
)
def no_duplicate_trips(
    context: dg.AssetCheckExecutionContext,
    redshift: RedshiftData,
) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    res = redshift.query(
        f"select count(*) as rows, count(*) - count(distinct vendor_id || '|' || pickup_at || '|' || dropoff_at || '|' || pickup_zone_id || '|' || dropoff_zone_id) as extra "
        f"from staging.stg_trips where trip_month = '{month}'"
    )
    r = res[0] if res else {"rows": 0, "extra": 0}
    rows = int(r.get("rows") or 0)
    extra = int(r.get("extra") or 0)
    dup_rate = extra / rows if rows > 0 else 0
    passed = extra == 0 or dup_rate < 0.01

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=f"{extra} duplicate rows in month {month} ({dup_rate:.2%})",
        metadata={
            "rows": dg.MetadataValue.int(rows),
            "duplicate_rows": dg.MetadataValue.int(extra),
            "duplicate_rate": dg.MetadataValue.float(float(dup_rate)),
        },
    )
