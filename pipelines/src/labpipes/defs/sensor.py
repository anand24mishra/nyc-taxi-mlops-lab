"""S1: the S3 sensor (and the job it starts).

Watch trips/ in the data bucket (DataBucket.trip_files). When a month's file appears or changes, add
the month to the trip_months partitions and request a run that goes from trip_files to zone_hour_stats.
Patterns: pipelines/reference/patterns.py.
"""

import json

import dagster as dg

from ..partitions import trip_months
from ..resources import DataBucket
from .marts import zone_hour_stats
from .raw_trips import raw_trips
from .staging import stg_trips
from .trip_files import trip_files

ingest_month = dg.define_asset_job(
    "ingest_month",
    selection=dg.AssetSelection.assets(trip_files, raw_trips, stg_trips, zone_hour_stats),
    description="Ingest one month from S3 into Redshift and rebuild zone_hour_stats.",
)


@dg.sensor(
    job=ingest_month,
    minimum_interval_seconds=60,
)
def trip_files_sensor(context: dg.SensorEvaluationContext, data_bucket: DataBucket):
    """Watches trips/ in the data bucket, adds dynamic partitions, and requests ingest_month runs."""
    cursor_data = json.loads(context.cursor or "{}")
    if isinstance(cursor_data, list):
        seen = {m: "" for m in cursor_data}
    else:
        seen = cursor_data

    files = data_bucket.trip_files()
    to_add_partitions = []
    run_requests = []

    for f in files:
        if f.month not in seen:
            to_add_partitions.append(f.month)
            run_requests.append(dg.RunRequest(partition_key=f.month, run_key=f"{f.month}:{f.etag}"))
        elif seen.get(f.month) != f.etag:
            run_requests.append(dg.RunRequest(partition_key=f.month, run_key=f"{f.month}:{f.etag}"))

    new_cursor = {f.month: f.etag for f in files}

    if not run_requests and not to_add_partitions:
        return dg.SkipReason("No new or modified trip files under trips/")

    return dg.SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=[trip_months.build_add_request(to_add_partitions)] if to_add_partitions else [],
        cursor=json.dumps(new_cursor, sort_keys=True),
    )
