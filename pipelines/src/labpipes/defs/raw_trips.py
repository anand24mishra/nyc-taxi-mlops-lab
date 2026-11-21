"""S1: the raw_trips asset.

One partition per month (partitions.trip_months), depending on trip_files. COPY the month's file into
Redshift with the LabRole attached to the cluster (`iam_role default`), so that re-running a month never
duplicates its rows. The landing columns come from contracts/trips.schema.yaml
(contracts.redshift_columns). Decision points: COPY vs Spectrum; dist and sort keys.
"""

import dagster as dg

from ..contracts import redshift_columns, trips_schema
from ..partitions import check_month, source_file, trip_months
from ..resources import DataBucket, RedshiftData


@dg.asset(
    partitions_def=trip_months,
    deps=["trip_files"],
    group_name="ingest",
    kinds={"redshift"},
    pool="redshift",
)
def raw_trips(
    context: dg.AssetExecutionContext,
    redshift: RedshiftData,
    data_bucket: DataBucket,
) -> dg.MaterializeResult:
    month = check_month(context.partition_key)
    cols = redshift_columns(trips_schema())
    file = source_file(month)
    uri = data_bucket.trip_uri(month)

    redshift.execute(
        "create schema if not exists ingest",
        f"create table if not exists ingest.raw_trips ({cols}, source_file varchar(64)) "
        "diststyle auto sortkey (tpep_pickup_datetime)",
        f"create temp table landing ({cols})",
        f"copy landing from '{uri}' iam_role default format as parquet",
        f"delete from ingest.raw_trips where source_file = '{file}'",
        f"insert into ingest.raw_trips select *, '{file}' as source_file from landing",
        "drop table landing",
    )
    rows = redshift.scalar(f"select count(*) from ingest.raw_trips where source_file = '{file}'")
    return dg.MaterializeResult(
        metadata={
            "rows": dg.MetadataValue.int(int(rows)),
            "source_file": file,
            "uri": uri,
        }
    )
