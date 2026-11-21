# 004: COPY vs Redshift Spectrum

Stage: S1 · Date: 2026-11-21 · Author: anand24mishra · Drafted by agent: yes

## Choice

Load monthly NYC taxi Parquet files from S3 into Redshift managed columnar tables (`ingest.raw_trips`) using the Redshift `COPY` command via an atomic temporary table landing pattern.

## Rejected alternative

Querying the Parquet files in place on Amazon S3 using Redshift Spectrum external tables via AWS Glue Data Catalog.

## Why

The monthly trip dataset is fixed-volume batch data (~3M rows/month) used repeatedly for feature transformation, hourly percentile aggregations, and ML training sets. Loading into Redshift columnar storage provides local block zone maps, data compression, and significantly faster aggregation queries than reading over S3 on every query, while eliminating recurring Spectrum S3 scan costs ($5/TB).

## What would show this was wrong

Storage exhaustion on the single-node `ra3.large` Redshift cluster, or data ingest latency dominating pipeline execution time as historical months scale up beyond cluster capacity.
