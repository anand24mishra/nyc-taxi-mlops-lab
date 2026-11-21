# 005: Quality Check Strictness and Thresholds

Stage: S1 · Date: 2026-11-21 · Author: anand24mishra · Drafted by agent: yes

## Choice

Enforce a hard blocking check on `trip_files` (`schema_contract`) that stops pipeline execution on column count, ordering, or type mismatch before COPY; enforce warning-level checks on `stg_trips` that alert if bad trips (nonpositive duration, > 180 min duration, unknown zones) exceed 5% of monthly volume.

## Rejected alternative

Hard-blocking the entire pipeline run on any single anomalous row (e.g., aborting if nonpositive duration > 0).

## Why

Real-world TLC taxi data contains inherent sensor and manual input errors (< 1.5% zero-distance or negative-duration records). Aborting the entire monthly pipeline on a handful of dirty rows would prevent updating production marts and models for minor noise. Retaining bad rows in staging allows data quality observability, while `zone_hour_stats.sql` explicitly filters valid trips (`duration_min > 0 and duration_min <= 180 and pickup_zone_id between 1 and 263`).

## What would show this was wrong

Corrupt or anomalous trips leaking into `marts.zone_hour_stats` or training datasets, distorting duration percentiles or degrading model evaluation metrics.
