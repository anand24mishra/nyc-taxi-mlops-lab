# 006: Table Distribution and Sort Keys

Stage: S1 · Date: 2026-11-21 · Author: anand24mishra · Drafted by agent: yes

## Choice

Use `diststyle auto sortkey (tpep_pickup_datetime)` for `ingest.raw_trips` and `sortkey (pickup_at)` for `staging.stg_trips`.

## Rejected alternative

Default unsorted storage or hash distributing on zone ID (`distkey(pulocationid)`).

## Why

In Learner Lab, Redshift runs on a single `ra3.large` node, making inter-node network distribution shuffle irrelevant (`diststyle auto` lets Redshift optimize storage). Sorting physically by pickup timestamp populates 1 MB block Zone Maps (min/max values), enabling Redshift to skip non-matching blocks when filtering by month or computing hourly aggregates.

## What would show this was wrong

Queries or join patterns bottlenecking on zone-based lookups where hash distribution on zone ID outperforms time-based block pruning.
