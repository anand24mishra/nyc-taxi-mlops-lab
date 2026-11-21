# 009: Training Window and Holdout Split

Stage: S2 · Date: 2026-11-21 · Author: anand24mishra · Drafted by agent: yes

## Choice

Train the candidate model on a 3-month window (`2024-01` through `2024-03`, ~9.5 million trips) and evaluate exclusively on the subsequent unseen holdout month (`2024-04`, ~3.5 million trips). Zone-pair distance lookups (`est_distance_miles`) are calculated strictly from the training window.

## Rejected alternative

A random train/validation split across all four months, or a single-month training window (`2024-03`).

## Why

Random splitting leaks temporal correlation across train and test sets, inflating validation accuracy. In production, models must predict future trips. Evaluating on `2024-04` tests real forward-looking generalization. Three months of training data give the LightGBM booster sufficient coverage across all 263 zones, day-of-week patterns, and hourly cycles without leaking holdout statistics into the distance lookup table.

## What would show this was wrong

If rapid seasonal shifts between Q1 (winter) and Q2 (spring) cause significant holdout performance degradation, requiring a shorter sliding window or seasonal recency weighting.
