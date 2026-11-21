# 008: Promotion Metric, Margin, and Latency Budget

Stage: S2 · Date: 2026-11-21 · Author: anand24mishra · Drafted by agent: yes

## Choice

Use holdout Mean Absolute Error (MAE in minutes) as the primary promotion metric, requiring a candidate model to beat or match the current champion (margin $\Delta \text{MAE} \le 0.0$ min) while strictly adhering to a single-row inference latency budget of $p95 \le 20.0$ ms.

## Rejected alternative

Using Root Mean Squared Error (RMSE), or enforcing an aggressive minimum improvement margin (e.g. $> 0.5$ min) before designating a version as challenger.

## Why

NYC taxi records contain extreme duration outliers ($> 180$ minutes) from sensor malfunctions or unclosed meters. RMSE squares errors and would heavily distort predictions for ordinary 15-minute trips to compensate for outliers. MAE optimizes for typical trip durations. A $0.0$ margin ensures any true improvement advances to challenger, while the 20 ms $p95$ latency budget prevents complex models from violating production response SLAs.

## What would show this was wrong

If minor or noisy MAE gains cause frequent alias flapping without tangible user experience improvements, or if unweighted slice degradations (e.g., airport or night trips) degrade despite a better overall MAE.
