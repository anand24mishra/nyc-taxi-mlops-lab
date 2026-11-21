# 007: Notebook vs Pipeline Separation of Concerns

Stage: S2 · Date: 2026-11-21 · Author: anand24mishra · Drafted by agent: yes

## Choice

Keep exploratory data analysis, feature experimentation, baseline comparisons, and hyperparameter tuning in the interactive SageMaker notebook (`explore.ipynb`). Restrict the automated Dagster pipeline to deterministic dataset preparation (`training_set`), model fitting (`trained_model`), standardized evaluation (`eval_report`), and gated alias assignment (`challenger`).

## Rejected alternative

Automating exploratory search and dynamic hyperparameter optimization jobs directly within the production Dagster pipeline.

## Why

Notebooks offer immediate feedback, visualizations, and freedom for data scientists to inspect distributions and try candidate architectures. Embedding exploratory loops into Dagster would bloat pipeline runtimes, introduce non-deterministic execution, and tie up compute resources in production. The pipeline should only execute validated, deterministic workflows.

## What would show this was wrong

If operational friction between notebook research and production deployment causes lag in shipping improved model architectures, or if hyperparameters require frequent automated retuning across data shifts.
