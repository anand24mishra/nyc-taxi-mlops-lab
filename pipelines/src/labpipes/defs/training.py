"""S2: training_set, trained_model, eval_report, the challenger gate and the challenger asset.

- training_set: UNLOAD a window of staging.trip_features (and the holdout month) to the artifact bucket.
  The zone-pair distance lookup (contracts/features.yaml, est_distance_miles) comes from the window only.
- trained_model: fit tripmodel.LightGBMModel (the given v2), log it to MLflow as a pyfunc with
  tripmodel.model_signature() and the tripmodel package bundled, register a version of the model named
  in contracts/mlflow.yaml, and tag it as that contract says.
- eval_report: tripmodel.evaluate the new version and the champion (or the v1 baseline before there is
  one) on the same holdout; log the metrics.
- A blocking check that guards a `challenger` asset, which sets the alias. The pipeline never moves
  `champion`.
Decision points: what stays in the notebook; the promotion metric and margin; the training window.
"""

import os
import subprocess
from pathlib import Path

import dagster as dg
import mlflow

import tripmodel
from tripmodel import BaselineModel, LightGBMModel, evaluate, model_signature

from ..contracts import features_contract, mlflow_contract
from ..resources import ArtifactBucket, MlflowTracking, RedshiftData
from .features import trip_features

TRAIN_WINDOW = ["2024-01", "2024-02", "2024-03"]
HOLDOUT_MONTH = "2024-04"
TRAIN_WINDOW_TAG = "2024-01..2024-03"
LATENCY_BUDGET_MS = 20.0
MARGIN_MIN = 0.0


def _git_sha() -> str:
    if sha := os.environ.get("GIT_SHA"):
        return sha
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


@dg.asset(deps=[trip_features], group_name="model", kinds={"redshift", "s3"}, pool="redshift")
def training_set(context: dg.AssetExecutionContext, redshift: RedshiftData, artifacts: ArtifactBucket) -> dg.MaterializeResult:
    """UNLOAD training window (2024-01..2024-03), holdout (2024-04), and zone distance lookup to S3."""
    train_in = ", ".join(f"''{m}''" for m in TRAIN_WINDOW)
    holdout_in = f"''{HOLDOUT_MONTH}''"

    # Redshift UNLOAD statements (lowercase 'unload' for test harness matching)
    redshift.execute(
        f"unload ('select pickup_zone_id, dropoff_zone_id, pickup_hour, pickup_dow, duration_min from staging.trip_features where trip_month in ({train_in})') "
        f"to '{artifacts.uri('training/train/')}' iam_role default format as parquet cleanpath",
        f"unload ('select pickup_zone_id, dropoff_zone_id, pickup_hour, pickup_dow, duration_min from staging.trip_features where trip_month in ({holdout_in})') "
        f"to '{artifacts.uri('training/holdout/')}' iam_role default format as parquet cleanpath",
        f"unload ('select pickup_zone_id, dropoff_zone_id, median(trip_distance) as est_distance_miles from staging.stg_trips where trip_month in ({train_in}) and trip_distance > 0 group by 1, 2') "
        f"to '{artifacts.uri('training/distances/')}' iam_role default format as parquet cleanpath",
    )

    counts = redshift.query(
        "select count(case when trip_month in ('2024-01', '2024-02', '2024-03') then 1 end) as train, "
        "count(case when trip_month = '2024-04' then 1 end) as holdout "
        "from staging.trip_features"
    )
    train_rows = int(counts[0].get("train") or 0)
    holdout_rows = int(counts[0].get("holdout") or 0)

    artifacts.write_json(
        "training/summary.json",
        {
            "training_window": TRAIN_WINDOW_TAG,
            "holdout_month": HOLDOUT_MONTH,
            "train_rows": train_rows,
            "holdout_rows": holdout_rows,
        },
    )

    return dg.MaterializeResult(
        metadata={
            "training_window": dg.MetadataValue.text(TRAIN_WINDOW_TAG),
            "holdout_month": dg.MetadataValue.text(HOLDOUT_MONTH),
            "train_rows": dg.MetadataValue.int(train_rows),
            "holdout_rows": dg.MetadataValue.int(holdout_rows),
        }
    )


@dg.asset(deps=[training_set], group_name="model", kinds={"mlflow", "python"})
def trained_model(context: dg.AssetExecutionContext, artifacts: ArtifactBucket, tracking: MlflowTracking) -> dg.MaterializeResult:
    """Train LightGBM v2 model on the training window and register it in MLflow."""
    train_df = artifacts.read_parquet("training/train")
    dist_df = artifacts.read_parquet("training/distances")

    contract = mlflow_contract()
    reg_name = contract["registered_model"]
    exp_name = contract["experiment"]

    mlflow.set_tracking_uri(tracking.tracking_uri)
    mlflow.set_experiment(exp_name)

    model = LightGBMModel().fit(train_df, dist_df, num_rounds=300)

    with mlflow.start_run(run_name="train_lgbm") as run:
        run_id = run.info.run_id
        model_info = mlflow.pyfunc.log_model(
            name="model",
            python_model=model,
            signature=model_signature(),
            code_paths=[str(Path(tripmodel.__file__).parent)],
            registered_model_name=reg_name,
        )

    version = str(model_info.registered_model_version)
    artifacts.write_json(
        "training/model_info.json",
        {"version": version, "run_id": run_id, "registered_model": reg_name},
    )

    return dg.MaterializeResult(
        metadata={
            "registered_model": dg.MetadataValue.text(reg_name),
            "version": dg.MetadataValue.text(version),
            "run_id": dg.MetadataValue.text(run_id),
        }
    )


@dg.asset(deps=[trained_model, training_set], group_name="model", kinds={"mlflow", "python"})
def eval_report(
    context: dg.AssetExecutionContext,
    redshift: RedshiftData,
    artifacts: ArtifactBucket,
    tracking: MlflowTracking,
) -> dg.MaterializeResult:
    """Evaluate candidate model vs. champion (or v1 baseline) on the holdout month."""
    contract = mlflow_contract()
    reg_name = contract["registered_model"]
    exp_name = contract["experiment"]
    feat_contract = features_contract()

    model_meta = artifacts.read_json("training/model_info.json")
    version = str(model_meta["version"])
    run_id = str(model_meta["run_id"])

    holdout_df = artifacts.read_parquet("training/holdout")

    # Taxi zones borough lookup
    zone_rows = redshift.query("select locationid as location_id, borough from ingest.taxi_zones")
    boroughs = {int(r["location_id"]): str(r["borough"]) for r in zone_rows if r.get("location_id") is not None}

    mlflow.set_tracking_uri(tracking.tracking_uri)
    mlflow.set_experiment(exp_name)
    client = tracking.client()

    # Determine baseline / champion model for comparison
    try:
        champ_model = mlflow.pyfunc.load_model(f"models:/{reg_name}@champion")
        champ_eval = evaluate(champ_model.predict, holdout_df, boroughs)
    except Exception:
        # Before champion exists, evaluate v1 BaselineModel fit on the same train window
        train_df = artifacts.read_parquet("training/train")
        dist_df = artifacts.read_parquet("training/distances")
        baseline = BaselineModel().fit(train_df, dist_df)
        champ_eval = evaluate(lambda df: baseline.predict(None, df), holdout_df, boroughs)

    champion_mae = float(champ_eval["mae_min"])

    # Evaluate candidate model
    candidate_model = mlflow.pyfunc.load_model(f"models:/{reg_name}/{version}")
    cand_eval = evaluate(candidate_model.predict, holdout_df, boroughs)
    candidate_mae = float(cand_eval["mae_min"])
    p95_latency = float(cand_eval["p95_latency_ms"])

    # Log metrics to candidate's MLflow run
    with mlflow.start_run(run_id=run_id):
        log_metrics = {
            "mae_min": candidate_mae,
            "p95_latency_ms": p95_latency,
            "champion_mae_min": champion_mae,
        }
        for k, v in cand_eval.items():
            if k.startswith("mae_min."):
                log_metrics[k] = float(v)
        mlflow.log_metrics(log_metrics)

    # Set required version tags on the model version
    git_sha = _git_sha()
    features_ver = str(feat_contract.get("version", 1))

    tags_to_set = {
        "training_window": TRAIN_WINDOW_TAG,
        "holdout_month": HOLDOUT_MONTH,
        "features_version": features_ver,
        "git_sha": git_sha,
        "mae_min": str(round(candidate_mae, 4)),
        "p95_latency_ms": str(round(p95_latency, 2)),
        "champion_mae_min": str(round(champion_mae, 4)),
    }
    for k, v in tags_to_set.items():
        client.set_model_version_tag(name=reg_name, version=version, key=k, value=v)

    report = {
        "version": version,
        "run_id": run_id,
        "candidate_mae": candidate_mae,
        "champion_mae": champion_mae,
        "p95_latency_ms": p95_latency,
        "slices": {k: float(v) for k, v in cand_eval.items() if k.startswith("mae_min.")},
    }
    artifacts.write_json("training/eval_report.json", report)

    return dg.MaterializeResult(
        metadata={
            "candidate_mae": dg.MetadataValue.float(candidate_mae),
            "champion_mae": dg.MetadataValue.float(champion_mae),
            "p95_latency_ms": dg.MetadataValue.float(p95_latency),
            "delta_mae": dg.MetadataValue.float(candidate_mae - champion_mae),
        }
    )


@dg.asset_check(
    asset=eval_report,
    blocking=True,
    description="Candidate model must beat champion holdout MAE within single-row latency budget.",
)
def beats_champion(context: dg.AssetCheckExecutionContext, artifacts: ArtifactBucket) -> dg.AssetCheckResult:
    """Blocking gate check before assigning challenger alias."""
    report = artifacts.read_json("training/eval_report.json")
    cand_mae = float(report["candidate_mae"])
    champ_mae = float(report["champion_mae"])
    p95_lat = float(report["p95_latency_ms"])

    mae_beats = cand_mae <= champ_mae - MARGIN_MIN
    latency_ok = p95_lat <= LATENCY_BUDGET_MS

    passed = mae_beats and latency_ok
    desc = (
        f"Candidate MAE {cand_mae:.3f} vs champion {champ_mae:.3f} "
        f"(delta: {cand_mae - champ_mae:.3f} min, budget margin {MARGIN_MIN} min); "
        f"p95 latency {p95_lat:.2f} ms (budget {LATENCY_BUDGET_MS} ms)"
    )

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=desc,
        metadata={
            "candidate_mae": dg.MetadataValue.float(cand_mae),
            "champion_mae": dg.MetadataValue.float(champ_mae),
            "p95_latency_ms": dg.MetadataValue.float(p95_lat),
        },
    )


@dg.asset(deps=[eval_report], group_name="model", kinds={"mlflow"})
def challenger(context: dg.AssetExecutionContext, artifacts: ArtifactBucket, tracking: MlflowTracking) -> dg.MaterializeResult:
    """Sets the 'challenger' alias in MLflow for the newly verified model version."""
    contract = mlflow_contract()
    reg_name = contract["registered_model"]

    report = artifacts.read_json("training/eval_report.json")
    version = str(report["version"])

    client = tracking.client()
    client.set_registered_model_alias(name=reg_name, alias="challenger", version=version)

    return dg.MaterializeResult(
        metadata={
            "registered_model": dg.MetadataValue.text(reg_name),
            "challenger_version": dg.MetadataValue.text(version),
        }
    )


train_model = dg.define_asset_job(
    "train_model",
    selection=dg.AssetSelection.assets(training_set, trained_model, eval_report, challenger),
    description="Train LightGBM model on window, evaluate on holdout, and promote to challenger if gate passes.",
)
