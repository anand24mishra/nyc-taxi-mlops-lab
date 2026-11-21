"""Tests for S2: feature_contract, training_set, trained_model, eval_report, beats_champion, and challenger."""

from pathlib import Path

import dagster as dg
import mlflow
from fakes import FakeRedshift, LocalArtifacts, unloading_redshift

from labpipes.defs.feature_check import feature_contract
from labpipes.defs.features import trip_features
from labpipes.defs.training import (
    beats_champion,
    challenger,
    eval_report,
    trained_model,
    training_set,
)
from labpipes.partitions import trip_months
from labpipes.resources import MlflowTracking


class FeatureCheckRedshift(FakeRedshift):
    cluster_id: str = "lab"
    database: str = "lab"
    db_user: str = "labadmin"
    cols: list = ["trip_month", "pickup_zone_id", "dropoff_zone_id", "pickup_hour", "pickup_dow", "duration_min"]
    bad_rows: int = 0
    total_rows: int = 1000

    def query(self, sql: str) -> list[dict]:
        if "from information_schema.columns" in sql:
            return [{"column_name": c} for c in self.cols]
        return super().query(sql)

    def scalar(self, sql: str):
        if "pickup_zone_id not between" in sql:
            return self.bad_rows
        if "count(*) from staging.trip_features" in sql:
            return self.total_rows
        return super().scalar(sql)


def run_feature_contract(rs, month="2024-01"):
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(trip_months.name, [month])
    res = dg.materialize(
        [trip_features, feature_contract],
        partition_key=month,
        instance=instance,
        resources={"redshift": rs},
        raise_on_error=False,
    )
    # Return the check evaluation
    for ev in res.get_asset_check_evaluations():
        return ev
    return None


def test_feature_contract_passes_on_valid_schema():
    rs = FeatureCheckRedshift()
    ev = run_feature_contract(rs)
    assert ev is not None
    assert ev.passed
    assert "match feature contract" in ev.description


def test_feature_contract_fails_on_missing_column():
    rs = FeatureCheckRedshift(cols=["trip_month", "pickup_zone_id"])
    ev = run_feature_contract(rs)
    assert ev is not None
    assert not ev.passed
    assert "Missing required columns" in ev.description


def test_feature_contract_fails_on_forbidden_column():
    rs = FeatureCheckRedshift(cols=["trip_month", "pickup_zone_id", "dropoff_zone_id", "pickup_hour", "pickup_dow", "duration_min", "trip_distance"])
    ev = run_feature_contract(rs)
    assert ev is not None
    assert not ev.passed
    assert "Forbidden post-trip columns" in ev.description


def test_feature_contract_fails_on_out_of_range_values():
    rs = FeatureCheckRedshift(bad_rows=42)
    ev = run_feature_contract(rs)
    assert ev is not None
    assert not ev.passed
    assert "42 rows with values out of contract range" in ev.description


def test_training_pipeline_materializes_and_promotes_challenger(tmp_path):
    # Setup local artifacts directory
    art_root = str(tmp_path / "artifacts")
    Path(art_root).mkdir()
    artifacts = LocalArtifacts(bucket="art", root=art_root)
    rs = unloading_redshift(art_root)

    # Setup local MLflow SQLite tracking
    tracking_uri = f"sqlite:///{tmp_path}/mlflow.db"
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.create_experiment("trip-duration")
    tracking = MlflowTracking(tracking_uri=tracking_uri)

    instance = dg.DagsterInstance.ephemeral()
    res = dg.materialize(
        [training_set, trained_model, eval_report, challenger, beats_champion],
        instance=instance,
        resources={
            "redshift": rs,
            "artifacts": artifacts,
            "tracking": tracking,
        },
    )
    assert res.success

    # Verify registered model in MLflow
    client = mlflow.MlflowClient(tracking_uri)
    model_version = client.get_model_version_by_alias("trip-duration", "challenger")
    assert model_version is not None
    assert str(model_version.version) == "1"

    # Verify version tags
    tags = model_version.tags
    assert tags["training_window"] == "2024-01..2024-03"
    assert tags["holdout_month"] == "2024-04"
    assert "mae_min" in tags
    assert "p95_latency_ms" in tags
    assert "champion_mae_min" in tags
