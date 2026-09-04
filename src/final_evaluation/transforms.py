"""Frozen Phase 5C configuration and helpers for the Phase 5D final TEST evaluation."""

from __future__ import annotations

from ml_selection.transforms import FEATURE_ROOT


ALL_MONTHS = tuple(range(1, 13))
TEST_MONTHS = (11, 12)

# Frozen from data/silver/ml_candidate_selection_report.json (Phase 5C, validation-only
# selection). Model family is fixed to Regularized Linear Regression; only the
# per-horizon, per-feature-set hyperparameters differ. Recorded before any TEST access.
FROZEN_CONFIGS = {
    1: {
        "A": {"reg_param": 0.1, "elastic_net_param": 0.0, "max_iter": 50},
        "B": {"reg_param": 0.1, "elastic_net_param": 0.0, "max_iter": 50},
    },
    3: {
        "A": {"reg_param": 0.1, "elastic_net_param": 0.0, "max_iter": 50},
        "B": {"reg_param": 0.1, "elastic_net_param": 0.0, "max_iter": 50},
    },
    6: {
        "A": {"reg_param": 0.01, "elastic_net_param": 0.0, "max_iter": 50},
        "B": {"reg_param": 0.01, "elastic_net_param": 0.0, "max_iter": 50},
    },
}

# Frozen from data/silver/chronological_splits_baselines_report.json (Phase 5B),
# previous-week seasonal naive, the selected baseline. Not recomputed here.
FROZEN_BASELINE_TEST_METRICS = {
    1: {"rows": 106412, "mae": 20.96935495996692, "rmse": 43.094060201597074},
    3: {"rows": 106338, "mae": 20.99651112490361, "rmse": 43.126228765284566},
    6: {"rows": 106338, "mae": 21.009413379977055, "rmse": 43.13518234648631},
}


def all_month_partition_paths(horizon: int) -> list[str]:
    if horizon not in (1, 3, 6):
        raise ValueError(f"Unsupported horizon: {horizon}")
    return [
        f"{FEATURE_ROOT}/horizon_hours={horizon}/target_local_month={month}"
        for month in ALL_MONTHS
    ]
