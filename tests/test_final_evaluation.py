from __future__ import annotations

import unittest

from pyspark.sql import SparkSession

from final_evaluation.transforms import (
    ALL_MONTHS,
    FROZEN_BASELINE_TEST_METRICS,
    FROZEN_CONFIGS,
    TEST_MONTHS,
    all_month_partition_paths,
)
from ml_selection.transforms import FEATURE_CONTRACTS
from final_evaluation.job import build_frozen_estimator


class FinalEvaluationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("final-evaluation-focused-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_frozen_configs_cover_every_horizon_and_feature_set(self) -> None:
        for horizon in (1, 3, 6):
            for feature_set in ("A", "B"):
                parameters = FROZEN_CONFIGS[horizon][feature_set]
                self.assertIn("reg_param", parameters)
                self.assertEqual(parameters["elastic_net_param"], 0.0)
                self.assertEqual(parameters["max_iter"], 50)

    def test_frozen_configs_match_phase_5c_selection_report(self) -> None:
        # Matches data/silver/ml_candidate_selection_report.json best-by-feature-set
        # regularized_linear_regression rows recorded before this phase began.
        self.assertEqual(FROZEN_CONFIGS[1]["A"]["reg_param"], 0.1)
        self.assertEqual(FROZEN_CONFIGS[1]["B"]["reg_param"], 0.1)
        self.assertEqual(FROZEN_CONFIGS[3]["A"]["reg_param"], 0.1)
        self.assertEqual(FROZEN_CONFIGS[3]["B"]["reg_param"], 0.1)
        self.assertEqual(FROZEN_CONFIGS[6]["A"]["reg_param"], 0.01)
        self.assertEqual(FROZEN_CONFIGS[6]["B"]["reg_param"], 0.01)

    def test_frozen_baseline_test_metrics_match_phase_5b_report(self) -> None:
        # Matches data/silver/chronological_splits_baselines_report.json
        # previous_week_seasonal_naive test rows, not recomputed here.
        self.assertAlmostEqual(FROZEN_BASELINE_TEST_METRICS[1]["mae"], 20.96935495996692)
        self.assertAlmostEqual(FROZEN_BASELINE_TEST_METRICS[3]["mae"], 20.99651112490361)
        self.assertAlmostEqual(FROZEN_BASELINE_TEST_METRICS[6]["mae"], 21.009413379977055)
        for horizon in (1, 3, 6):
            self.assertGreater(FROZEN_BASELINE_TEST_METRICS[horizon]["rows"], 0)

    def test_all_month_partition_paths_include_test_months(self) -> None:
        for horizon in (1, 3, 6):
            paths = all_month_partition_paths(horizon)
            self.assertEqual(len(paths), 12)
            self.assertEqual(ALL_MONTHS, tuple(range(1, 13)))
            self.assertTrue(any("target_local_month=11" in path for path in paths))
            self.assertTrue(any("target_local_month=12" in path for path in paths))
        self.assertEqual(TEST_MONTHS, (11, 12))

    def test_estimator_uses_frozen_hyperparameters_per_horizon_and_feature_set(self) -> None:
        for horizon in (1, 3, 6):
            for feature_set in ("A", "B"):
                estimator = build_frozen_estimator(feature_set, horizon)
                parameters = FROZEN_CONFIGS[horizon][feature_set]
                self.assertEqual(estimator.getRegParam(), parameters["reg_param"])
                self.assertEqual(estimator.getElasticNetParam(), parameters["elastic_net_param"])
                self.assertEqual(estimator.getMaxIter(), parameters["max_iter"])
                self.assertEqual(estimator.getFeaturesCol(), "features")
                self.assertEqual(estimator.getLabelCol(), "pickup_count")

    def test_feature_contracts_reused_unchanged_from_phase_5c(self) -> None:
        self.assertNotIn("pickup_count", FEATURE_CONTRACTS["A"].original_columns)
        self.assertNotIn("pickup_count", FEATURE_CONTRACTS["B"].original_columns)
        self.assertIn("location_id", FEATURE_CONTRACTS["A"].categorical_columns)


if __name__ == "__main__":
    unittest.main(verbosity=2)
