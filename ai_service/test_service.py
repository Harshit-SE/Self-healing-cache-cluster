import unittest
import asyncio

from pydantic import ValidationError

import main


class HotKeyAdvisorApiTest(unittest.TestCase):
    def setUp(self):
        if main.model is None:
            asyncio.run(main.app.router.lifespan_context(main.app).__aenter__())

    def test_health_and_time_split_evaluation_are_exposed(self):
        self.assertTrue(main.health()["model_loaded"])
        report = main.get_evaluation()
        self.assertIn("held out", report["split"])
        self.assertIn("logistic_regression", report)
        self.assertIn("rule_baseline", report)
        self.assertIn("Synthetic", report["limitations"])
        self.assertGreater(report["logistic_regression"]["f1"], report["rule_baseline"]["f1"])

    def test_predict_returns_a_controlled_replica_recommendation(self):
        observation = main.HotKeyObservation(**{
            "node_id": "cache-a", "key": "popular-product", "access_rate": 32,
            "read_ratio": 0.92, "node_load": 110, "mean_latency_ms": 5.4,
            "accesses_60s": 1920,
        })
        result = main.predict(observation)
        self.assertGreaterEqual(result["hot_probability"], 0.0)
        self.assertLessEqual(result["hot_probability"], 1.0)
        self.assertIn("recommendation", result)
        self.assertFalse(result["automatically_applied"])

    def test_invalid_observation_is_rejected(self):
        observation = {
            "node_id": "cache-a", "key": "bad-ratio", "access_rate": 1,
            "read_ratio": 1.2, "node_load": 2, "mean_latency_ms": 1,
            "accesses_60s": 60,
        }
        with self.assertRaises(ValidationError):
            main.HotKeyObservation(**observation)

    def test_batch_predictions_are_ranked_by_probability(self):
        common = {"node_id": "cache-a", "node_load": 30, "mean_latency_ms": 1,
                  "accesses_60s": 60}
        cold = main.HotKeyObservation(key="cold", access_rate=0.1, read_ratio=0.4, **common)
        hot = main.HotKeyObservation(key="hot", access_rate=30, read_ratio=0.9, **common)
        results = main.predict_batch([cold, hot])["predictions"]
        self.assertEqual(results[0]["key"], "hot")


if __name__ == "__main__":
    unittest.main()
