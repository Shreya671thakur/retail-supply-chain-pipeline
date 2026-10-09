
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from utils import metrics


class TestPipelineMetrics(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.metrics_dir = Path(self.temp_dir.name)

        self.validation_file = self.metrics_dir / "latest_run.json"
        self.pipeline_file = self.metrics_dir / "pipeline_run.json"

        self.patches = [
            patch.object(metrics, "METRICS_DIR", self.metrics_dir),
            patch.object(metrics, "METRICS_FILE", self.validation_file),
            patch.object(
                metrics,
                "PIPELINE_STATUS_FILE",
                self.pipeline_file,
            ),
        ]

        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def test_validation_metrics_are_saved(self):
        metrics.record_metrics(
            total_records=2000,
            valid_records=1995,
            invalid_records=5,
            duplicate_shipments=2,
            status="FAILED",
            duration_seconds=3.456,
        )

        with self.validation_file.open(encoding="utf-8") as file:
            data = json.load(file)

        self.assertEqual(data["total_records"], 2000)
        self.assertEqual(data["valid_records"], 1995)
        self.assertEqual(data["invalid_records"], 5)
        self.assertEqual(data["duplicate_shipments"], 2)
        self.assertEqual(data["status"], "FAILED")
        self.assertEqual(data["duration_seconds"], 3.46)
        self.assertIn("run_timestamp_utc", data)

    def test_pipeline_failure_is_saved(self):
        metrics.record_pipeline_status(
            status="FAILED",
            duration_seconds=12.345,
            failed_step="SQL Analytics",
        )

        with self.pipeline_file.open(encoding="utf-8") as file:
            data = json.load(file)

        self.assertEqual(data["status"], "FAILED")
        self.assertEqual(data["duration_seconds"], 12.35)
        self.assertEqual(data["failed_step"], "SQL Analytics")

    def test_successful_pipeline_status_is_saved(self):
        metrics.record_pipeline_status(
            status="PASSED",
            duration_seconds=20.0,
        )

        with self.pipeline_file.open(encoding="utf-8") as file:
            data = json.load(file)

        self.assertEqual(data["status"], "PASSED")
        self.assertEqual(data["duration_seconds"], 20.0)
        self.assertIsNone(data["failed_step"])


if __name__ == "__main__":
    unittest.main()