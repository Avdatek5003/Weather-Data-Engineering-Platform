import unittest
from types import SimpleNamespace

from weather_pipeline.batch import build_batch_key


class TestBatchIdentity(unittest.TestCase):

    def test_full_date_range(self):
        args = SimpleNamespace(
            mode="full",
            start_date="1940-01-01",
            end_date="1940-01-31",
        )

        self.assertEqual(
            build_batch_key(args),
            "historical_full_19400101_19400131",
        )

    def test_same_input_same_key(self):
        args = SimpleNamespace(
            mode="full",
            start_date="1940-02-01",
            end_date="1940-02-28",
        )

        self.assertEqual(
            build_batch_key(args),
            build_batch_key(args),
        )

    def test_different_ranges_different_keys(self):
        january = SimpleNamespace(
            mode="full",
            start_date="1940-01-01",
            end_date="1940-01-31",
        )

        february = SimpleNamespace(
            mode="full",
            start_date="1940-02-01",
            end_date="1940-02-28",
        )

        self.assertNotEqual(
            build_batch_key(january),
            build_batch_key(february),
        )

    def test_sample_mode(self):
        args = SimpleNamespace(
            mode="sample",
            start_date=None,
            end_date=None,
        )

        self.assertEqual(
            build_batch_key(args),
            "historical_sample_unbounded",
        )


if __name__ == "__main__":
    unittest.main()