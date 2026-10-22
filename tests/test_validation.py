import unittest

from weather_pipeline.validation import validate_row_counts


class TestRowCountValidation(unittest.TestCase):

    def test_valid_counts(self):
        validate_row_counts(448, 443, 5)

    def test_mismatched_counts(self):
        with self.assertRaises(ValueError):
            validate_row_counts(448, 440, 5)

    def test_empty_landing(self):
        with self.assertRaises(ValueError):
            validate_row_counts(0, 0, 0)

    def test_negative_count(self):
        with self.assertRaises(ValueError):
            validate_row_counts(10, -1, 11)


if __name__ == "__main__":
    unittest.main()