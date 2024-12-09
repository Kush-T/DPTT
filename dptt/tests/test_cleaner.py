import unittest
from dptt.table.table import Table
from dptt.data_cleaning import cleaner

class TestHandleMissing(unittest.TestCase):
    def setUp(self):
        """Set up test data."""
        self.table = Table({
            "A": [1, 2, None, 4],
            "B": [5, None, 7, 8],
            "C": ["x", None, "y", "z"]
        })

    def test_mean_imputation(self):
        """Test handling missing values with mean imputation."""
        result = cleaner.fill_missing(self.table, strategy="mean", columns=["A"])
        expected = [1, 2, 2.3333333333333335, 4]  # Mean = (1 + 2 + 4) / 3
        self.assertEqual(result.data["A"], expected)
        self.assertEqual(result.data["B"], [5, None, 7, 8])  # Unaffected

    def test_median_imputation(self):
        """Test handling missing values with median imputation."""
        result = cleaner.fill_missing(self.table, strategy="median", columns=["A"])
        expected = [1, 2, 2, 4]  # Median = 2
        self.assertEqual(result.data["A"], expected)

    def test_mode_imputation(self):
        """Test handling missing values with mode imputation."""
        table = Table({"A": [1, 1, None, 4]})
        result = cleaner.fill_missing(table, strategy="mode", columns=["A"])
        expected = [1, 1, 1, 4]  # Mode = 1
        self.assertEqual(result.data["A"], expected)

    def test_drop_missing(self):
        """Test dropping rows with missing values."""
        result = cleaner.fill_missing(self.table, strategy="drop")
        expected = Table({
            "A": [1, 4],
            "B": [5, 8],
            "C": ["x", "z"]
        })
        self.assertEqual(result.data, expected.data)

    def test_custom_strategy(self):
        """Test using the custom strategy with a specified value."""
        result = cleaner.fill_missing(self.table, strategy="custom", custom_value=99, columns=["A", "B"])
        self.assertEqual(result.data["A"], [1, 2, 99, 4])  # Custom value = 99
        self.assertEqual(result.data["B"], [5, 99, 7, 8])  # Custom value = 99

    def test_empty_table(self):
        """Test handling an empty table."""
        empty_table = Table({})
        result = cleaner.fill_missing(empty_table, strategy="mean")
        self.assertEqual(result.data, {})

    def test_all_columns_empty(self):
        """Test handling a table with all columns empty."""
        empty_data_table = Table({"A": [None, None], "B": [None, None]})
        result = cleaner.fill_missing(empty_data_table, strategy="custom", custom_value=0)
        expected = {"A": [0, 0], "B": [0, 0]}
        self.assertEqual(result.data, expected)

    def test_no_missing_values(self):
        """Test table with no missing values."""
        no_missing_table = Table({"A": [1, 2, 3], "B": [4, 5, 6]})
        result = cleaner.fill_missing(no_missing_table, strategy="mean")
        self.assertEqual(result.data, {"A": [1, 2, 3], "B": [4, 5, 6]})

    def test_invalid_custom_value(self):
        """Test handling an invalid custom value."""
        table = Table({"A": [1, 2, None], "C": ["x", "y", None]})
        result = cleaner.fill_missing(table, strategy="custom", custom_value=99, columns=["C"])
        self.assertEqual(result.data["C"], ["x", "y", None])  # Unaffected due to type mismatch

    def test_invalid_column(self):
        """Test handling a column that doesn't exist."""
        with self.assertRaises(ValueError):
            cleaner.fill_missing(self.table, strategy="mean", columns=["D"])

    def test_invalid_strategy(self):
        """Test an unsupported strategy."""
        with self.assertRaises(ValueError):
            cleaner.fill_missing(self.table, strategy="invalid")

    def test_non_table_input(self):
        """Test that a TypeError is raised when input is not a Table object."""
        with self.assertRaises(TypeError):
            cleaner.fill_missing([], strategy="mean")

    def test_mode_with_non_numeric(self):
        """Test mode strategy with non-numeric columns."""
        table = Table({"C": ["x", "x", None, "y"]})
        result = cleaner.fill_missing(table, strategy="mode")
        self.assertEqual(result.data["C"], ["x", "x", "x", "y"])

    def test_numeric_columns_different_strategies(self):
        """Test mean, median, and mode strategies on entirely numeric columns."""
        table = Table({
            "A": [1.5, 2.5, None, 4.5],
            "B": [10, 20, None, 30]
        })
        
        # Mean strategy
        mean_result = cleaner.fill_missing(table, strategy="mean", columns=["A"])
        self.assertAlmostEqual(mean_result.data["A"][2], 2.8333, places=4)
        
        # Median strategy
        median_result = cleaner.fill_missing(table, strategy="median", columns=["B"])
        self.assertEqual(median_result.data["B"][2], 20)

    def test_mode_with_no_unique_mode(self):
        """Test mode strategy when no unique mode exists."""
        table = Table({"A": [1, 2, None, 3]})
        result = cleaner.fill_missing(table, strategy="mode")
        # This should use the first value when no unique mode exists
        self.assertTrue(result.data["A"][2] in [1, 2, 3])