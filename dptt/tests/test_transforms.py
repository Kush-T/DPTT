import unittest
from dptt.table import Table
from dptt.data_cleaning import cleaner
from dptt.transformations import transformations as tfs


class TestTransforms(unittest.TestCase):

    def setUp(self):
        """Set up test data."""
        self.table = Table({
            "numeric_col": [1, 2, 3, 4, None],
            "category_col": ["a", "b", "a", None, "c"],
            "mixed_col": [1, "a", 2.5, None, "b"]
        })

    def test_scale_numeric_minmax(self):
        """Test min-max scaling of numeric columns."""
        result = tfs.scale_numeric(self.table, columns=["numeric_col"], scaling_type="minmax")
        
        # Calculate expected values dynamically
        col_values = [1, 2, 3, 4]  # Exclude None for min-max scaling
        min_val, max_val = min(col_values), max(col_values)
        expected = [(x - min_val) / (max_val - min_val) if x is not None else None for x in self.table.data["numeric_col"]]
        
        self.assertEqual(result.data["numeric_col"], expected)

    def test_scale_numeric_zscore(self):
        """Test z-score scaling of numeric columns."""
        result = tfs.scale_numeric(self.table, columns=["numeric_col"], scaling_type="zscore")
        expected = [
            (x - 2.5) / 1.118033988749895 if x is not None else None
            for x in [1, 2, 3, 4, None]
        ]
        self.assertAlmostEqual(result.data["numeric_col"], expected)

    def test_encode_categorical(self):
        """Test encoding of categorical columns."""
        result = tfs.encode_categorical(self.table, columns=["category_col"])
        expected = {"a": 0, "b": 1, "c": 2}
        self.assertEqual(result.data["category_col"], [0, 1, 0, None, 2])

    def test_convert_column_types(self):
        """Test column type conversions."""
        result = tfs.convert_column_types(self.table, {"numeric_col": float, "category_col": str})
        self.assertTrue(all(isinstance(x, (float, type(None))) for x in result.data["numeric_col"]))
        self.assertTrue(all(isinstance(x, (str, type(None))) for x in result.data["category_col"]))

    def test_aggregate_data_sum(self):
        """Test data aggregation with sum."""
        table = Table({
            "group_col": ["A", "B", "A", "B", "A"],
            "value_col": [1, 2, 3, 4, 5]
        })
        result = tfs.aggregate_data(table, group_by_columns=["group_col"], agg_column="value_col", agg_func=sum)
        self.assertEqual(result.data["group_col"], ["A", "B"])
        self.assertEqual(result.data["value_col"], [9, 6])

    def test_aggregate_data_mean(self):
        """Test data aggregation with mean."""
        table = Table({
            "group_col": ["X", "Y", "X", "Y", "X"],
            "value_col": [10, 20, 30, 40, 50]
        })
        result = tfs.aggregate_data(table, group_by_columns=["group_col"], agg_column="value_col", agg_func=lambda x: sum(x) / len(x))
        self.assertEqual(result.data["group_col"], ["X", "Y"])
        self.assertEqual(result.data["value_col"], [30.0, 30.0])
