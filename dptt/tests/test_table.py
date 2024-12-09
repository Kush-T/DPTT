import unittest
from datetime import datetime
from dptt.table.types import (
    StringType, IntegerType, FloatType, 
    DateType, BooleanType, infer_column_type
)
from dptt.table.table import Table

class TestColumnTypes(unittest.TestCase):
    def test_string_type(self):
        """Test StringType validation and conversion"""
        string_type = StringType()
        
        # Validation
        self.assertTrue(string_type.validate("hello"))
        self.assertTrue(string_type.validate("123"))
        self.assertTrue(string_type.validate(""))
        
        # Conversion
        self.assertEqual(string_type.convert("hello"), "hello")
        self.assertEqual(string_type.convert(123), "123")

    def test_integer_type(self):
        """Test IntegerType validation and conversion"""
        int_type = IntegerType()
        
        # Validation
        self.assertTrue(int_type.validate("123"))
        self.assertTrue(int_type.validate("-456"))
        self.assertFalse(int_type.validate("12.34"))
        self.assertFalse(int_type.validate("abc"))
        
        # Conversion
        self.assertEqual(int_type.convert("123"), 123)
        self.assertEqual(int_type.convert("-456"), -456)
        with self.assertRaises(ValueError):
            int_type.convert("abc")

    def test_float_type(self):
        """Test FloatType validation and conversion"""
        float_type = FloatType()
        
        # Validation
        self.assertTrue(float_type.validate("123.45"))
        self.assertTrue(float_type.validate("-0.001"))
        self.assertTrue(float_type.validate("123"))
        self.assertFalse(float_type.validate("abc"))
        
        # Conversion
        self.assertAlmostEqual(float_type.convert("123.45"), 123.45)
        self.assertAlmostEqual(float_type.convert("-0.001"), -0.001)
        self.assertAlmostEqual(float_type.convert("123"), 123.0)

    def test_date_type(self):
        """Test DateType validation and conversion"""
        date_type = DateType()
        
        # Validation
        valid_dates = [
            "2023-12-31", 
            "31/12/2023", 
            "12/31/2023", 
            "2023/12/31"
        ]
        invalid_dates = [
            "2023-13-32", 
            "abc", 
            "31-12-2023"
        ]
        
        for date in valid_dates:
            self.assertTrue(date_type.validate(date), f"Failed to validate {date}")
        
        for date in invalid_dates:
            self.assertFalse(date_type.validate(date), f"Incorrectly validated {date}")
        
        # Conversion
        converted_date = date_type.convert("2023-12-31")
        self.assertIsInstance(converted_date, datetime)
        self.assertEqual(converted_date.year, 2023)
        self.assertEqual(converted_date.month, 12)
        self.assertEqual(converted_date.day, 31)

    def test_boolean_type(self):
        """Test BooleanType validation and conversion"""
        bool_type = BooleanType()
        
        # Validation
        true_values = ['true', 'yes', '1', 't', 'y']
        false_values = ['false', 'no', '0', 'f', 'n']
        
        for val in true_values + false_values:
            self.assertTrue(bool_type.validate(val), f"Failed to validate {val}")
        
        self.assertFalse(bool_type.validate("maybe"))
        
        # Conversion
        for val in true_values:
            self.assertTrue(bool_type.convert(val))
        
        for val in false_values:
            self.assertFalse(bool_type.convert(val))
        
        with self.assertRaises(ValueError):
            bool_type.convert("maybe")

    def test_type_inference(self):
        """Test type inference functionality"""
        # Integer inference
        int_sample = ['1', '2', '3', '4', '5']
        self.assertIsInstance(infer_column_type(int_sample), IntegerType)
        
        # Float inference
        float_sample = ['1.1', '2.2', '3.3', '4.4']
        self.assertIsInstance(infer_column_type(float_sample), FloatType)
        
        # Date inference
        date_sample = ['2023-12-31', '2022-01-15', '2024-06-20']
        self.assertIsInstance(infer_column_type(date_sample), DateType)
        
        # Boolean inference
        bool_sample = ['true', 'false', 'yes', 'no']
        self.assertIsInstance(infer_column_type(bool_sample), BooleanType)
        
        # String inference with mixed types
        mixed_sample = ['1', 'a', '2.5']
        self.assertIsInstance(infer_column_type(mixed_sample), StringType)

class TestTable(unittest.TestCase):
    def setUp(self):
        """Create a sample table for testing"""
        data = {
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [30, 25, 35],
            'salary': [50000.0, 60000.0, 75000.0]
        }
        self.table = Table(data)

    def test_table_initialization(self):
        """Test table initialization and basic properties"""
        self.assertEqual(len(self.table.data['name']), 3)
        self.assertEqual(self.table.data['name'][0], 'Alice')
        self.assertEqual(self.table.data['age'][1], 25)

    def test_table_repr(self):
        """Test string representation of the table"""
        repr_str = repr(self.table)
        self.assertIn('columns', repr_str)
        self.assertIn('rows=3', repr_str)

    def test_print_rows(self):
        """
        Test print_rows method.
        Note: This mainly checks that the method runs without errors.
        Actual visual verification would be manual.
        """
        import io
        import sys
        
        # Capture stdout
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            self.table.print_rows()
            # Ensure something was printed
            output = captured_output.getvalue()
            self.assertTrue(len(output) > 0)
        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__
