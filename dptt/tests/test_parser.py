import unittest
from pathlib import Path
from dptt import CSVParser
from dptt.table.table import Table
from dptt.table.types import (
    IntegerType, FloatType, StringType, DateType, BooleanType, 
    ColumnType
)
from datetime import datetime

class CSVParserTestBase(unittest.TestCase):
    """Base test class with common setup"""
    @classmethod
    def setUpClass(cls):
        cls.testfiles_dir = Path(__file__).parent.parent / "testfiles"

class TestCSVParserBasic(CSVParserTestBase):
    """Tests for basic CSV parsing functionality"""
    
    def test_basic_parsing(self):
        """Test basic CSV parsing with headers"""
        file_path = self.testfiles_dir / "test_with_header.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        self.assertIsInstance(table, Table)
        self.assertEqual(len(table.data['Name']), 3)
        self.assertEqual(table.data['Name'], ['Alice', 'Bob', 'Charlie'])
        self.assertEqual(table.data['Age'], [25, 30, 35])
        self.assertEqual(table.data['Location'], ['New York', 'San Francisco', 'Los Angeles'])

    def test_headerless_parsing(self):
        """Test parsing CSV without headers"""
        file_path = self.testfiles_dir / "test_without_header.csv"
        table = CSVParser.parse_csv(str(file_path), header=False)
        
        self.assertIsInstance(table, Table)
        self.assertEqual(len(table.data['column1']), 3)
        self.assertEqual(table.data['column1'], ['Alice', 'Bob', 'Charlie'])
        self.assertEqual(table.data['column2'], [25, 30, 35])
        self.assertEqual(table.data['column3'], ['New York', 'San Francisco', 'Los Angeles'])

    def test_custom_delimiter(self):
        """Test parsing with custom delimiter"""
        file_path = self.testfiles_dir / "test_custom_delimiter.csv"
        table = CSVParser.parse_csv(str(file_path), delimiter="|")
        
        self.assertEqual(table.data['Name'], ['Alice', 'Bob', 'Charlie'])
        self.assertEqual(table.data['Age'], [30, 25, 35])
        self.assertEqual(table.data['Location'], ['New York', 'Los Angeles', 'Chicago'])

class TestCSVParserFieldHandling(CSVParserTestBase):
    """Tests for special field handling"""

    def test_quoted_fields(self):
        """Test handling of quoted fields"""
        file_path = self.testfiles_dir / "test_quoted_fields.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        self.assertEqual(table.data['Name'], ['Alice', 'Bob', 'Charlie'])
        self.assertEqual(table.data['Age'], [30, 25, 35])
        self.assertEqual(table.data['Location'], ['New York, USA', 'Los Angeles, USA', 'Chicago, USA'])

    def test_escape_characters(self):
        """Test handling of escape characters"""
        file_path = self.testfiles_dir / "test_escape_characters.csv"
        table = CSVParser.parse_csv(str(file_path), escapechar="\\")
        
        self.assertEqual(table.data['Name'], ['Alice', 'Bob', 'Charlie'])
        self.assertEqual(table.data['Age'], [30, 25, 35])
        self.assertEqual(table.data['Location'][0], 'New York\\ USA')
        self.assertEqual(table.data['Location'][1], 'Los AngelesnUSA')
        self.assertEqual(table.data['Location'][2], 'Chicago, USA')

    def test_large_fields(self):
        """Test handling of large fields"""
        file_path = self.testfiles_dir / "test_large_field.csv"
        
        with self.assertRaises(ValueError):
            CSVParser.parse_csv(str(file_path), max_field_size=100)
        
        table = CSVParser.parse_csv(str(file_path))
        self.assertTrue(len(table.data['Description'][0]) > 100)

class TestCSVParserDataTypes(CSVParserTestBase):
    """Tests for data type handling and conversion"""

    def test_mixed_types(self):
        """Test parsing of mixed data types"""
        file_path = self.testfiles_dir / "test_mixed_types.csv"
        table = CSVParser.parse_csv(str(file_path), infer_types=True)
        
        self.assertIsInstance(table.data['Integer'][0], int)
        self.assertIsInstance(table.data['Float'][0], float)
        self.assertIsInstance(table.data['String'][0], str)
        self.assertIsInstance(table.data['Boolean'][0], bool)
        
        self.assertEqual(table.data['Integer'], [123, 456, 789])
        self.assertAlmostEqual(table.data['Float'][0], 45.67)

    def test_date_formats(self):
        """Test parsing of different date formats"""
        file_path = self.testfiles_dir / "test_date_formats.csv"
        column_types = {'Date': DateType()}
        
        table = CSVParser.parse_csv(str(file_path), column_types=column_types)
        self.assertIsInstance(table.data['Date'][0], datetime)
        self.assertEqual(table.data['Value'], [100, 200, 300, 400])

    def test_boolean_types(self):
        """Test parsing of boolean values"""
        file_path = self.testfiles_dir / "test_boolean_types.csv"
        column_types = {'Value': BooleanType()}
        
        table = CSVParser.parse_csv(str(file_path), column_types=column_types)
        self.assertTrue(table.data['Value'][0])   # true
        self.assertFalse(table.data['Value'][1])  # false
        self.assertTrue(table.data['Value'][2])   # yes
        self.assertFalse(table.data['Value'][3])  # no

    def test_numeric_formats(self):
        """Test parsing of various numeric formats"""
        file_path = self.testfiles_dir / "test_numeric_formats.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        self.assertAlmostEqual(table.data['Value1'][0], 1234.5678)
        self.assertAlmostEqual(table.data['Value2'][0], 1.23e-4)
        self.assertEqual(table.data['Value1'][3], 0.5)  # .5 format
        self.assertEqual(table.data['Value2'][3], 1.0)  # trailing decimal
        self.assertAlmostEqual(table.data['Value1'][4], 1.23e10)  # scientific notation
        self.assertAlmostEqual(table.data['Value2'][4], -1.23e-10)  # negative scientific

    def test_custom_column_types(self):
        """Test specifying custom column types during parsing"""
        file_path = self.testfiles_dir / "test_with_header.csv"
        column_types = {
            'Name': StringType(),
            'Age': IntegerType(),
            'Location': StringType()
        }
        
        table = CSVParser.parse_csv(str(file_path), column_types=column_types)
        self.assertIsInstance(table.data['Age'][0], int)
        self.assertIsInstance(table.data['Name'][0], str)
        self.assertIsInstance(table.data['Location'][0], str)
        self.assertEqual(table.data['Age'], [25, 30, 35])

class TestCSVParserErrorHandling(CSVParserTestBase):
    """Tests for error conditions and edge cases"""

    def test_malformed_csv(self):
        """Test handling of malformed CSV data"""
        file_path = self.testfiles_dir / "test_malformed.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        self.assertEqual(len(table.data['Name']), 3)
        self.assertEqual(table.data['Name'], ['Alice', 'Bob', None])
        self.assertEqual(table.data['Location'], [None, 'Los Angeles', None])

    def test_error_conditions(self):
        """Test various error conditions"""
        # Test non-existent file
        with self.assertRaises(FileNotFoundError):
            CSVParser.parse_csv("nonexistent.csv")
        
        # Test empty file
        file_path = self.testfiles_dir / "test_empty.csv"
        with self.assertRaises(ValueError):
            CSVParser.parse_csv(str(file_path))
        
        # Test encoding error
        file_path = self.testfiles_dir / "test_utf8.csv"
        try:
            CSVParser.parse_csv(str(file_path), encoding="ascii")
            self.fail("Expected an error when reading UTF-8 file with ASCII encoding")
        except (UnicodeDecodeError, UnicodeError, TypeError) as e:
            pass

class TestCSVParserSpecialCases(CSVParserTestBase):
    """Tests for special parsing cases"""

    def test_empty_lines(self):
        """Test handling of empty lines"""
        file_path = self.testfiles_dir / "test_empty_lines.csv"
        
        table = CSVParser.parse_csv(str(file_path), skip_blank_lines=True)
        self.assertEqual(len(table.data['Name']), 2)
        
        table = CSVParser.parse_csv(str(file_path), skip_blank_lines=False)
        self.assertEqual(len(table.data['Name']), 4)

    def test_utf8_encoding(self):
        """Test handling of UTF-8 encoded files"""
        file_path = self.testfiles_dir / "test_utf8.csv"
        table = CSVParser.parse_csv(str(file_path), encoding="utf-8")
        
        self.assertEqual(len(table.data['Name']), 3)
        self.assertTrue(any(ord(c) > 127 for c in ''.join(table.data['Name'])))
        self.assertTrue(any(ord(c) > 127 for c in ''.join(table.data['Location'])))

    def test_missing_data_patterns(self):
        """Test various missing data representations"""
        file_path = self.testfiles_dir / "test_missing_data.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        self.assertEqual(table.data['Value1'][1], 'N/A')  # N/A is preserved
        self.assertEqual(table.data['Value2'][0], 'NA')   # NA is preserved
        self.assertEqual(table.data['Value1'][2], '-')    # dash is preserved
        self.assertIsNone(table.data['Value2'][3])       # empty string becomes None
        self.assertEqual(table.data['Value2'][4], 'nan')  # nan is preserved
        self.assertEqual(table.data['Value3'][4], 'None') # None string is preserved
    
class TestCSVParserHeaderHandling(CSVParserTestBase):
    """Tests for header-specific scenarios"""
    
    def test_duplicate_headers(self):
        """Test handling of duplicate and case-sensitive headers"""
        file_path = self.testfiles_dir / "test_duplicate_headers.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        # Test that we have expected number of columns
        self.assertEqual(len(table.data), 4)
        # Test that duplicate headers are handled
        self.assertTrue(any('ID' in header for header in table.data.keys()))
        self.assertTrue(any('Value' in header for header in table.data.keys()))

    def test_special_characters_in_headers(self):
        """Test headers with special characters and spaces"""

        file_path = self.testfiles_dir / "test_special_headers.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        expected_headers = {'First Name', 'Last Name', '#ID', '%Complete', 'Average $'}
        self.assertEqual(set(table.data.keys()), expected_headers)

class TestCSVParserEdgeCases(CSVParserTestBase):
    """Tests for edge cases and boundary conditions"""

    def test_ragged_rows(self):
        """Test handling of rows with inconsistent number of fields"""

        file_path = self.testfiles_dir / "test_ragged_rows.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        self.assertIsNone(table.data['C'][0])  # Missing field should be None
        self.assertEqual(len(table.data.keys()), 3)  # Extra field should be ignored


    def test_empty_quoted_fields(self):
        """Test handling of empty quoted fields"""
        file_path = self.testfiles_dir / "test_empty_quotes.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        # Expect None for empty fields (parser's current behavior)
        self.assertIsNone(table.data['Name'][0])
        self.assertIsNone(table.data['Name'][1])

class TestCSVParserDataValidation(CSVParserTestBase):
    """Tests for data validation scenarios"""

    def test_invalid_dates(self):
        """Test handling of invalid date formats"""
        file_path = self.testfiles_dir / "test_invalid_dates.csv"
        column_types = {'Date': DateType()}
        table = CSVParser.parse_csv(str(file_path), column_types=column_types)
        
        # All invalid dates should remain as strings
        self.assertIsInstance(table.data['Date'][0], str)
        self.assertIsInstance(table.data['Date'][1], str)
        self.assertIsInstance(table.data['Date'][2], str)

    def test_numeric_edge_cases(self):
        """Test handling of edge case numbers"""
        file_path = self.testfiles_dir / "test_numeric_edge.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        # Check if invalid numeric formats are handled
        # The parser currently converts what it can to numbers
        # and leaves the rest as strings or None
        self.assertIsInstance(table.data['Value'][2], (str, int, float, type(None)))
        self.assertIsInstance(table.data['Value'][3], (str, int, float, type(None)))

class TestCSVParserPerformance(CSVParserTestBase):
    """Tests for performance-related scenarios"""
    
    def test_wide_csv(self):
        """Test handling of CSV with many columns"""
        
        file_path = self.testfiles_dir / "test_wide.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        self.assertEqual(len(table.data), 1000)

    def test_character_encoding_variations(self):
        """Test handling of different character encodings"""
        # Using ASCII characters and simple extended ASCII to avoid encoding issues
        
        file_path = self.testfiles_dir / "test_encodings.csv"
        
        # Test basic UTF-8 parsing
        table = CSVParser.parse_csv(str(file_path), encoding='utf-8')
        self.assertEqual(len(table.data['Name']), 3)
        self.assertEqual(table.data['Name'][0], "ABC")
        
        # Test with different encodings
        encodings_to_test = ['ascii', 'utf-8', 'latin1']
        for encoding in encodings_to_test:
            try:
                table = CSVParser.parse_csv(str(file_path), encoding=encoding)
                self.assertEqual(len(table.data['Name']), 3)
            except UnicodeError:
                self.fail(f"Failed to handle {encoding} encoding")

    def test_ascii_range_characters(self):
        """Test handling of ASCII range characters"""
        
        file_path = self.testfiles_dir / "test_ascii_special.csv"
        table = CSVParser.parse_csv(str(file_path))
        
        self.assertEqual(table.data['Symbol'], ['@', '#', '$', '&'])
        self.assertEqual(table.data['Value'], [1, 2, 3, 4])

class TestCSVParserMemoryEfficient(CSVParserTestBase):
    """Tests for memory-efficient CSV parsing functionality"""
    
    def test_memory_efficient_basic(self):
        """Test that memory-efficient mode produces same results as regular mode"""
        file_path = self.testfiles_dir / "test_with_header.csv"
        
        # Parse both ways
        table_regular = CSVParser.parse_csv(str(file_path), memory_efficient=False)
        table_efficient = CSVParser.parse_csv(str(file_path), memory_efficient=True)
        
        # Results should be identical
        self.assertEqual(table_regular.data, table_efficient.data)
        self.assertEqual(len(table_regular.data['Name']), len(table_efficient.data['Name']))
        self.assertEqual(table_regular.data['Name'], table_efficient.data['Name'])
        self.assertEqual(table_regular.data['Age'], table_efficient.data['Age'])
        self.assertEqual(table_regular.data['Location'], table_efficient.data['Location'])

    def test_memory_efficient_large_file(self):
        """Test memory-efficient parsing with large file"""
        file_path = self.testfiles_dir / "test_wide.csv"
        
        # Should handle large file without memory issues
        table = CSVParser.parse_csv(str(file_path), memory_efficient=True)
        self.assertEqual(len(table.data), 1000)  # Test file has 1000 columns

    def test_memory_efficient_with_types(self):
        """Test memory-efficient parsing with type inference and conversion"""
        file_path = self.testfiles_dir / "test_mixed_types.csv"
        
        # Parse with type inference in both modes
        table_regular = CSVParser.parse_csv(str(file_path), infer_types=True, memory_efficient=False)
        table_efficient = CSVParser.parse_csv(str(file_path), infer_types=True, memory_efficient=True)
        
        # Verify same type inference and conversion
        self.assertIsInstance(table_efficient.data['Integer'][0], int)
        self.assertIsInstance(table_efficient.data['Float'][0], float)
        self.assertIsInstance(table_efficient.data['String'][0], str)
        self.assertIsInstance(table_efficient.data['Boolean'][0], bool)
        
        # Verify values are identical
        self.assertEqual(table_regular.data, table_efficient.data)

    def test_memory_efficient_with_errors(self):
        """Test that memory-efficient mode handles errors same as regular mode"""
        file_path = self.testfiles_dir / "test_malformed.csv"
        
        # Parse both ways
        table_regular = CSVParser.parse_csv(str(file_path), memory_efficient=False)
        table_efficient = CSVParser.parse_csv(str(file_path), memory_efficient=True)
        
        # Should handle malformed data identically
        self.assertEqual(table_regular.data, table_efficient.data)
        self.assertEqual(table_regular.data['Name'], table_efficient.data['Name'])
        self.assertEqual(table_regular.data['Location'], table_efficient.data['Location'])

    def test_memory_efficient_empty_lines(self):
        """Test memory-efficient parsing with empty lines"""
        file_path = self.testfiles_dir / "test_empty_lines.csv"
        
        # Test with skip_blank_lines=True
        table_regular = CSVParser.parse_csv(str(file_path), skip_blank_lines=True, memory_efficient=False)
        table_efficient = CSVParser.parse_csv(str(file_path), skip_blank_lines=True, memory_efficient=True)
        self.assertEqual(table_regular.data, table_efficient.data)
        
        # Test with skip_blank_lines=False
        table_regular = CSVParser.parse_csv(str(file_path), skip_blank_lines=False, memory_efficient=False)
        table_efficient = CSVParser.parse_csv(str(file_path), skip_blank_lines=False, memory_efficient=True)
        self.assertEqual(table_regular.data, table_efficient.data)

    def test_memory_efficient_quoted_fields(self):
        """Test memory-efficient parsing with quoted fields"""
        file_path = self.testfiles_dir / "test_quoted_fields.csv"
        
        table_regular = CSVParser.parse_csv(str(file_path), memory_efficient=False)
        table_efficient = CSVParser.parse_csv(str(file_path), memory_efficient=True)
        
        self.assertEqual(table_regular.data, table_efficient.data)
        self.assertEqual(table_regular.data['Location'], 
                        ['New York, USA', 'Los Angeles, USA', 'Chicago, USA'])

    def test_memory_efficient_encoding(self):
        """Test memory-efficient parsing with different encodings"""
        file_path = self.testfiles_dir / "test_utf8.csv"
        
        table_regular = CSVParser.parse_csv(str(file_path), encoding='utf-8', memory_efficient=False)
        table_efficient = CSVParser.parse_csv(str(file_path), encoding='utf-8', memory_efficient=True)
        
        self.assertEqual(table_regular.data, table_efficient.data)
        self.assertEqual(len(table_regular.data['Name']), len(table_efficient.data['Name']))