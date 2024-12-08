import unittest
from dptt.table import Table
from dptt.exporting import export
from pathlib import Path

class TestExport(unittest.TestCase):

    def setUp(self):
        """Set up test data."""
        self.table = Table({
            "col1": [1, 2, 3],
            "col2": ["a", "b, quoted", "c"],
            "col3": [10.5, None, 30.5]
        })
        self.output_file = "test_output.txt"

    def tearDown(self):
        """Clean up after tests."""
        path = Path(self.output_file)
        if path.exists():
            path.unlink()

    def test_export_default(self):
        """Test exporting with default settings."""
        export.export_to_file(self.table, self.output_file)
        path = Path(self.output_file)
        self.assertTrue(path.is_file())

        with path.open("r", encoding="utf-8") as f:
            lines = f.readlines()

        expected = [
            "col1,col2,col3\n",
            "1,a,10.5\n",
            '2,"b, quoted",\n',
            "3,c,30.5\n"
        ]
        self.assertEqual(lines, expected)

    def test_export_custom_delimiter(self):
        """Test exporting with a custom delimiter."""
        export.export_to_file(self.table, self.output_file, delimiter="|")
        path = Path(self.output_file)
        self.assertTrue(path.is_file())

        with path.open("r", encoding="utf-8") as f:
            lines = f.readlines()

        expected = [
            "col1|col2|col3\n",
            "1|a|10.5\n",
            '2|"b, quoted"|\n',
            "3|c|30.5\n"
        ]
        self.assertEqual(lines, expected)

    def test_export_custom_line_ending(self):
        """Test exporting with custom line endings."""
        export.export_to_file(self.table, self.output_file, delimiter=",", line_ending="\r\n")
        
        # Read in binary mode to preserve line endings
        with open(self.output_file, 'rb') as f:
            content = f.read().decode('utf-8')

        expected = (
            "col1,col2,col3\r\n"
            "1,a,10.5\r\n"
            '2,"b, quoted",\r\n'
            "3,c,30.5\r\n"
        )
        self.assertEqual(content, expected)

    def test_export_empty_table(self):
        """Test exporting an empty table."""
        empty_table = Table({})
        with self.assertRaises(ValueError) as context:
            export.export_to_file(empty_table, self.output_file)
        self.assertEqual(str(context.exception), "Table object is empty or invalid.")

    def test_validate_table(self):
        """Test table validation."""
        is_valid = export.validate_table(self.table)
        self.assertTrue(is_valid)

    def test_validate_empty_table(self):
        """Test validation of an empty table."""
        empty_table = Table({})
        is_valid = export.validate_table(empty_table)
        self.assertFalse(is_valid)

    def test_inconsistent_table(self):
        """Test validation of a table with inconsistent row counts."""
        inconsistent_table = Table({
            "col1": [1, 2],
            "col2": ["a", "b", "c"]
        })
        is_valid = export.validate_table(inconsistent_table)
        self.assertFalse(is_valid)

        with self.assertRaises(ValueError) as context:
            export.export_to_file(inconsistent_table, self.output_file)
        self.assertEqual(
            str(context.exception),
            "Column 'col2' has inconsistent row count with the table."
        )