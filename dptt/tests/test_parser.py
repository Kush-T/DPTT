import unittest
from pathlib import Path
from dptt import CSVParser
from dptt.table import Table

class TestParser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up the testfiles directory
        cls.testfiles_dir = Path(__file__).parent.parent / "testfiles"

    def test_csv_with_header(self):
        file_path = self.testfiles_dir / "test_with_header.csv"
        data = CSVParser.parse_csv(str(file_path))
        self.assertIsInstance(data, Table)
        print("\n--- Parsed Data (with header) ---")
        data.print_rows()

    def test_csv_without_header(self):
        file_path = self.testfiles_dir / "test_without_header.csv"
        data = CSVParser.parse_csv(str(file_path), header=False)
        self.assertIsInstance(data, Table)
        print("\n--- Parsed Data (without header) ---")
        data.print_rows()

    def test_csv_with_custom_delimiter(self):
        file_path = self.testfiles_dir / "test_custom_delimiter.csv"
        data = CSVParser.parse_csv(str(file_path), delimiter="|")
        self.assertIsInstance(data, Table)
        print("\n--- Parsed Data (custom delimiter '|') ---")
        data.print_rows()

    def test_csv_with_quoted_fields(self):
        file_path = self.testfiles_dir / "test_quoted_fields.csv"
        data = CSVParser.parse_csv(str(file_path))
        self.assertIsInstance(data, Table)
        print("\n--- Parsed Data (with quoted fields) ---")
        data.print_rows()

    def test_csv_with_escape_characters(self):
        file_path = self.testfiles_dir / "test_escape_characters.csv"
        data = CSVParser.parse_csv(str(file_path), escapechar="\\")
        self.assertIsInstance(data, Table)
        print("\n--- Parsed Data (with escape characters) ---")
        data.print_rows()

    def test_malformed_csv(self):
        file_path = self.testfiles_dir / "test_malformed.csv"
        data = CSVParser.parse_csv(str(file_path), delimiter=",", header=True)
        self.assertIsInstance(data, Table)
        print("\n--- Parsed Data (malformed) ---")
        data.print_rows()
