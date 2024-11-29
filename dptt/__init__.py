from .csv_handler.parser import CSVParser
from .table import Table

read_csv = CSVParser.read_csv

__all__ = ["parse_csv", "Table"]