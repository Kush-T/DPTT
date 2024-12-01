from .csv_handler.parser import CSVParser
from .table import Table

parse_csv = CSVParser.parse_csv

__all__ = ["parse_csv", "Table"]