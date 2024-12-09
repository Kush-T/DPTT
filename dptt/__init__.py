from .csv_handler.parser import CSVParser
from .data_cleaning.cleaner import fill_missing
from .transformations.transformations import scale_numeric, encode_categorical
from .exporting.export import export_to_file
from .table.table import Table

# Shortcuts for key functions
parse_csv = CSVParser.parse_csv

__all__ = [
    "parse_csv",
    "fill_missing",
    "scale_numeric",
    "encode_categorical",
    "export_to_file",
    "Table",
]
