class Table:
    def __init__(self, data, column_types=None):
        """
        Initialize the table with the parsed CSV data.

        Args:
            data: Dictionary where keys are column names, and values are lists of column data
            column_types: Dictionary mapping column names to their ColumnType instances
        """
        self.data = data
        self.column_types = column_types or {}

    def print_rows(self):
        """
        Prints the data row by row, ensuring the output has dynamic column widths.
        Includes type information if available.
        """
        headers = list(self.data.keys())
        n_rows = len(self.data[headers[0]]) if headers else 0
        
        # Calculate the maximum width for each column
        col_widths = {}
        for header in headers:
            type_str = f"({self.column_types[header].name})" if header in self.column_types else ""
            col_widths[header] = max(
                len(str(header)),
                len(type_str),
                max(len(str(value)) for value in self.data[header])
            )
        
        # Print header row
        for header in headers:
            print(f"{header:<{col_widths[header]}}", end="  ")
        print()
        
        # Print type row if types are available
        if self.column_types:
            for header in headers:
                type_str = f"({self.column_types[header].name})" if header in self.column_types else ""
                print(f"{type_str:<{col_widths[header]}}", end="  ")
            print()
        
        print("-" * (sum(col_widths.values()) + len(headers) * 2))
        
        # Print each row
        for i in range(n_rows):
            row = [self.data[header][i] for header in headers]
            formatted_row = "  ".join(f"{str(value):<{col_widths[header]}}" for header, value in zip(headers, row))
            print(formatted_row)

    def __repr__(self):
        """
        Provide a string representation of the table.
        """
        headers = list(self.data.keys())
        n_rows = len(self.data[headers[0]]) if headers else 0
        type_info = ""
        if self.column_types:
            types = {header: self.column_types[header].name for header in headers}
            type_info = f", types={types}"
        return f"Table(columns={headers}{type_info}, rows={n_rows})"
