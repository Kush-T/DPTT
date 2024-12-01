class Table:
    def __init__(self, data):
        """
        Initialize the table with the parsed CSV data.

        Args:
            data (dict): Dictionary where keys are column names, and values are lists of column data.
        """
        self.data = data

    def print_rows(self):
        """
        Prints the data row by row, ensuring the output has dynamic column widths.
        """
        headers = list(self.data.keys())
        n_rows = len(self.data[headers[0]]) if headers else 0
        
        # Calculate the maximum width for each column
        col_widths = {
            header: max(len(str(header)), max(len(str(value)) for value in self.data[header]))
            for header in headers
        }
        
        # Print header row
        header_row = "  ".join(f"{header:<{col_widths[header]}}" for header in headers)
        print(header_row)
        print("-" * len(header_row))
        
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
        return f"Table(columns={headers}, rows={n_rows})"
