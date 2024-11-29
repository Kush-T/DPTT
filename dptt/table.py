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
        Prints the data row by row.
        """
        headers = list(self.data.keys())
        n_rows = len(self.data[headers[0]]) if headers else 0
        for i in range(n_rows):
            row = {header: self.data[header][i] for header in headers}
            print(row)

    def __repr__(self):
        """
        Provide a string representation of the table.
        """
        headers = list(self.data.keys())
        n_rows = len(self.data[headers[0]]) if headers else 0
        return f"Table(columns={headers}, rows={n_rows})"
