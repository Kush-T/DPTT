from dptt.table import Table

class CSVParser:
    @classmethod
    def read_csv(cls, file_path, delimiter=","):
        """
        Reads the CSV file and returns a DataFrame object.

        Args:
            file_path (str): Path to the CSV file.
            delimiter (str): Delimiter used in the file. Default is ','.

        Returns:
            DataFrame: An instance of the DataFrame class containing the parsed data.
        """
        data = {}
        try:
            with open(file_path, mode="r", encoding="utf-8") as file:
                # Read headers
                headers = file.readline().strip().split(delimiter)
                data = {header: [] for header in headers}

                # Read data row by row
                for line in file:
                    values = line.strip().split(delimiter)
                    for header, value in zip(headers, values):
                        data[header].append(value)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")
        
        return Table(data)
