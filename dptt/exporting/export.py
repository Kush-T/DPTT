from dptt.table import Table

def export_to_file(table, file_path, delimiter=",", line_ending="\n", quotechar='"'):
    """
    Export a Table object to a custom file format.

    Args:
        table (Table): Input Table object.
        file_path (str): Path to save the file.
        delimiter (str): Delimiter for separating values. Default is ','.
        line_ending (str): Line ending character(s). Default is '\n'.
        quotechar (str): Character used to quote fields. Default is '"'.

    Raises:
        ValueError: If the Table object is empty or invalid.
    """
    if not isinstance(table, Table):
        raise TypeError("Input must be a Table object.")

    if not table.data or not table.data.keys():
        raise ValueError("Table object is empty or invalid.")

    column_names = list(table.data.keys())
    row_count = len(next(iter(table.data.values())))

    for col, values in table.data.items():
        if len(values) != row_count:
            raise ValueError(f"Column '{col}' has inconsistent row count with the table.")

    def escape_field(value):
        if value is None:
            return ""
        value = str(value)
        if delimiter in value or quotechar in value or ',' in value or ' ' in value:
            return f'{quotechar}{value.replace(quotechar, quotechar + quotechar)}{quotechar}'
        return value

    # Open in binary mode for consistent line ending handling
    with open(file_path, mode='wb') as file:
        # Write header
        header = delimiter.join(escape_field(col) for col in column_names)
        file.write(header.encode('utf-8'))
        file.write(line_ending.encode('utf-8'))
        
        # Write rows
        for i in range(row_count):
            row = delimiter.join(escape_field(table.data[col][i]) for col in column_names)
            file.write(row.encode('utf-8'))
            file.write(line_ending.encode('utf-8'))

def validate_table(table):
    """
    Validate a Table object to ensure data consistency.

    Args:
        table (Table): Input Table object.

    Returns:
        bool: True if the Table is valid, False otherwise.
    """
    if not isinstance(table, Table):
        raise TypeError("Input must be a Table object.")

    if not table.data:
        print("Validation failed: Table data is empty.")
        return False

    row_count = len(next(iter(table.data.values())))
    for col, values in table.data.items():
        if len(values) != row_count:
            print(f"Validation failed: Column '{col}' has inconsistent row count.")
            return False

    print("Validation passed: Table is consistent.")
    return True
