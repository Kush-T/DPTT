from dptt.table import Table

def _apply_mean_strategy(values):
    """Calculate mean of numeric values."""
    return sum(values) / len(values)

def _apply_median_strategy(values):
    """Calculate median of numeric values."""
    sorted_values = sorted(values)
    n = len(sorted_values)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_values[mid - 1] + sorted_values[mid]) / 2
    return sorted_values[mid]

def _apply_mode_strategy(values):
    """Calculate mode of values, falling back to first value if no unique mode."""
    frequency = {}
    for value in values:
        frequency[value] = frequency.get(value, 0) + 1
    max_count = max(frequency.values())
    modes = [key for key, count in frequency.items() if count == max_count]
    if len(modes) == 1:
        return modes[0]
    print(f"No unique mode found, using first valid value: {values[0]}")
    return values[0]

def fill_missing(table, strategy="mean", columns=None, custom_value=None):
    """
    Handles missing values in a Table object.
    """
    if not isinstance(table, Table):
        raise TypeError("Expected a Table object.")

    data = table.data
    columns = columns or list(data.keys())
    cleaned_data = {col: data[col][:] for col in data}  # Deep copy column data

    # Early validation of columns
    invalid_columns = [col for col in columns if col not in data]
    if invalid_columns:
        raise ValueError(f"Columns {invalid_columns} do not exist in the Table.")

    # Early validation of strategy
    valid_strategies = {"mean", "median", "mode", "drop", "custom"}
    if strategy not in valid_strategies:
        raise ValueError(
            f"Unsupported strategy '{strategy}'. Supported strategies are: {', '.join(valid_strategies)}."
        )

    if strategy == "custom" and custom_value is None:
        raise ValueError("custom_value must be provided when using custom strategy")

    # Handle case where table is empty
    if not data:
        print("Table is empty. Returning original table.")
        return table

    # Handle case where all columns are empty
    if all(not any(row for row in col_values) for col_values in data.values()):
        if strategy == "custom" and custom_value is not None:
            return Table({col: [custom_value] * len(data[next(iter(data))]) for col in data})
        print("All columns are empty. Returning original table.")
        return table

    # Early return if no missing values in specified columns
    if not any(any(value is None or value == '' for value in data[col]) for col in columns):
        print("No missing values found. Returning original table.")
        return table

    # Special handling for drop strategy as it affects all columns at once
    if strategy == "drop":
        valid_indices = [
            i for i in range(len(data[next(iter(data))]))
            if all(data[col][i] is not None and data[col][i] != "" for col in columns)
        ]
        if not valid_indices:
            print("No valid rows remain after applying drop strategy. Returning empty table.")
            return Table({key: [] for key in data.keys()})

        return Table({
            key: [value for idx, value in enumerate(values) if idx in valid_indices]
            for key, values in data.items()
        })

    # Process each column for other strategies
    for col in columns:
        col_values = data[col]
        
        # Get missing indices
        missing_indices = [
            i for i, value in enumerate(col_values) 
            if value is None or value == ''
        ]

        # Skip if no missing values
        if not missing_indices:
            continue

        try:
            # Calculate fill value based on strategy
            if strategy == "custom":
                if valid_values := [v for v in col_values if v is not None]:
                    column_type = type(valid_values[0])
                    if not isinstance(custom_value, column_type):
                        print(f"Skipping column '{col}': Custom value {custom_value} is incompatible with column type {column_type.__name__}.")
                        continue
                fill_value = custom_value
                for i in missing_indices:
                    cleaned_data[col][i] = fill_value
            elif strategy == "mean":
                is_numeric = all(isinstance(value, (int, float)) or value is None or value == '' for value in col_values)
                if not is_numeric:
                    print(f"Skipping column '{col}': Non-numeric data found for mean strategy.")
                    continue
                valid_values = [float(v) for v in col_values if v is not None and v != '']
                fill_value = _apply_mean_strategy(valid_values)
                for i in missing_indices:
                    cleaned_data[col][i] = fill_value
            elif strategy == "median":
                is_numeric = all(isinstance(value, (int, float)) or value is None or value == '' for value in col_values)
                if not is_numeric:
                    print(f"Skipping column '{col}': Non-numeric data found for median strategy.")
                    continue
                valid_values = [float(v) for v in col_values if v is not None and v != '']
                fill_value = _apply_median_strategy(valid_values)
                for i in missing_indices:
                    cleaned_data[col][i] = fill_value
            else:  # mode strategy
                valid_values = [v for v in col_values if v is not None and v != '']
                fill_value = _apply_mode_strategy(valid_values)
                for i in missing_indices:
                    cleaned_data[col][i] = fill_value

        except ValueError as e:
            print(f"Skipping column '{col}': Failed to compute {strategy} - {str(e)}")
            continue

    return Table(cleaned_data)
