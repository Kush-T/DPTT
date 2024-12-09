from dptt.table.table import Table

def scale_numeric(table, columns=None, scaling_type="minmax"):
    """
    Scale numeric columns in the dataset.
    
    Args:
        table (Table): Input Table object.
        columns (list): List of columns to scale. If None, all numeric columns will be scaled.
        scaling_type (str): Scaling type, either "minmax" or "zscore".
    
    Returns:
        Table: Table object with scaled numeric columns.
    """
    data = table.data
    columns = columns or [col for col in data if isinstance(data[col][0], (int, float))]
    scaled_data = {col: data[col][:] for col in data}

    for col in columns:
        col_values = [v for v in data[col] if v is not None]
        if scaling_type == "minmax":
            min_val, max_val = min(col_values), max(col_values)
            scaled_data[col] = [(x - min_val) / (max_val - min_val) if x is not None else None for x in data[col]]
        elif scaling_type == "zscore":
            mean_val = sum(col_values) / len(col_values)
            std_dev = (sum((x - mean_val) ** 2 for x in col_values) / len(col_values)) ** 0.5
            scaled_data[col] = [(x - mean_val) / std_dev if x is not None else None for x in data[col]]
        else:
            raise ValueError("Unsupported scaling type. Use 'minmax' or 'zscore'.")
    
    return Table(scaled_data)


def encode_categorical(table, columns=None):
    """
    Encode categorical columns using label encoding.
    
    Args:
        table (Table): Input Table object.
        columns (list): List of columns to encode. If None, all non-numeric columns will be encoded.
    
    Returns:
        Table: Table object with encoded categorical columns.
    """
    data = table.data
    columns = columns or [col for col in data if isinstance(data[col][0], str)]
    encoded_data = {col: data[col][:] for col in data}

    for col in columns:
        # Exclude None values before sorting
        unique_values = {v: i for i, v in enumerate(sorted({v for v in data[col] if v is not None}))}
        encoded_data[col] = [unique_values.get(x, None) for x in data[col]]
    
    return Table(encoded_data)



def convert_column_types(table, column_types):
    """
    Convert column types to specified types.
    
    Args:
        table (Table): Input Table object.
        column_types (dict): Dictionary of column names and their target types (e.g., {"col1": int, "col2": float}).
    
    Returns:
        Table: Table object with updated column types.
    """
    data = table.data
    converted_data = {col: data[col][:] for col in data}

    for col, target_type in column_types.items():
        if col not in data:
            raise ValueError(f"Column {col} does not exist in the Table.")
        converted_data[col] = [target_type(value) if value is not None else None for value in data[col]]
    
    return Table(converted_data)


def aggregate_data(table, group_by_columns, agg_column, agg_func):
    """
    Perform data aggregation to summarize datasets.
    
    Args:
        table (Table): Input Table object.
        group_by_columns (list): Columns to group by.
        agg_column (str): Column to aggregate.
        agg_func (function): Aggregation function (e.g., sum, max, min, len, lambda x: sum(x) / len(x)).
    
    Returns:
        Table: Aggregated Table object.
    """
    data = table.data
    if agg_column not in data:
        raise ValueError(f"Aggregation column {agg_column} does not exist in the Table.")
    
    grouped_data = {}
    for i, row in enumerate(zip(*[data[col] for col in group_by_columns])):
        key = tuple(row)
        if key not in grouped_data:
            grouped_data[key] = []
        grouped_data[key].append(data[agg_column][i])
    
    result = {col: [] for col in group_by_columns + [agg_column]}
    for key, values in grouped_data.items():
        for i, value in enumerate(key):
            result[group_by_columns[i]].append(value)
        result[agg_column].append(agg_func(values))
    
    return Table(result)
