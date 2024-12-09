# Data Processing and Transformation Toolkit (DPTT)

## Overview
The **Data Processing and Transformation Toolkit (DPTT)** is a Python package designed to simplify CSV file processing with minimal dependencies. It provides tools for:
- Data parsing
- Data cleaning
- Transformation
- Exportation

DPTT is tailored for medium-sized datasets, offering memory-efficient file handling, robust error management, and an intuitive API for data scientists and researchers.

---

## Features
### Key Functionalities:
1. **Core Data Structure**:
   - `Table` class for managing tabular data.
   - Custom columnar storage data structure
   - Dynamically adjusts column widths when printing for better readability.
2. **Type Inference**:
   - Automatic type inference for columns based on data samples.
   - Supports `integer`, `float`, `date`, `boolean`, and `string` types.
   - Customizable type inference sample size.
3. **CSV Parsing**:
   - Custom `parse_csv()` function for line-by-line CSV file processing.
   - Handles various delimiters, escape characters, and quoted fields.
   - Supports memory-efficient parsing for large files.
4. **Data Cleaning**:
   - Missing value handling using mean, median, or mode strategies.
   - Data type validation and correction.
5. **Transformations**:
   - Scaling (`minmax`, `z-score`) for numeric data.
   - Categorical data encoding.
6. **Exporting**:
   - Writing `Table` objects back to CSV files with validations.
---

## Installation
To install the DPTT package, clone this repository and include the directory in your Python project:

```bash
git clone https://github.com/Kush-T/DPTT.git
cd DPTT
```

---

## Usage

### Example Workflow

1. **Parsing a CSV File**:
   ```python
   from dptt import CSVParser

   table = CSVParser.parse_csv('data.csv', delimiter=',')
   ```

2. **Cleaning Data**:
   ```python
   from dptt import fill_missing

   clean_table = fill_missing(table, strategy="mean")
   ```

3. **Transforming Data**:
   ```python
   from dptt import scale_numeric

   scaled_table = scale_numeric(clean_table, columns=['Age'], scaling_type='zscore')
   ```

4. **Exporting Data**:
   ```python
   from dptt import export_to_file

   export_to_file(scaled_table, 'output.csv')
   ```

---

## Modules and Structure

1. **`table.py`**:
   - Core `Table` class for tabular data operations.
2. **`csv_handler/parser.py`**:
   - Parses CSV files with flexible options for delimiters and escape sequences.
3. **`data_cleaning/cleaner.py`**:
   - Handles missing values and validates data types.
4. **`transformations/transformations.py`**:
   - Supports scaling and encoding transformations.
5. **`exporting/export.py`**:
   - Exports tabular data to CSV with robust validation.


---

## Tests
Unit tests are provided in the `tests/` directory, covering core functionality:
- Table structure and type validations
- Parsing
- Data cleaning
- Transformations
- Exporting

To run tests:
```bash
python tests.py
```

---

## Authors
- Kushagra Trivedi
- Shaun Kirthan
- Shubankar Radhakrishna

## License

