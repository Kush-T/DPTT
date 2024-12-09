# Data Processing and Transformation Toolkit (DPTT)

## Overview
The **Data Processing and Transformation Toolkit (DPTT)** is a Python package designed to simplify CSV file processing with minimal dependencies. It provides tools for:
- Data cleaning
- Transformation
- Exportation

DPTT is tailored for medium-sized datasets, offering memory-efficient file handling, robust error management, and an intuitive API for data scientists and researchers.

---

## Features
### Key Functionalities:
1. **CSV Parsing**:
   - Custom `parse_csv()` function for line-by-line CSV file processing.
   - Handles various delimiters, escape characters, and quoted fields.
2. **Data Cleaning**:
   - Missing value handling using mean, median, or mode strategies.
   - Data type validation and correction.
   - Basic statistical outlier detection.
3. **Transformations**:
   - Scaling (`minmax`, `z-score`) for numeric data.
   - Categorical data encoding.
4. **Exporting**:
   - Writing `Table` objects back to CSV files with validations.
5. **Core Data Structure**:
   - `Table` class for managing tabular data.

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
   from dptt.csv_handler.parser import CSVParser

   table = CSVParser.parse_csv('data.csv', delimiter=',')
   ```

2. **Cleaning Data**:
   ```python
   from dptt.data_cleaning.cleaner import handle_missing

   clean_table = handle_missing(table, strategy="mean")
   ```

3. **Transforming Data**:
   ```python
   from dptt.transformations.transformations import scale_numeric

   scaled_table = scale_numeric(clean_table, columns=['Age'], scaling_type='zscore')
   ```

4. **Exporting Data**:
   ```python
   from dptt.exporting.export import export_to_file

   export_to_file(scaled_table, 'output.csv')
   ```

---

## Modules and Structure

1. **`csv_handler/parser.py`**:
   - Parses CSV files with flexible options for delimiters and escape sequences.
2. **`data_cleaning/cleaner.py`**:
   - Handles missing values and validates data types.
3. **`transformations/transformations.py`**:
   - Supports scaling and encoding transformations.
4. **`exporting/export.py`**:
   - Exports tabular data to CSV with robust validation.
5. **`table.py`**:
   - Core `Table` class for tabular data operations.

---

## Tests
Unit tests are provided in the `tests/` directory, covering core functionality:
- Parsing
- Data cleaning
- Transformations
- Exporting

To run tests:
```bash
python -m unittest discover dptt/tests
```

---

## Authors
- Kushagra Trivedi
- Shaun Kirthan
- Shubankar Radhakrishna

## License
This project is licensed under the MIT License.

---

Let me know if you would like additional details or further customization!
