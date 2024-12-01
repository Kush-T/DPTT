from dptt import CSVParser

def test_csv_with_header():
    print("\n--- Test: CSV with Header ---")
    try:
        data = CSVParser.parse_csv("test_with_header.csv")
        print("Parsed Data (with header):")
        data.print_rows()
    except Exception as e:
        print(f"Error in test_csv_with_header: {e}")

def test_csv_without_header():
    print("\n--- Test: CSV without Header ---")
    try:
        data = CSVParser.parse_csv("test_without_header.csv", header=False)
        print("Parsed Data (without header):")
        data.print_rows()
    except Exception as e:
        print(f"Error in test_csv_without_header: {e}")

def test_csv_with_custom_delimiter():
    print("\n--- Test: CSV with Custom Delimiter ---")
    try:
        data = CSVParser.parse_csv("test_custom_delimiter.csv", delimiter="|")
        print("Parsed Data (custom delimiter '|'):")
        data.print_rows()
    except Exception as e:
        print(f"Error in test_csv_with_custom_delimiter: {e}")

def test_csv_with_quoted_fields():
    print("\n--- Test: CSV with Quoted Fields ---")
    try:
        data = CSVParser.parse_csv("test_quoted_fields.csv")
        print("Parsed Data (with quoted fields):")
        data.print_rows()
    except Exception as e:
        print(f"Error in test_csv_with_quoted_fields: {e}")

def test_csv_with_escape_characters():
    print("\n--- Test: CSV with Escape Characters ---")
    try:
        data = CSVParser.parse_csv("test_escape_characters.csv",escapechar='\\')
        print("Parsed Data (with escape characters):")
        data.print_rows()
    except Exception as e:
        print(f"Error in test_csv_with_escape_characters: {e}")

def test_malformed_csv():
    print("\n--- Test: Malformed CSV ---")
    try:
        data = CSVParser.parse_csv("test_malformed.csv", delimiter=",", header=True)
        print("Parsed Data (malformed):")
        data.print_rows()
    except Exception as e:
        print(f"Error in test_malformed_csv: {e}")

if __name__ == "__main__":
    # Test cases
    test_csv_with_header()
    test_csv_without_header()
    test_csv_with_custom_delimiter()
    test_csv_with_quoted_fields()
    test_csv_with_escape_characters()
    test_malformed_csv()