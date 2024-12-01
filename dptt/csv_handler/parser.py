from dptt.table import Table

class CSVParser:
    @classmethod
    def parse_csv(cls, file_path, delimiter=",", escapechar=None, quotechar='"', 
                 header=True, encoding="utf-8", skip_blank_lines=True, max_field_size=None):
        """
        Reads a CSV file and returns a Table object with custom parsing logic.
        
        Args:
            file_path: Path to the CSV file
            delimiter: Delimiter used in the file
            escapechar: Escape character for special characters
            quotechar: Quote character for fields with special characters
            header: Whether the first line is a header
            encoding: File encoding to use
            skip_blank_lines: Whether to skip blank lines in the input
            max_field_size: Maximum allowed size for any field
            
        Returns:
            Table: An instance of the Table class containing the parsed data
            
        Raises:
            ValueError: If the file is empty or if field size exceeds max_field_size
            FileNotFoundError: If the input file doesn't exist
            UnicodeDecodeError: If the file cannot be decoded with the specified encoding
        """
        data = {}
        
        try:
            with open(file_path, mode="r", encoding=encoding) as file:
                lines = [line.strip() for line in file if line.strip() or not skip_blank_lines]
                
                if not lines:
                    raise ValueError("Empty file")
                
                # Parse headers
                headers = cls._parse_headers(lines[0], delimiter, escapechar, quotechar, header)
                data = {header: [] for header in headers}
                
                # Parse data lines
                for line_num, line in enumerate(lines[1:] if header else lines, start=2):
                    try:
                        row = cls._parse_line(line, delimiter, escapechar, quotechar, max_field_size)
                        
                        # Validate row length
                        if len(row) != len(headers):
                            print(f"Warning - Line {line_num}: Expected {len(headers)} fields, got {len(row)}")
                            # Pad or truncate row to match header length
                            row = row[:len(headers)] + [''] * (len(headers) - len(row))
                            
                        for header, value in zip(headers, row):
                            data[header].append(value)
                            
                    except ValueError as e:
                        print(f"Error parsing line {line_num}: {e}")
                        raise
                        
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except UnicodeDecodeError:
            raise UnicodeDecodeError(
                f"Unable to decode file with {encoding} encoding. Try a different encoding."
            )
            
        return Table(data)

    @staticmethod
    def _parse_headers(line, delimiter, escapechar, quotechar, has_header):
        """Parse and validate header row."""
        if has_header:
            headers = CSVParser._parse_line(line, delimiter, escapechar, quotechar)
            # Ensure unique header names
            seen_headers = set()
            for i, header in enumerate(headers):
                original = header
                counter = 1
                while header in seen_headers:
                    header = f"{original}_{counter}"
                    counter += 1
                headers[i] = header
                seen_headers.add(header)
            return headers
        else:
            first_line = CSVParser._parse_line(line, delimiter, escapechar, quotechar)
            return [f"column{i+1}" for i in range(len(first_line))]

    @staticmethod
    def _parse_line(line, delimiter, escapechar, quotechar, max_field_size=None):
        """
        Parses a single line of a CSV file into fields.
        When escapechar is None, backslashes are treated as regular characters.
        """
        fields = []
        field = []  # Use list for efficient string building
        inside_quotes = False
        i = 0

        while i < len(line):
            char = line[i]

            if max_field_size and len(field) > max_field_size:
                raise ValueError(f"Field size exceeds maximum allowed size of {max_field_size}")

            if inside_quotes:
                if char == quotechar:
                    if i + 1 < len(line) and line[i + 1] == quotechar:
                        field.append(quotechar)
                        i += 1
                    else:
                        inside_quotes = False
                else:
                    field.append(char)
            else:
                if char == quotechar:
                    inside_quotes = True
                elif char == delimiter:
                    fields.append(''.join(field))
                    field = []
                elif escapechar and char == escapechar and i + 1 < len(line):
                    # Process escape sequences only if escapechar is not None
                    field.append(line[i + 1])
                    i += 1
                else:
                    # Append regular characters, including backslashes, as-is
                    field.append(char)
            i += 1

        if inside_quotes:
            raise ValueError("Unmatched quotes in line")

        fields.append(''.join(field))
        return fields
