# types.py

from datetime import datetime

class ColumnType:
    """Base class for column types"""
    name = "unknown"
    
    def validate(self, value):
        """Check if a string value matches this type's pattern"""
        raise NotImplementedError
        
    def convert(self, value):
        """Convert a string value to this type"""
        raise NotImplementedError

class StringType(ColumnType):
    name = "string"
    
    def validate(self, value):
        return True  # All values are valid strings
        
    def convert(self, value):
        return str(value)

class IntegerType(ColumnType):
    name = "integer"
    
    def validate(self, value):
        try:
            int(value)
            return True
        except ValueError:
            return False
    
    def convert(self, value):
        return int(value)

class FloatType(ColumnType):
    name = "float"
    
    def validate(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def convert(self, value):
        return float(value)

class DateType(ColumnType):
    name = "date"
    formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d"
    ]
    
    def validate(self, value):
        for fmt in self.formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        return False
    
    def convert(self, value):
        for fmt in self.formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        raise ValueError(f"Could not parse date: {value}")

class BooleanType(ColumnType):
    name = "boolean"
    true_values = {'true', 'yes', '1', 't', 'y'}
    false_values = {'false', 'no', '0', 'f', 'n'}
    
    def validate(self, value):
        return value.lower() in self.true_values or value.lower() in self.false_values
    
    def convert(self, value):
        value = value.lower()
        if value in self.true_values:
            return True
        if value in self.false_values:
            return False
        raise ValueError(f"Invalid boolean value: {value}")

def infer_column_type(values, sample_size=100):
    """
    Infer the type of a column based on a sample of values.
    Returns an instance of the most specific type that can handle all non-empty values.
    """
    # Sample the values, excluding empty strings
    sample = [v for v in values[:sample_size] if v.strip()]
    if not sample:
        return StringType()
    
    # Try types from most specific to least specific
    types_to_try = [
        IntegerType(),
        FloatType(),
        DateType(),
        BooleanType(),
        StringType()
    ]
    
    for type_instance in types_to_try:
        if all(type_instance.validate(value) for value in sample):
            return type_instance
            
    return StringType()  # Default to string if no other type matches