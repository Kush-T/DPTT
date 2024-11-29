import dptt

# Read the CSV data
data = dptt.read_csv("test.csv")

# Print the data structure
print(data)  # Output: DataFrame(columns=['Name', 'Age', 'Location'], rows=3)

# Print rows directly
data.print_rows()
