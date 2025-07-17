import pandas as pd

# Full path to your file
file_path = "/media/dev/Expansion/Big_data/raw_data.csv"

# Read first 5 columns and 5 rows
df = pd.read_csv(file_path, usecols=range(5), nrows=5)

# Display
print(df)
