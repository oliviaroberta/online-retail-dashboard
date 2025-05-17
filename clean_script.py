import pandas as pd

# Step 1: Load the CSV file
df = pd.read_csv("online_retail.csv", encoding='ISO-8859-1')  # Use correct encoding for special characters

# Step 2: Drop rows with missing CustomerID or Description
df = df.dropna(subset=['Customer ID', 'Description'])

# Step 3: Remove rows with Quantity <= 0 or UnitPrice <= 0
df = df[(df['Quantity'] > 0) & (df['Price'] > 0)]

# Step 4: Strip whitespace from text columns
df['Description'] = df['Description'].str.strip()
df['Country'] = df['Country'].str.strip()

# Step 5: Remove duplicate rows
df = df.drop_duplicates()

# Step 6: Save the cleaned data
df.to_csv("online_retail_cleaned.csv", index=False)

print("✅ Data cleaning complete. Clean file saved as 'online_retail_cleaned.csv'.")
