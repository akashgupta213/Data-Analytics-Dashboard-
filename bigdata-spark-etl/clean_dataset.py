import pandas as pd

#load dataset
df = pd.read_csv("sales.csv")   # change filename if needed

print("Original shape:", df.shape)


# 2. Trim whitespace from text columns

text_cols = ["Product", "Purchase Address", "City", "Product Type"]

for col in text_cols:
    df[col] = df[col].astype(str).str.strip()


# 3. Fix numeric columns

# Quantity Ordered
df["Quantity Ordered"] = pd.to_numeric(
    df["Quantity Ordered"], errors="coerce"
)

# Price (remove commas)
df["Price"] = (
    df["Price"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .astype(float)
)


# 4. Combine Date and Time into Timestamp

df["Order Timestamp"] = pd.to_datetime(
    df["Order Date"] + " " + df["Time"],
    format="%d-%m-%Y %I:%M %p",
    errors="coerce"
)

# Drop old date & time columns
df.drop(columns=["Order Date", "Time"], inplace=True)


# 5. Remove invalid / empty rows

df.dropna(
    subset=["Order ID", "Product", "Order Timestamp"],
    inplace=True
)


# 6. Remove duplicate rows (if any)

df.drop_duplicates(inplace=True)


# 7. Reset index

df.reset_index(drop=True, inplace=True)


# 8. Save cleaned data

df.to_csv("sales_cleaned_final.csv", index=False)

print("✅ Data cleaning completed successfully")
print("Final shape:", df.shape)
print(df.head())
