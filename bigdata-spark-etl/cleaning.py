from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, regexp_replace, to_timestamp, concat_ws
)
from pyspark.sql.types import IntegerType, DoubleType

# -----------------------------
# 0. Create Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("Sales Data Cleaning") \
    .getOrCreate()

# -----------------------------
# 1. Load data
# -----------------------------
df = spark.read.csv(
    "sales.csv",
    header=True,
    inferSchema=False
)

print("Original count:", df.count())

# -----------------------------
# 2. Trim whitespace from text columns
# -----------------------------
text_cols = ["Product", "Purchase Address", "City", "Product Type"]

for c in text_cols:
    df = df.withColumn(c, trim(col(c)))

# -----------------------------
# 3. Fix numeric columns
# -----------------------------

# Quantity Ordered → numeric
df = df.withColumn(
    "Quantity Ordered",
    col("Quantity Ordered").cast(IntegerType())
)

# Price → remove commas + cast to float
df = df.withColumn(
    "Price",
    regexp_replace(col("Price"), ",", "").cast(DoubleType())
)

# -----------------------------
# 4. Combine Date and Time into Timestamp
# -----------------------------
df = df.withColumn(
    "Order Timestamp",
    to_timestamp(
        concat_ws(" ", col("Order Date"), col("Time")),
        "dd-MM-yyyy hh:mm a"
    )
)

# Drop old date & time columns
df = df.drop("Order Date", "Time")

# -----------------------------
# 5. Remove invalid / empty rows
# -----------------------------
df = df.dropna(
    subset=["Order ID", "Product", "Order Timestamp"]
)

# -----------------------------
# 6. Remove duplicate rows
# -----------------------------
df = df.dropDuplicates()

# -----------------------------
# 7. (Spark has no index → skip reset index)
# -----------------------------

# -----------------------------
# 8. Save cleaned data
# -----------------------------
df.write.mode("overwrite") \
    .option("header", True) \
    .csv("sales_cleaned_final")

print("✅ Data cleaning completed successfully")
print("Final count:", df.count())

df.show(5, truncate=False)
