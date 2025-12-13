import pandas as pd

# Load raw reviews
df = pd.read_csv("data/raw/raw_reviews.csv")
print("Original rows:", len(df))

# 1. Remove empty reviews
df["review"] = df["review"].astype(str).str.strip()
df = df[df["review"] != ""]
print("After removing empty reviews:", len(df))

# 2. Keep duplicates for analysis (skip drop_duplicates)

# 3. Normalize date format
df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
df = df[df["date"].notna()]
print("Rows with valid dates:", len(df))

# 4. Save cleaned data
df.to_csv("data/processed/reviews_cleaned.csv", index=False)
print("Cleaned data saved:", len(df), "reviews")
