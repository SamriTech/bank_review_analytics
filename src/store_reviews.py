import pandas as pd
import psycopg2

# 1. Credentials (Update with YOUR password)
DB_CONFIG = {
    "dbname": "bank_reviews",
    "user": "postgres",
    "password": "6565",
    "host": "localhost"
}

def store_to_database():
    # Load your Task 2 data
    df = pd.read_csv("data/processed/analyzed_reviews.csv")
    
    # Establish the connection
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Step A: Populate the 'banks' table first
    unique_banks = df['bank'].unique()
    for bank in unique_banks:
        cur.execute("INSERT INTO banks (bank_name) VALUES (%s) ON CONFLICT DO NOTHING", (bank,))
    conn.commit()

    # Get a map of bank names to IDs (e.g., {'CBE': 1})
    cur.execute("SELECT bank_name, bank_id FROM banks")
    bank_id_map = dict(cur.fetchall())

    # Step B: Insert all reviews
    print("Uploading reviews to database...")
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO reviews (bank_id, review_text, rating, review_date, sentiment_label, sentiment_score, theme)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            bank_id_map[row['bank']],
            row['review'],
            row['rating'],
            row['date'],
            row['sentiment_label'],
            row['sentiment_score'],
            row['theme']
        ))
    
    conn.commit() # Make sure changes are saved!
    cur.close()
    conn.close()
    print("Task 3 Complete! Check pgAdmin to see your data.")

if __name__ == "__main__":
    store_to_database()