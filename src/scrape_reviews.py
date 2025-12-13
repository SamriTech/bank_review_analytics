from google_play_scraper import Sort, reviews
import pandas as pd
import time

# Updated App IDs
APPS = {
    "CBE": "com.combanketh.mobilebanking",
    "BOA": "com.boa.boaMobileBanking",
    "Dashen": "com.dashen.dashensuperapp"
}

all_reviews = []

# Function to scrape reviews per bank in batches
def scrape_bank(bank, app_id, total_count=400, batch=100):
    bank_reviews = []
    for start in range(0, total_count, batch):
        result, _ = reviews(
            app_id,
            lang="en",
            country="us",  # 'us' is more reliable than 'et'
            sort=Sort.NEWEST,
            count=batch
        )
        bank_reviews.extend(result)
        time.sleep(1)  # polite pause to avoid being blocked
    return bank_reviews

# Loop through each bank
for bank, app_id in APPS.items():
    print(f"Scraping {bank}...")
    result = scrape_bank(bank, app_id, total_count=400, batch=100)
    print(f"{bank}: {len(result)} reviews scraped")

    if not result:
        print(f"WARNING: No reviews found for {bank}")

    # Append to the main list
    for r in result:
        all_reviews.append({
            "review": r["content"],
            "rating": r["score"],
            "date": r["at"],
            "bank": bank,
            "source": "Google Play"
        })

# Convert all reviews to DataFrame
df = pd.DataFrame(all_reviews)
df.to_csv("data/raw/raw_reviews.csv", index=False)

# Print final counts per bank
print("Reviews per bank:")
for bank in APPS.keys():
    count = len(df[df["bank"] == bank])
    print(f"{bank}: {count} reviews")

print("Total reviews saved:", len(df))
