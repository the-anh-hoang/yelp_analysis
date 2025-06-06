from utils import db_connect
import json


db = db_connect()
cursor = db.cursor()

query = """
    INSERT INTO Business (
        business_id, name, address, city, state, postal_code,
        latitude, longitude, stars, review_count, is_open, categories
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
PATH = "../data_raw/Yelp JSON/yelp_academic_dataset_business.json"

# Status variables
skipped_business_id = {}
inserted_count = 0

with open(PATH, "r", encoding='utf-8') as f:
    for line in f:
        business = json.loads(line)
        business_id = business["business_id"]
        name = business["name"]
        address = business["address"]
        city = business["city"]
        state = business["state"]
        postal_code = business["postal_code"]
        latitude = business["latitude"]
        longitude = business["longitude"]
        stars = business["stars"]
        review_count = business["review_count"]
        if business["is_open"]:
            is_open = True
        else:
            is_open = False
        categories = business["categories"]
        
        values = (business_id, name, address, city, state, postal_code, latitude,
                  longitude, stars, review_count, is_open, categories)
        try:
            cursor.execute(query, values)
            inserted_count += 1
        except Exception as e:
            skipped_business_id[business_id] = e
            
print(f"\nInserted: {inserted_count} businesses")
print(f"Skipped: {len(skipped_business_id)} businesses")

if len(skipped_business_id) == 0:
    db.commit()
    print(cursor.rowcount, "record inserted.")
    print("All rows inserted successfully.")
else:
    db.rollback()
    print("Insert failed. Transaction rolled back.")
    print("First few skipped records:")
    for bid, err in list(skipped_business_id.items())[:5]:
        print(f"- {bid}: {err}")