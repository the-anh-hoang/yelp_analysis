import json
from utils import db_connect

db = db_connect()
cursor = db.cursor()

insert_query = "INSERT INTO AttributesAutomotive (business_id) VALUES (%s)"

inserted = 0
skipped = {}

with open("../data_classified/business_by_cat/automotive.json", "r", encoding="utf-8") as f:
    businesses = json.load(f)

for business in businesses:
    business_id = business.get("business_id")
    if not business_id:
        continue
    try:
        cursor.execute(insert_query, (business_id,))
        inserted += 1
    except Exception as e:
        skipped[business_id] = str(e)

print(f"\nInserted: {inserted} rows")
print(f"Skipped: {len(skipped)} rows")

if len(skipped) == 0:
    db.commit()
    print("All automotive business IDs inserted successfully.")
else:
    db.rollback()
    print("Insert failed. Rolled back.")
    print("First 5 skipped:")
    for bid, err in list(skipped.items())[:5]:
        print(f"    {bid}: {err}")

cursor.close()
db.close()
