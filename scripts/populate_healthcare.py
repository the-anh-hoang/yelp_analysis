import json
from utils import db_connect

def str_to_bool(val):
    return val in ['True', True, 'true']

db = db_connect()
cursor = db.cursor()

insert_query = """
INSERT INTO AttributesHealthcare (business_id, accepts_insurance)
VALUES (%s, %s)
"""

inserted = 0
skipped = {}

with open("../data_classified/business_by_cat/healthcare.json", "r", encoding="utf-8") as f:
    businesses = json.load(f)

for business in businesses:
    business_id = business.get("business_id")
    attrs = business.get("attributes", {})
    if not business_id or not attrs:
        continue

    try:
        accepts_insurance = str_to_bool(attrs.get("AcceptsInsurance"))
        cursor.execute(insert_query, (business_id, accepts_insurance))
        inserted += 1
    except Exception as e:
        skipped[business_id] = str(e)

print(f"\nInserted: {inserted} rows")
print(f"Skipped: {len(skipped)} rows")

if len(skipped) == 0:
    db.commit()
    print("All rows inserted successfully.")
else:
    db.rollback()
    print("Insert failed. Rolled back.")
    for bid, err in list(skipped.items())[:5]:
        print(f"    {bid}: {err}")

cursor.close()
db.close()
