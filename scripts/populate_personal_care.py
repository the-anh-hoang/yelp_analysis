import json
import ast
from utils import db_connect

def str_to_bool(val):
    return val in ['True', True, 'true']

def safe_json_str(val):
    if not val or val == 'None':
        return None
    try:
        return json.dumps(ast.literal_eval(val))
    except:
        return None

db = db_connect()
cursor = db.cursor()

insert_query = """
INSERT INTO AttributesPersonalCare (
    business_id, accepts_insurance, hair_specializes_in
) VALUES (%s, %s, %s)
"""

inserted_count = 0
skipped = {}

with open("../data_classified/business_by_cat/personal_care.json", "r", encoding="utf-8") as f:
    businesses = json.load(f)

for business in businesses:
    try:
        attrs = business.get("attributes", {})
        if not attrs:
            continue

        business_id = business["business_id"]
        accepts_insurance = str_to_bool(attrs.get("AcceptsInsurance"))
        hair_specializes_in = safe_json_str(attrs.get("HairSpecializesIn"))

        values = (
            business_id,
            accepts_insurance,
            hair_specializes_in
        )

        cursor.execute(insert_query, values)
        inserted_count += 1

    except Exception as e:
        skipped[business.get("business_id", "UNKNOWN")] = str(e)


print(f"Inserted: {inserted_count}")
print(f"Skipped: {len(skipped)}")

if skipped:
    db.rollback()
    print("Rolled back due to errors.")
else:
    db.commit()
    print("All rows inserted.")
if skipped:
    print("First few skipped:")
    for bid, err in list(skipped.items())[:5]:
        print(f"- {bid}: {err}")

cursor.close()
db.close()
