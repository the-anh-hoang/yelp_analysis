import json
import ast
from utils import db_connect


def str_to_bool(val):
    return val in ['True', True, 'true']

def safe_enum(val, allowed):
    if not isinstance(val, str):
        return None
    val = val.strip("u' ").replace("_", " ").lower()
    return val if val in allowed else None

def safe_json(val):
    if not val or val == 'None':
        return None
    try:
        return ast.literal_eval(val)
    except:
        return None

def safe_json_str(val):
    try:
        return json.dumps(safe_json(val))
    except:
        return None


db = db_connect()
cursor = db.cursor()


insert_query = """
INSERT INTO AttributesRetail (
    business_id, caters, outdoor_seating, reservations, drive_through,
    has_tv, table_service, alcohol, coat_check, noise_level,
    good_for_groups, attire, ambience, smoking, accepts_insurance,
    open_24_hours, music, good_for_dancing
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

inserted = 0
skipped = {}


with open("../data_classified/business_by_cat/retail.json", "r", encoding="utf-8") as f:
    businesses = json.load(f)

for business in businesses:
    try:
        attrs = business.get("attributes", {})
        if not attrs:
            continue

        business_id = business["business_id"]
        values = (
            business_id,
            str_to_bool(attrs.get("Caters")),
            str_to_bool(attrs.get("OutdoorSeating")),
            str_to_bool(attrs.get("RestaurantsReservations")),
            str_to_bool(attrs.get("DriveThru")),
            str_to_bool(attrs.get("HasTV")),
            str_to_bool(attrs.get("RestaurantsTableService")),
            safe_enum(attrs.get("Alcohol"), ["none", "beer_and_wine", "full_bar"]),
            str_to_bool(attrs.get("CoatCheck")),
            safe_enum(attrs.get("NoiseLevel"), ["quiet", "average", "loud", "very_loud"]),
            str_to_bool(attrs.get("RestaurantsGoodForGroups")),
            safe_enum(attrs.get("RestaurantsAttire"), ["casual", "dressy", "formal"]),
            safe_json_str(attrs.get("Ambience")),
            safe_enum(attrs.get("Smoking"), ["no", "yes", "outdoor"]),
            str_to_bool(attrs.get("AcceptsInsurance")),
            str_to_bool(attrs.get("Open24Hours")),
            safe_json_str(attrs.get("Music")),
            str_to_bool(attrs.get("GoodForDancing"))
        )

        cursor.execute(insert_query, values)
        inserted += 1

    except Exception as e:
        skipped[business.get("business_id", "UNKNOWN")] = str(e)


print(f"\nInserted: {inserted} rows")
print(f"Skipped: {len(skipped)} rows")

if len(skipped) == 0:
    db.commit()
    print("All rows inserted successfully.")
else:
    db.rollback()
    print("Insert failed. Rolled back.")
    print("First few skipped:")
    for bid, err in list(skipped.items())[:5]:
        print(f"    {bid}: {err}")


cursor.close()
db.close()
