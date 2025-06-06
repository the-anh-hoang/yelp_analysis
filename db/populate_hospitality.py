import json
import ast
from utils import db_connect


def str_to_bool(val):
    return val in ['True', True, 'true']

def safe_int(val):
    try:
        return int(val)
    except:
        return None

def safe_enum(val, allowed):
    if not isinstance(val, str):
        return None
    val = val.strip("u' ").lower().replace(" ", "_")
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
INSERT INTO AttributesHospitality (
    business_id, price_range, alcohol, noise_level, smoking, attire,
    reservations, good_for_groups, delivery, cater, dogs_allowed,
    ambience, good_for_meal, music
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

inserted = 0
skipped = {}


with open("../data_classified/business_by_cat/hospitality.json", "r", encoding="utf-8") as f:
    businesses = json.load(f)

for business in businesses:
    try:
        attrs = business.get("attributes", {})
        if not attrs:
            continue

        business_id = business["business_id"]

        values = (
            business_id,
            safe_int(attrs.get("RestaurantsPriceRange2")),
            safe_enum(attrs.get("Alcohol"), ["none", "beer_and_wine", "full_bar"]),
            safe_enum(attrs.get("NoiseLevel"), ["quiet", "average", "loud", "very loud"]),
            safe_enum(attrs.get("Smoking"), ["no", "yes", "outdoor"]),
            safe_enum(attrs.get("RestaurantsAttire"), ["casual", "dressy", "formal"]),
            str_to_bool(attrs.get("RestaurantsReservations")),
            str_to_bool(attrs.get("RestaurantsGoodForGroups")),
            str_to_bool(attrs.get("RestaurantsDelivery")),
            str_to_bool(attrs.get("Caters")),
            str_to_bool(attrs.get("DogsAllowed")),
            safe_json_str(attrs.get("Ambience")),
            safe_json_str(attrs.get("GoodForMeal")),
            safe_json_str(attrs.get("Music"))
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
    print("First few skipped rows:")
    for bid, err in list(skipped.items())[:5]:
        print(f"    {bid}: {err}")

cursor.close()
db.close()
