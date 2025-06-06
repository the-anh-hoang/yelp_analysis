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

def safe_json_str(val):
    try:
        return json.dumps(ast.literal_eval(val)) if val not in [None, 'None'] else None
    except:
        return None

# === DB Connection ===
db = db_connect()
cursor = db.cursor()

# === Insert Query ===
insert_query = """
INSERT INTO AttributesNightlife (
    business_id, alcohol, ambience, best_nights, good_for_meal,
    noise_level, smoking, restaurant_price_range, restaurant_attire,
    restaurant_reservation, restaurant_delivery, restaurant_table_service,
    happy_hour, coat_check, good_for_dancing, caters, has_tv, dogs_allowed
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

inserted = 0
skipped = {}

# === Load Data ===
with open("../data_classified/business_by_cat/nightlife.json", "r", encoding="utf-8") as f:
    businesses = json.load(f)

for business in businesses:
    try:
        attrs = business.get("attributes", {})
        if not attrs:
            continue

        business_id = business["business_id"]
        values = (
            business_id,
            safe_enum(attrs.get("Alcohol"), ["none", "beer_and_wine", "full_bar"]),
            safe_json_str(attrs.get("Ambience")),
            safe_json_str(attrs.get("BestNights")),
            safe_json_str(attrs.get("GoodForMeal")),
            safe_enum(attrs.get("NoiseLevel"), ["quiet", "average", "loud", "very loud"]),
            safe_enum(attrs.get("Smoking"), ["no", "yes", "outdoor"]),
            safe_int(attrs.get("RestaurantsPriceRange2")),
            attrs.get("RestaurantsAttire"),  
            str_to_bool(attrs.get("RestaurantsReservations")),
            str_to_bool(attrs.get("RestaurantsDelivery")),
            str_to_bool(attrs.get("RestaurantsTableService")),
            str_to_bool(attrs.get("HappyHour")),
            str_to_bool(attrs.get("CoatCheck")),
            str_to_bool(attrs.get("GoodForDancing")),
            str_to_bool(attrs.get("Caters")),
            str_to_bool(attrs.get("HasTV")),
            str_to_bool(attrs.get("DogsAllowed"))
        )

        cursor.execute(insert_query, values)
        inserted += 1

    except Exception as e:
        skipped[business.get("business_id", "UNKNOWN")] = str(e)

# === Commit or Rollback ===
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
