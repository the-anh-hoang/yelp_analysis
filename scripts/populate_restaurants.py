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
    if isinstance(val, str):
        val = val.strip("'u ")  
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
INSERT INTO AttributesRestaurant (
    business_id, price_range, alcohol, noise_level, smoking, attire,
    restaurant_takeout, restaurant_delivery, restaurant_reservations, restaurant_good_for_groups,
    restaurant_table_service, restaurant_counter_service, caters, has_tv, drive_thru,
    happy_hour, dogs_allowed, coat_check, good_for_dancing, byob, corkage, open_24_hours,
    ambience, music, good_for_meal, best_nights, byo_b_corkage, ages_allowed, dietary_restrictions
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


inserted_count = 0
skipped = {}


with open("../data_classified/business_by_cat/restaurant.json", "r", encoding="utf-8") as f:
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
            safe_enum(attrs.get("NoiseLevel"), ["quiet", "average", "loud", "very_loud"]),
            safe_enum(attrs.get("Smoking"), ["no", "yes", "outdoor"]),
            safe_enum(attrs.get("RestaurantsAttire"), ["casual", "dressy", "formal"]),
            str_to_bool(attrs.get("RestaurantsTakeOut")),
            str_to_bool(attrs.get("RestaurantsDelivery")),
            str_to_bool(attrs.get("RestaurantsReservations")),
            str_to_bool(attrs.get("RestaurantsGoodForGroups")),
            str_to_bool(attrs.get("RestaurantsTableService")),
            str_to_bool(attrs.get("RestaurantsCounterService")),
            str_to_bool(attrs.get("Caters")),
            str_to_bool(attrs.get("HasTV")),
            str_to_bool(attrs.get("DriveThru")),
            str_to_bool(attrs.get("HappyHour")),
            str_to_bool(attrs.get("DogsAllowed")),
            str_to_bool(attrs.get("CoatCheck")),
            str_to_bool(attrs.get("GoodForDancing")),
            str_to_bool(attrs.get("BYOB")),
            str_to_bool(attrs.get("Corkage")),
            str_to_bool(attrs.get("Open24Hours")),
            safe_json_str(attrs.get("Ambience")),
            safe_json_str(attrs.get("Music")),
            safe_json_str(attrs.get("GoodForMeal")),
            safe_json_str(attrs.get("BestNights")),
            safe_enum(attrs.get("BYOBCorkage"), ["no", "yes_free", "yes_corkage"]),
            safe_enum(attrs.get("AgesAllowed"), ["allages", "21plus", "18plus"]),
            safe_json_str(attrs.get("DietaryRestrictions"))
        )

        cursor.execute(insert_query, values)
        inserted_count += 1

    except Exception as e:
        skipped[business.get("business_id", "UNKNOWN")] = str(e)


print(f"\nInserted: {inserted_count} rows")
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
