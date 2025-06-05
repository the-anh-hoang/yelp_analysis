import json
import ast
from utils import db_connect

def str_to_bool(val):
    return val in ['True', True, 'true']

def clean_wifi(raw):
    if not raw:
        return None
    if 'free' in raw:
        return 'free'
    elif 'paid' in raw:
        return 'paid'
    else:
        return 'no'
    
def extract_parking(parking_str):
    """
    Safely parses BusinessParking strings like "{'garage': False, ...}"
    Returns a dictionary or an empty dict on failure.
    """
    if not parking_str or parking_str == 'None':
        return {}
    try:
        return ast.literal_eval(parking_str)
    except (ValueError, SyntaxError):
        return {}

def get_parking_values(parking_dict):
    """
    Returns parking info in fixed order, or (None,...) if not a valid dict.
    """
    if not isinstance(parking_dict, dict):
        return (None, None, None, None, None)
    return (
        bool(parking_dict.get("garage")) if parking_dict.get("garage") is not None else None,
        bool(parking_dict.get("street")) if parking_dict.get("street") is not None else None,
        bool(parking_dict.get("validated")) if parking_dict.get("validated") is not None else None,
        bool(parking_dict.get("lot")) if parking_dict.get("lot") is not None else None,
        bool(parking_dict.get("valet")) if parking_dict.get("valet") is not None else None,
    )


db = db_connect()
cursor = db.cursor()


insert_query = """
INSERT INTO AttributesCommon (
    business_id, accepts_credit_cards, accepts_bitcoin, by_appointment_only, wifi,
    bike_parking, wheel_chair_accessible, parking_garage, parking_street,
    parking_validated, parking_lot, parking_valet
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


inserted_count = 0
skipped_business_id = {}

with open("data_raw/Yelp JSON/yelp_academic_dataset_business.json", "r", encoding="utf-8") as f:
    for line in f:
        try:
            business = json.loads(line)
            attrs = business.get("attributes", {})
            if not attrs:
                continue

            business_id = business["business_id"]
            accepts_credit_cards = str_to_bool(attrs.get("BusinessAcceptsCreditCards"))
            accepts_bitcoin = str_to_bool(attrs.get("AcceptsBitcoin"))
            by_appointment_only = str_to_bool(attrs.get("ByAppointmentOnly"))
            raw_wifi = attrs.get("WiFi")
            wifi = clean_wifi(attrs.get("WiFi"))
            bike_parking = str_to_bool(attrs.get("BikeParking"))
            wheel_chair_accessible = str_to_bool(attrs.get("WheelchairAccessible"))
            parking_dict = extract_parking(attrs.get("BusinessParking", "{}"))
            parking_values = get_parking_values(parking_dict)

            values = (
                business_id,
                accepts_credit_cards,
                accepts_bitcoin,
                by_appointment_only,
                wifi,
                bike_parking,
                wheel_chair_accessible,
                *parking_values
            )

            cursor.execute(insert_query, values)
            inserted_count += 1

        except Exception as e:
            skipped_business_id[business.get("business_id", "UNKNOWN")] = str(e)


print(f"\nInserted: {inserted_count} businesses")
print(f"Skipped: {len(skipped_business_id)} businesses")

if len(skipped_business_id) == 0:
    db.commit() 
    print("All rows inserted successfully.")
else:
    db.rollback()
    print("Insert failed. Transaction rolled back.")
    print("First 5 skipped records:")
    for bid, err in list(skipped_business_id.items())[:5]:
        print(f"    {bid}: {err}")

# === Clean up ===
cursor.close()
db.close()
