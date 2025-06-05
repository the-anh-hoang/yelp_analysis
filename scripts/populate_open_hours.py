import json
from utils import db_connect


PATH = "../data_raw/Yelp JSON/yelp_academic_dataset_business.json"
day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
rows = []

with open(PATH, "r", encoding="utf-8") as f:
    for line in f:
        business = json.loads(line)
        business_id = business["business_id"]
        hours = business.get("hours")

        # Normalize all 7 days
        if hours == None:
            hour_row = {
                "business_id": business_id,
                **{day.lower(): None for day in day_order}
            }
        else: 
            hour_row = {
                "business_id": business_id,
                **{day.lower(): hours.get(day, "Closed") for day in day_order}
            }

        rows.append(hour_row)


db = db_connect()

cursor = db.cursor()
sql = """INSERT INTO OpenHour (business_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        
skipped_business_id = {} # for debugging 
for row in rows:
    values = (
        row["business_id"],
        row["monday"],
        row["tuesday"],
        row["wednesday"],
        row["thursday"],
        row["friday"],
        row["saturday"],
        row["sunday"]
    )
    try:
        cursor.execute(sql, values)
    except Exception as e:
        skipped_business_id[row["business_id"]] = e


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
    