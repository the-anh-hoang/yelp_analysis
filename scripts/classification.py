import json
import requests
from pathlib import Path
from tqdm import tqdm

# ---- Configuration ----
INPUT_PATH = "../data_raw/Yelp JSON/yelp_academic_dataset_business.json"
OUTPUT_DIR = "../data/business_by_cat"
PROGRESS_PATH = f"{OUTPUT_DIR}/progress_by_cat.json"
MODEL = "mistral"
ISA_TYPES = ["automotive", "healthcare", "hospitality", "nightlife", "personal care", "restaurant", "retail"]

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# ---- Load progress ----
try:
    with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
        processed_ids = set(json.load(f))
except FileNotFoundError:
    processed_ids = set()

# ---- Load existing output files ----
businesses_by_type = {isa: [] for isa in ISA_TYPES}
for isa in ISA_TYPES:
    out_path = f"{OUTPUT_DIR}/{isa}.json"
    if Path(out_path).exists():
        with open(out_path, "r", encoding="utf-8") as f:
            businesses_by_type[isa] = json.load(f)

# ---- Build prompt ----
def build_prompt(name, categories):
   return f"""
    You are a classifier that assigns a business to one of the following types:
    automotive, healthcare, hospitality, nightlife, personal care, restaurant, retail.

    Business name: "{name}"
    Categories: {categories}

    Just respond with one of the 7 types (no explanation).
    """

# ---- Call Ollama API ----

def classify_with_ollama_api(name, categories):
    prompt = build_prompt(name, categories)
    response = requests.post("http://localhost:11434/api/chat", json={
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False
    }, timeout=30)

    content = response.json()["message"]["content"].strip().lower()
    for isa in ISA_TYPES:
        if isa in content:
            return isa
    return None

# ---- Main loop ----
buffer_by_type = {isa: [] for isa in ISA_TYPES}

with open(INPUT_PATH, "r", encoding="utf-8") as f:
    i = 0
    for line in tqdm(f, desc="Classifying businesses"):
        biz = json.loads(line)
        biz_id = biz.get("business_id")
        if not biz_id or biz_id in processed_ids:
            continue

        name = biz.get("name", "")
        categories = biz.get("categories", "")
        isa_type = classify_with_ollama_api(name, categories)

        if isa_type:
            buffer_by_type[isa_type].append(biz)
            processed_ids.add(biz_id)
            i += 1

            # Every 1000 classified businesses → flush to disk
            if i % 10000 == 0:
                for isa in ISA_TYPES:
                    if buffer_by_type[isa]:
                        businesses_by_type[isa].extend(buffer_by_type[isa])
                        with open(f"{OUTPUT_DIR}/{isa}.json", "w", encoding="utf-8") as f_out:
                            json.dump(businesses_by_type[isa], f_out, indent=2)
                        buffer_by_type[isa] = []  # Clear buffer

                # Save progress.json
                with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
                    json.dump(list(processed_ids), f)

print("✅ All businesses classified and saved.")
