"""
upload_csv.py
-------------
Reads a CSV of contacts and uploads them to Firestore (project: va-solutions-eadd3).
- Adds a tag 'Bernadette' to every record
- Normalizes multi-value fields into arrays
- Generates 'search_keywords' (for client-side search)
- Safe to re-run (merge updates)
Usage:
  python3 upload_csv.py contacts.csv
Prereqs:
  pip install google-cloud-firestore google-auth
  # EITHER export ADC:
  export GOOGLE_APPLICATION_CREDENTIALS="serviceAccount.json"
  # OR set SERVICE_ACCOUNT_FILE env var to point to the JSON:
  export SERVICE_ACCOUNT_FILE="serviceAccount.json"
"""

import csv
import sys
import os
import datetime
from google.cloud import firestore
from google.oauth2 import service_account

# ---------- Config ----------
CSV_FILE = "contacts.csv"
COLLECTION_NAME = "contacts"
PROJECT_ID = "va-solutions-eadd3"  # <- your Firebase projectId
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "serviceAccount.json")


# ---------- Helpers ----------
def normalize_list(field_value: str):
    """Turns semicolon/comma-separated strings into a clean list."""
    if not field_value:
        return []
    parts = [p.strip() for p in field_value.replace(";", ",").split(",")]
    return [p for p in parts if p]

def build_search_keywords(row: dict) -> str:
    """Lowercase bag-of-words to power client-side fuzzy search."""
    fields = [
        row.get("FullName", ""),
        row.get("job_title", ""),
        row.get("department", ""),
        row.get("Company", ""),
        row.get("Email", ""),
        row.get("Address", ""),
        row.get("city", ""),
        row.get("country", ""),
        row.get("org_type", ""),
    ]
    return " ".join(fields).lower().strip()

def safe_id(text: str) -> str:
    if not text:
        return ""
    t = (
        text.strip().lower()
        .replace("@", "_at_")
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace("#", "_")
        .replace("?", "_")
        .replace("&", "_")
        .replace("%", "_")
        .replace(":", "_")
        .replace("|", "_")
    )
    return "".join(ch for ch in t if ch.isalnum() or ch in "._-")[:200]

def get_db():
    # Prefer explicit service account file if present
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        return firestore.Client(project=PROJECT_ID, credentials=creds)
    # Fall back to ADC if GOOGLE_APPLICATION_CREDENTIALS is set
    return firestore.Client(project=PROJECT_ID)


# ---------- Main ----------
def upload_csv():
    required_headers = [
        "FullName","job_title","department","Company","org_type","Address","city","country",
        "country_code","office_number","mobile_number","fax_number","Email","Website","updated_at"
    ]

    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = CSV_FILE

    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV not found: {csv_file}")

    db = get_db()

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = [h for h in required_headers if h not in reader.fieldnames]
        if missing:
            raise ValueError(f"CSV missing headers: {missing}\nFound headers: {reader.fieldnames}")

        total, created, updated = 0, 0, 0
        for row in reader:
            total += 1

            # Build a stable document id from FullName + Company (fallback to auto-id if blank)
            base_id = "_".join(filter(None, [safe_id(row.get("FullName", "")), safe_id(row.get("Company", ""))]))
            doc_ref = db.collection(COLLECTION_NAME).document(base_id) if base_id else db.collection(COLLECTION_NAME).document()

            data = {
                "FullName": row.get("FullName", "").strip(),
                "job_title": row.get("job_title", "").strip(),
                "department": row.get("department", "").strip(),
                "company": row.get("Company", "").strip(),
                "org_type": row.get("org_type", "").strip(),
                "address": row.get("Address", "").strip(),
                "city": row.get("city", "").strip(),
                "country": row.get("country", "").strip(),
                "country_code": row.get("country_code", "").strip(),
                "office_number": normalize_list(row.get("office_number", "")),
                "mobile_number": normalize_list(row.get("mobile_number", "")),
                "fax_number": normalize_list(row.get("fax_number", "")),
                "email": normalize_list(row.get("Email", "")),
                "website": row.get("Website", "").strip(),
                "tag": "Bernadette",
                "search_keywords": build_search_keywords(row),
                "updated_at": (row.get("updated_at") or datetime.datetime.utcnow().isoformat() + "Z"),
                # optional fields your UI supports:
                "notes": row.get("notes", "").strip(),
                "created_at": firestore.SERVER_TIMESTAMP,
                "created_by": row.get("created_by", ""),
                "updated_by": row.get("updated_by", ""),
            }

            # Merge to be idempotent
            if doc_ref.get().exists:
                doc_ref.set(data, merge=True)
                updated += 1
            else:
                doc_ref.set(data)
                created += 1

        print(f"✅ Upload complete: {total} processed, {created} created, {updated} updated.")

if __name__ == "__main__":
    upload_csv()
