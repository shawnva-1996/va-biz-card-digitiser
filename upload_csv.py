#!/usr/bin/env python3
import csv, sys
from datetime import datetime
from google.cloud import firestore
from google.oauth2 import service_account

PROJECT_ID = "va-solutions-eadd3"
SERVICE_ACCOUNT_FILE = "serviceAccount.json"

def parse_list(val):
    if not val: return []
    return [v.strip() for v in val.replace(";",",").split(",") if v.strip()]

def build_search_keywords(row):
    fields = [
        row.get("FullName") or "",
        row.get("job_title") or "",
        row.get("department") or "",
        row.get("Company") or "",
        row.get("org_type") or "",
        row.get("Address") or "",
        row.get("city") or "",
        row.get("country") or "",
        row.get("country_code") or "",
        row.get("inferred_seniority") or "",
        row.get("inferred_name_origin") or "",
        row.get("inferred_region") or "",
        row.get("inferred_contact_tier") or "",
        row.get("network_cluster") or "",
        row.get("suggested_next_action") or "",
        row.get("tag") or ""
    ]
    # Filter out any potential empty strings from the final list before joining
    return " ".join(filter(None, fields)).lower()

def upload_csv():
    if len(sys.argv) < 2:
        print("Usage: python upload_csv.py contacts.csv")
        return
    csv_file = sys.argv[1]

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    db = firestore.Client(project=PROJECT_ID, credentials=creds)

    with open(csv_file, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc = {
                "FullName": (row.get("FullName") or "").strip(),
                "job_title": (row.get("job_title") or "").strip(),
                "department": (row.get("department") or "").strip(),
                "company": (row.get("Company") or "").strip(),
                "org_type": (row.get("org_type") or "").strip(),
                "address": (row.get("Address") or "").strip(),
                "city": (row.get("city") or "").strip(),
                "country": (row.get("country") or "").strip(),
                "country_code": (row.get("country_code") or "").strip(),
                "office_number": parse_list(row.get("office_number")),
                "mobile_number": parse_list(row.get("mobile_number")),
                "fax_number": parse_list(row.get("fax_number")),
                "email": parse_list(row.get("Email")),
                "website": (row.get("Website") or "").strip(),
                "updated_at": (row.get("updated_at") or "").strip() or datetime.utcnow().isoformat(),
                "created_at": datetime.utcnow(),
                "created_by": "",
                "inferred_seniority": (row.get("inferred_seniority") or "").strip(),
                "inferred_name_origin": (row.get("inferred_name_origin") or "").strip(),
                "inferred_region": (row.get("inferred_region") or "").strip(),
                "inferred_contact_tier": (row.get("inferred_contact_tier") or "").strip(),
                "network_cluster": (row.get("network_cluster") or "").strip(),
                "suggested_next_action": (row.get("suggested_next_action") or "").strip(),
                "tag": (row.get("tag") or "").strip(),
            }
            doc["search_keywords"] = build_search_keywords(row)
            db.collection("contacts").add(doc)
            print(f"Uploaded {doc['FullName']}")

if __name__ == "__main__":
    upload_csv()
