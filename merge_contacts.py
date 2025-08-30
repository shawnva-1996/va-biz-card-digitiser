#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Merge & Deduplicate Contacts (LLM-assisted via Ollama)

Requirements:
  - Python 3
  - pip install pandas requests
  - A local Ollama server running (http://localhost:11434) with model "llama3.1:8b" pulled:
      ollama pull llama3.1:8b

Behavior:
  - Reads all .csv files from ./input_contacts
  - Harmonizes column names (case-insensitive), ensuring "FullName" exists
  - Builds a normalized_name for duplicate grouping
  - Sends duplicate groups to Ollama for intelligent merging
  - Falls back to original rows if LLM fails or response isn't valid JSON
  - Writes merged output to merged_contacts.csv (no index)
"""

import os
import re
import glob
import json
import time
import traceback
from typing import List, Dict, Any, Optional

import pandas as pd
import requests

# ---------------------------
# Configuration
# ---------------------------
INPUT_DIR = "input_contacts"
OUTPUT_FILE = "merged_contacts.csv"

OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3.1:8b"
HTTP_TIMEOUT = 60  # seconds

# If you want to limit how many duplicate groups to process (for testing), set a small number here.
MAX_GROUPS: Optional[int] = None  # None means process all


# ---------------------------
# Utilities
# ---------------------------

def log(msg: str) -> None:
    """Simple console logger with timestamp."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Harmonize common column name variants to a unified set,
    focusing on ensuring 'FullName' exists.

    This function is deliberately conservative: it renames a few common variations.
    You can add more mappings if your datasets vary widely.
    """
    if df is None or df.empty:
        return df

    # Build a mapping (case-insensitive keys)
    col_map = {c.lower(): c for c in df.columns}
    renames = {}

    # 'FullName' standardization
    # Look for likely candidates in priority order
    fullname_candidates = [
        "fullname", "full_name", "name", "full name"
    ]
    for cand in fullname_candidates:
        if cand in col_map:
            renames[col_map[cand]] = "FullName"
            break

    # Common other fields (optional; only if you want uniform names)
    common_maps = {
        "job_title": ["job title", "title", "position", "jobtitle"],
        "department": ["dept", "division"],
        "company": ["company_name", "organization", "organisation", "employer", "company name"],
        "address": ["street", "mailing address", "fulladdress", "full address"],
        "email": ["email_address", "email address", "e-mail", "mail"],
        "website": ["site", "url", "web", "web site", "web url"],
        "mobile_number": ["mobile", "cell", "cellphone", "cell phone", "phone mobile", "mobile no", "mobile_no"],
        "phone_number": ["phone", "telephone", "tel", "office number", "office_number"],
        "country": ["country_name"],
        "city": ["town"],
    }

    for unified, variations in common_maps.items():
        if unified.lower() in col_map:
            # Already exists in some case; standardize to our casing
            renames[col_map[unified.lower()]] = unified
            continue
        for var in variations:
            if var in col_map:
                renames[col_map[var]] = unified
                break

    df = df.rename(columns=renames)

    # Ensure FullName exists even if missing (fill with empty string)
    if "FullName" not in df.columns:
        df["FullName"] = ""

    return df


def normalize_name(name: Any) -> str:
    """Lowercase, remove punctuation, trim whitespace, collapse spaces for comparing names."""
    if not isinstance(name, str):
        return ""
    s = name.strip().lower()
    # Replace punctuation with space
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def safe_json_loads(text: str) -> Optional[Dict[str, Any]]:
    """
    Try to parse a JSON object from text. Handles:
      - raw JSON
      - JSON within code fences
      - JSON preceded/followed by commentary (extract first {...} block)
    Returns a dict if successful, else None.
    """
    # 1) Direct attempt
    try:
        return json.loads(text)
    except Exception:
        pass

    # 2) Strip code fences if present
    fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fence_match:
        inner = fence_match.group(1)
        try:
            return json.loads(inner)
        except Exception:
            pass

    # 3) Find first plausible JSON object
    # This is a heuristic: find the first top-level {...} block.
    stack = []
    start_idx = None
    for i, ch in enumerate(text):
        if ch == "{":
            if not stack:
                start_idx = i
            stack.append("{")
        elif ch == "}":
            if stack:
                stack.pop()
                if not stack and start_idx is not None:
                    candidate = text[start_idx:i + 1]
                    try:
                        return json.loads(candidate)
                    except Exception:
                        # continue scanning in case there's another valid block later
                        start_idx = None
                        continue
    # Not parseable
    return None


def build_prompt_for_group(rows: List[Dict[str, Any]]) -> str:
    """
    Build the deduplication prompt given a list of row dicts for the same normalized_name.
    The prompt instructs the LLM to return only a single merged JSON object.
    """
    # Only include relevant keys (avoid the helper key)
    cleaned_rows = []
    for r in rows:
        r = {k: (None if pd.isna(v) else v) for k, v in r.items() if k != "normalized_name"}
        cleaned_rows.append(r)

    example_rules = (
        "You are an expert data deduplication assistant. I have the following contact records that might be for the same person. "
        "Your task is to merge them into a single, definitive JSON record.\n\n"
        "Rules:\n"
        "- Combine all available information.\n"
        "- For conflicting fields, choose the most complete or most professional-looking value (e.g., a full job title is better than an acronym).\n"
        "- If one record has a value and another is empty, use the value that is present.\n"
        "- Ensure all phone numbers are in a standard international format (E.164 with +country code when possible).\n"
        "- Prefer consistent casing (e.g., emails lowercase, names in title case if appropriate).\n"
        "- Keep field names as-is from the input (do not invent new fields unless necessary to clarify a value).\n\n"
        "Here are the records to merge:\n"
        f"{json.dumps(cleaned_rows, ensure_ascii=False, indent=2)}\n\n"
        "Please provide ONLY the merged JSON object as your response, with no other text or explanation."
    )
    return example_rules


def call_ollama(prompt: str) -> Optional[Dict[str, Any]]:
    """
    Call the local Ollama API with the given prompt and return a parsed JSON object
    from the model's 'response' text. Returns None on failure.
    """
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }
    try:
        resp = requests.post(OLLAMA_ENDPOINT, json=payload, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            log(f"ERROR: Ollama returned status {resp.status_code}: {resp.text[:2000]}")
            return None
        data = resp.json()
        # Ollama /api/generate returns {"model":..., "created_at":..., "response": "...", "done": true, ...}
        text = data.get("response", "")
        merged = safe_json_loads(text)
        if merged is None:
            log("ERROR: LLM response was not valid JSON after multiple extraction attempts.")
        return merged
    except requests.RequestException as e:
        log(f"ERROR: Request to Ollama failed: {e}")
        return None
    except Exception as e:
        log(f"ERROR: Unexpected error calling Ollama: {e}")
        traceback.print_exc()
        return None


# ---------------------------
# Main pipeline
# ---------------------------

def load_all_csvs(input_dir: str) -> pd.DataFrame:
    """Load and concatenate all CSVs from the input directory, allowing different column orders."""
    pattern = os.path.join(input_dir, "*.csv")
    paths = glob.glob(pattern)
    if not paths:
        log(f"WARNING: No CSV files found in '{input_dir}'. Producing empty output.")
        return pd.DataFrame()

    dfs = []
    for p in paths:
        try:
            df = pd.read_csv(p, dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"])
            df = standardize_columns(df)
            dfs.append(df)
            log(f"Loaded: {p} with {len(df)} rows")
        except Exception as e:
            log(f"ERROR: Failed to read '{p}': {e}")
    if not dfs:
        return pd.DataFrame()
    # Concatenate without sorting columns; differing columns will be unioned
    master = pd.concat(dfs, ignore_index=True, sort=False)
    return master


def prepare_dataframe(master: pd.DataFrame) -> pd.DataFrame:
    """Add normalized_name for grouping and return the prepared DataFrame."""
    if master is None or master.empty:
        return pd.DataFrame()

    if "FullName" not in master.columns:
        # Ensure existence (already ensured in standardize_columns, but double-protect)
        master["FullName"] = ""

    # Normalize names
    master["normalized_name"] = master["FullName"].apply(normalize_name)

    return master


def process_duplicates(master: pd.DataFrame) -> pd.DataFrame:
    """
    Identify duplicate groups by normalized_name. For each group with >1 rows,
    call the LLM to merge; otherwise keep the single row. Returns a final DataFrame.
    """
    if master is None or master.empty:
        log("Input is empty. Nothing to process.")
        return master

    # Identify duplicate groups (exclude empty normalized_name to avoid accidental merges)
    grouped = master.groupby("normalized_name", dropna=False)
    unique_rows = []
    merged_rows = []

    # Stats
    dup_keys = [k for k, g in grouped if k and len(g) > 1]
    log(f"Found {len(dup_keys)} potential duplicate groups to process...")

    processed_count = 0

    for key, group_df in grouped:
        rows = group_df.to_dict(orient="records")

        # Treat empty normalized_name as always unique (skip LLM)
        if not key or len(rows) == 1:
            unique_rows.extend(rows)
            continue

        # Optional cap for testing
        if MAX_GROUPS is not None and processed_count >= MAX_GROUPS:
            unique_rows.extend(rows)
            continue

        # Build and call LLM
        # Choose a nice display name for logs:
        display_name = rows[0].get("FullName", "") or key
        log(f"Merging records for '{display_name}'... ({len(rows)} candidates)")

        prompt = build_prompt_for_group(rows)
        merged_json = call_ollama(prompt)

        if merged_json is None or not isinstance(merged_json, dict):
            log(f"LLM merge failed for '{display_name}'. Keeping original records for this group.")
            unique_rows.extend(rows)
            continue

        # Keep the merged JSON as one canonical row
        # Ensure normalized_name for the merged row is rederived from 'FullName' (if present)
        if "normalized_name" in merged_json:
            merged_json.pop("normalized_name", None)

        # Re-add normalized_name to keep consistent schema
        merged_fullname = merged_json.get("FullName", "")
        merged_json["normalized_name"] = normalize_name(merged_fullname)

        merged_rows.append(merged_json)
        processed_count += 1

    # Compose final DataFrame:
    # Union of all columns across uniques + merges
    final_df = pd.DataFrame(unique_rows + merged_rows)

    # Ideally we remove helper column before saving
    if "normalized_name" in final_df.columns:
        final_df = final_df.drop(columns=["normalized_name"])

    # Optional: Reorder columns so "FullName" comes first if present
    cols = list(final_df.columns)
    if "FullName" in cols:
        cols.remove("FullName")
        final_df = final_df[["FullName"] + cols]

    return final_df


def main():
    os.makedirs(INPUT_DIR, exist_ok=True)

    log("Loading CSV files...")
    master = load_all_csvs(INPUT_DIR)

    if master.empty:
        # Still write an empty file with no index
        pd.DataFrame().to_csv(OUTPUT_FILE, index=False)
        log(f"Done. Wrote empty output to {OUTPUT_FILE}")
        return

    log(f"Total loaded rows: {len(master)}")
    master = prepare_dataframe(master)

    final_df = process_duplicates(master)

    # Save output
    final_df.to_csv(OUTPUT_FILE, index=False)
    log(f"Done. Wrote {len(final_df)} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
