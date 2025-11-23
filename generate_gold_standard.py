import sqlite3
import pandas as pd
import re
import os
import json
import time

# --- CONFIGURATION ---
# Path to the folder containing your extracted .db files
DB_FOLDER_PATH = "./experiment_data" 
# Where to save the ground truth file
OUTPUT_FILE = "gold_standard_truth.json"

# --- REGEX PATTERNS (Class I: Identifiers) ---
EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
PHONE_REGEX = r'(?<!\d)\+?\d{10,15}(?!\d)' # Basic 10-15 digit sequences

# --- HEURISTICS ---
# Keywords to identify Temporal columns (Class II)
TIME_KEYWORDS = ['time', 'date', 'timestamp', 'created', 'modified', 'duration']
# Keywords to identify Relational IDs (Class III)
RELATION_KEYWORDS = ['_id', 'userid', 'groupid', 'sender', 'receiver', 'conversation']

# --- CONFIGURATION UPDATE ---
# Set to None to save ALL data, or an integer (e.g., 5) to save just a preview
MAX_SAMPLES = None 

def analyze_database(db_path):
    """Connects to a DB, scans tables, and extracts both COUNTS and DATA VALUES."""
    results = {
        "meta": {"size_bytes": os.path.getsize(db_path)},
        "summary_counts": {"identifier": 0, "temporal": 0, "relational": 0},
        "tables": {}
    }
    
    try:
        conn = sqlite3.connect(db_path)
        # Use pandas to read efficiently
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cursor.fetchall()]
        
        for table in tables:
            try:
                df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)
                if df.empty:
                    continue

                # Structure to hold data for this table
                table_data = {
                    "counts": {"identifier": 0, "temporal": 0, "relational": 0},
                    "values": {"emails": [], "phones": [], "temporal_cols": {}, "relational_cols": {}}
                }

                # 1. SCAN IDENTIFIERS (Extract Regex Matches)
                str_df = df.select_dtypes(include=['object', 'string']).astype(str)
                for col in str_df.columns:
                    # -- Extract Emails --
                    # findall returns a list per row. explode flattens it. unique removes duplicates.
                    found_emails = str_df[col].str.findall(EMAIL_REGEX).explode().dropna().unique().tolist()
                    if found_emails:
                        table_data["counts"]["identifier"] += len(found_emails)
                        # Append to list, respecting sample limit
                        current_len = len(table_data["values"]["emails"])
                        if MAX_SAMPLES is None or current_len < MAX_SAMPLES:
                            table_data["values"]["emails"].extend(found_emails)

                    # -- Extract Phones --
                    found_phones = str_df[col].str.findall(PHONE_REGEX).explode().dropna().unique().tolist()
                    if found_phones:
                        table_data["counts"]["identifier"] += len(found_phones)
                        current_len = len(table_data["values"]["phones"])
                        if MAX_SAMPLES is None or current_len < MAX_SAMPLES:
                            table_data["values"]["phones"].extend(found_phones)

                # 2. SCAN TEMPORAL (Extract Raw Values)
                for col in df.columns:
                    if any(kw in col.lower() for kw in TIME_KEYWORDS):
                        # Get non-null values
                        valid_times = df[col].dropna().tolist()
                        count = len(valid_times)
                        if count > 0:
                            table_data["counts"]["temporal"] += count
                            # Store the column name and a sample of its data
                            limit = MAX_SAMPLES if MAX_SAMPLES else len(valid_times)
                            table_data["values"]["temporal_cols"][col] = valid_times[:limit]

                # 3. SCAN RELATIONAL (Extract IDs)
                for col in df.columns:
                    if any(kw in col.lower() for kw in RELATION_KEYWORDS):
                        valid_ids = df[col].dropna().unique().tolist()
                        count = len(valid_ids)
                        if count > 0:
                            table_data["counts"]["relational"] += count
                            limit = MAX_SAMPLES if MAX_SAMPLES else len(valid_ids)
                            table_data["values"]["relational_cols"][col] = valid_ids[:limit]

                # Update Global Summary
                results["summary_counts"]["identifier"] += table_data["counts"]["identifier"]
                results["summary_counts"]["temporal"] += table_data["counts"]["temporal"]
                results["summary_counts"]["relational"] += table_data["counts"]["relational"]
                
                # Apply limits to the final stored lists if they grew too large during iteration
                if MAX_SAMPLES:
                    table_data["values"]["emails"] = table_data["values"]["emails"][:MAX_SAMPLES]
                    table_data["values"]["phones"] = table_data["values"]["phones"][:MAX_SAMPLES]

                results["tables"][table] = table_data

            except Exception as e:
                print(f"  [!] Error reading table {table}: {e}")
                
        conn.close()
        return results

    except Exception as e:
        print(f"[-] Failed to open {db_path}: {e}")
        return None
# def analyze_database(db_path):
#     """Connects to a DB and scans all tables for entity counts."""
#     results = {
#         "meta": {"size_bytes": os.path.getsize(db_path)},
#         "entities": {"identifier": 0, "temporal": 0, "relational": 0},
#         "details": {}
#     }
    
#     try:
#         conn = sqlite3.connect(db_path)
#         cursor = conn.cursor()
        
#         # Get all tables
#         cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#         tables = [r[0] for r in cursor.fetchall()]
        
#         for table in tables:
#             try:
#                 # Read entire table into pandas (efficient for regex)
#                 df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)
                
#                 if df.empty:
#                     continue

#                 table_stats = {"identifier": 0, "temporal": 0, "relational": 0}

#                 # 1. SCAN IDENTIFIERS (Text Regex)
#                 # Convert all object columns to string for searching
#                 str_df = df.select_dtypes(include=['object', 'string']).astype(str)
#                 for col in str_df.columns:
#                     # Count Emails
#                     emails = str_df[col].str.count(EMAIL_REGEX).sum()
#                     # Count Phones
#                     phones = str_df[col].str.count(PHONE_REGEX).sum()
#                     table_stats["identifier"] += int(emails + phones)

#                 # 2. SCAN TEMPORAL (Column Name Heuristic + Data Type)
#                 # If column name suggests time AND data is not null
#                 for col in df.columns:
#                     if any(kw in col.lower() for kw in TIME_KEYWORDS):
#                         table_stats["temporal"] += int(df[col].count())

#                 # 3. SCAN RELATIONAL (Foreign Keys / IDs)
#                 # Count non-null rows in columns that look like IDs (excluding primary key 'id' usually)
#                 for col in df.columns:
#                     if any(kw in col.lower() for kw in RELATION_KEYWORDS):
#                         # Avoid counting the row index itself as a relation unless it's a known foreign key pattern
#                         table_stats["relational"] += int(df[col].count())

#                 # Update Total DB Stats
#                 results["entities"]["identifier"] += table_stats["identifier"]
#                 results["entities"]["temporal"] += table_stats["temporal"]
#                 results["entities"]["relational"] += table_stats["relational"]
                
#                 results["details"][table] = table_stats

#             except Exception as e:
#                 print(f"  [!] Error reading table {table}: {e}")
                
#         conn.close()
#         return results

#     except Exception as e:
#         print(f"[-] Failed to open {db_path}: {e}")
#         return None

# def main():
#     print(f"[*] Starting Gold Standard Scan in: {DB_FOLDER_PATH}")
    
#     report = {}
    
#     # Ensure directory exists
#     if not os.path.exists(DB_FOLDER_PATH):
#         print(f"[!] Directory {DB_FOLDER_PATH} not found. Please create it and add .db files.")
#         return

#     files = [f for f in os.listdir(DB_FOLDER_PATH) if f.endswith('.db') or f.endswith('.sqlite')]
    
#     if not files:
#         print("[!] No .db files found.")
#         return

#     for f in files:
#         print(f"[*] Analyzing: {f}...")
#         path = os.path.join(DB_FOLDER_PATH, f)
#         data = analyze_database(path)
#         if data:
#             report[f] = data
            
#     # Save to JSON
#     with open(OUTPUT_FILE, 'w') as f:
#         json.dump(report, f, indent=4)
        
#     print(f"\n[+] Success. Gold Standard saved to: {OUTPUT_FILE}")
#     print("    Use these counts to calculate Recall (Agent_Found / Gold_Count).")

def main():
    print(f"[*] Starting Recursive Gold Standard Scan in: {DB_FOLDER_PATH}")
    
    report = {}
    
    # Ensure directory exists
    if not os.path.exists(DB_FOLDER_PATH):
        print(f"[!] Directory {DB_FOLDER_PATH} not found. Please create it and add .db files.")
        return

    # --- CHANGED SECTION: RECURSIVE SEARCH ---
    db_paths = []
    for root, dirs, filenames in os.walk(DB_FOLDER_PATH):
        for f in filenames:
            if f.endswith('.db') or f.endswith('.sqlite'):
                # Construct the full path
                full_path = os.path.join(root, f)
                db_paths.append(full_path)
    # -----------------------------------------
    
    if not db_paths:
        print("[!] No .db files found in any subfolders.")
        return

    for path in db_paths:
        # Use the filename (e.g., 'sms.db') as the report key
        # If you have duplicate names in different folders, change this to: key = path
        key = os.path.basename(path) 
        
        print(f"[*] Analyzing: {key}...")
        
        # 'path' is already the full path, so we pass it directly
        data = analyze_database(path)
        
        if data:
            report[key] = data
            
    # Save to JSON
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(report, f, indent=4)
        
    print(f"\n[+] Success. Gold Standard saved to: {OUTPUT_FILE}")
    print("    Use these counts to calculate Recall (Agent_Found / Gold_Count).")

if __name__ == "__main__":
    main()
