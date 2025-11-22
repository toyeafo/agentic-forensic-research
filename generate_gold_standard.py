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

def analyze_database(db_path):
    """Connects to a DB and scans all tables for entity counts."""
    results = {
        "meta": {"size_bytes": os.path.getsize(db_path)},
        "entities": {"identifier": 0, "temporal": 0, "relational": 0},
        "details": {}
    }
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cursor.fetchall()]
        
        for table in tables:
            try:
                # Read entire table into pandas (efficient for regex)
                df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)
                
                if df.empty:
                    continue

                table_stats = {"identifier": 0, "temporal": 0, "relational": 0}

                # 1. SCAN IDENTIFIERS (Text Regex)
                # Convert all object columns to string for searching
                str_df = df.select_dtypes(include=['object', 'string']).astype(str)
                for col in str_df.columns:
                    # Count Emails
                    emails = str_df[col].str.count(EMAIL_REGEX).sum()
                    # Count Phones
                    phones = str_df[col].str.count(PHONE_REGEX).sum()
                    table_stats["identifier"] += int(emails + phones)

                # 2. SCAN TEMPORAL (Column Name Heuristic + Data Type)
                # If column name suggests time AND data is not null
                for col in df.columns:
                    if any(kw in col.lower() for kw in TIME_KEYWORDS):
                        table_stats["temporal"] += int(df[col].count())

                # 3. SCAN RELATIONAL (Foreign Keys / IDs)
                # Count non-null rows in columns that look like IDs (excluding primary key 'id' usually)
                for col in df.columns:
                    if any(kw in col.lower() for kw in RELATION_KEYWORDS):
                        # Avoid counting the row index itself as a relation unless it's a known foreign key pattern
                        table_stats["relational"] += int(df[col].count())

                # Update Total DB Stats
                results["entities"]["identifier"] += table_stats["identifier"]
                results["entities"]["temporal"] += table_stats["temporal"]
                results["entities"]["relational"] += table_stats["relational"]
                
                results["details"][table] = table_stats

            except Exception as e:
                print(f"  [!] Error reading table {table}: {e}")
                
        conn.close()
        return results

    except Exception as e:
        print(f"[-] Failed to open {db_path}: {e}")
        return None

def main():
    print(f"[*] Starting Gold Standard Scan in: {DB_FOLDER_PATH}")
    
    report = {}
    
    # Ensure directory exists
    if not os.path.exists(DB_FOLDER_PATH):
        print(f"[!] Directory {DB_FOLDER_PATH} not found. Please create it and add .db files.")
        return

    files = [f for f in os.listdir(DB_FOLDER_PATH) if f.endswith('.db') or f.endswith('.sqlite')]
    
    if not files:
        print("[!] No .db files found.")
        return

    for f in files:
        print(f"[*] Analyzing: {f}...")
        path = os.path.join(DB_FOLDER_PATH, f)
        data = analyze_database(path)
        if data:
            report[f] = data
            
    # Save to JSON
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(report, f, indent=4)
        
    print(f"\n[+] Success. Gold Standard saved to: {OUTPUT_FILE}")
    print("    Use these counts to calculate Recall (Agent_Found / Gold_Count).")

if __name__ == "__main__":
    main()
