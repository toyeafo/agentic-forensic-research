import sqlite3
import pandas as pd
import os
import re
import json
import time

# --- CONFIGURATION ---
DB_FOLDER_PATH = "./experiment_data" 
OUTPUT_FILE = "gold_standard_truth.json"
MAX_SAMPLES = None 

# --- PATTERNS (Class I: Identifiers) ---
REGEX_MAP = {
    "email": r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    "phone": r'(?<!\d)\+?\d{10,15}(?!\d)',
    "uuid":  r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
}

# --- HEURISTICS (Class II & III) ---
# Columns containing these words are treated as Temporal
TIME_KEYWORDS = ['time', 'date', 'created', 'modified', 'duration', 'timestamp']

def is_valid_timestamp(series):
    """Checks if a numeric series looks like recent Unix timestamps (2000-2030)."""
    # 946684800 = Jan 1 2000, 1893456000 = Jan 1 2030
    try:
        numeric = pd.to_numeric(series, errors='coerce')
        return numeric.between(946684800, 1893456000)
    except:
        return pd.Series([False] * len(series))

def scan_database(db_path, db_filename):
    """
    Scans a DB using Pandas for speed, but preserves Row IDs for provenance.
    Returns a list of entity dictionaries.
    """
    all_entities = []
    
    try:
        conn = sqlite3.connect(db_path)
        # Get list of tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cursor.fetchall()]
        
        for table in tables:
            try:
                # READ TABLE WITH ROWID
                # We explicitly fetch ROWID to enable Provenance Checking (W3 Workflow)
                try:
                    df = pd.read_sql_query(f"SELECT ROWID as _pk_id, * FROM '{table}'", conn)
                except:
                    # Fallback for WITHOUT ROWID tables or views
                    df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)
                    if not df.empty:
                        df['_pk_id'] = df.index # Fake ID if none exists

                if df.empty:
                    continue
                
                # --- 1. SCAN IDENTIFIERS (Regex on Text Columns) ---
                # Select only string columns to speed up regex
                str_df = df.select_dtypes(include=['object', 'string']).astype(str)
                str_df['_pk_id'] = df['_pk_id'] # Keep ID for mapping
                
                for col in str_df.columns:
                    if col == '_pk_id': continue
                    
                    for type_name, pattern in REGEX_MAP.items():
                        # Extract all matches. Stack returns a MultiIndex (Row Index, Match Index)
                        matches = str_df[col].str.extractall(f"({pattern})")
                        
                        if not matches.empty:
                            # Map back to real Row ID
                            for idx, row in matches.iterrows():
                                original_row_idx = idx[0] 
                                row_id = str_df.at[original_row_idx, '_pk_id']
                                value = row[0]
                                
                                all_entities.append({
                                    "class": "Identifier",
                                    "type": type_name,
                                    "value": value,
                                    "provenance": {
                                        "table": table,
                                        "column": col,
                                        "row_id": int(row_id)
                                    }
                                })

                # --- 2. SCAN TEMPORAL (Heuristics) ---
                # Strategy: Check column name keywords OR value ranges
                for col in df.columns:
                    if col == '_pk_id': continue
                    
                    is_temporal = False
                    
                    # Name Check
                    if any(kw in col.lower() for kw in TIME_KEYWORDS):
                        is_temporal = True
                    # Value Check (if numeric)
                    elif pd.api.types.is_numeric_dtype(df[col]):
                        if is_valid_timestamp(df[col]).mean() > 0.5: # If >50% look like timestamps
                            is_temporal = True
                            
                    if is_temporal:
                        # Extract non-nulls
                        valid_rows = df[df[col].notna()]
                        for _, row in valid_rows.iterrows():
                            all_entities.append({
                                "class": "Temporal",
                                "type": "timestamp/date",
                                "value": str(row[col]),
                                "provenance": {
                                    "table": table,
                                    "column": col,
                                    "row_id": int(row['_pk_id'])
                                }
                            })
                            
            except Exception as e:
                print(f"  [!] Error reading table {table}: {e}")

        conn.close()
        return all_entities

    except Exception as e:
        print(f"[-] Failed to open {db_path}: {e}")
        return []

def main():
    print(f"[*] Starting Recursive Forensic Scan in: {DB_FOLDER_PATH}")
    
    master_report = {}
    
    if not os.path.exists(DB_FOLDER_PATH):
        print(f"[!] Directory {DB_FOLDER_PATH} not found.")
        return

    # Recursive Walk
    db_files = []
    for root, dirs, filenames in os.walk(DB_FOLDER_PATH):
        for f in filenames:
            if f.endswith('.db') or f.endswith('.sqlite'):
                db_files.append(os.path.join(root, f))

    if not db_files:
        print("[!] No database files found.")
        return

    total_entities_count = 0

    for full_path in db_files:
        # Use relative path as ID to handle duplicate filenames in different folders
        # e.g. "app_A/webview.db" vs "app_B/webview.db"
        rel_path = os.path.relpath(full_path, DB_FOLDER_PATH)
        print(f"[*] Scanning: {rel_path}...")
        
        entities = scan_database(full_path, rel_path)
        
        # Summary Stats
        stats = {"Identifier": 0, "Temporal": 0}
        for e in entities:
            stats[e["class"]] += 1
            
        master_report[rel_path] = {
            "meta": {
                "total_entities": len(entities),
                "breakdown": stats
            },
            "ground_truth_data": entities
        }
        total_entities_count += len(entities)

    # Save to JSON
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(master_report, f, indent=2)
        
    print(f"\n[+] Success. Processed {len(db_files)} databases.")
    print(f"[+] Total Evidence Entities Found: {total_entities_count}")
    print(f"[+] Ground Truth saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
