import sqlite3
import os
import re
import json

# --- CONFIGURATION ---
DB_FOLDER_PATH = './experiment_data'  # Root folder containing app subfolders
OUTPUT_FILE = 'gold_standard_truth.json'

# --- DEFINITIONS: CLASS I (IDENTIFIERS) ---
# Regex patterns to find atomic entities in text
PATTERNS = {
    "email": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    "phone": r'\b\+?1?[-.]?\(?\d{3}\)?[-.]?\d{3}[-.]?\d{4}\b',
    "ipv4": r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    "url": r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
}

# --- DEFINITIONS: CLASS II (TEMPORAL) ---
# Column names that likely imply time data
TIME_KEYWORDS = ['time', 'date', 'timestamp', 'created', 'modified', 'duration', 'added']

# --- DEFINITIONS: CLASS III (RELATIONAL) ---
# Column names that imply Foreign Keys or IDs (excluding the primary key 'id' usually)
RELATION_KEYWORDS = ['_id', 'userid', 'groupid', 'sender', 'receiver', 'conversation', 'contact_id', 'thread_id']

def is_valid_timestamp(value):
    """Heuristic: Is this integer a valid Unix timestamp between 2000 and 2030?"""
    try:
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
            val = int(value)
            # Range: Jan 1, 2000 (946684800) to Jan 1, 2030 (1893456000)
            if 946684800 < val < 1893456000:
                return True
    except:
        return False
    return False

def scan_database(db_path):
    """Scans a DB and returns a precise list of every forensic artifact found."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row # Allows accessing columns by name
    cursor = conn.cursor()
    
    entities_found = []
    
    # 1. Get List of Tables
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r['name'] for r in cursor.fetchall()]
    except sqlite3.DatabaseError:
        print(f"[-] Could not read {db_path} (Encrypted or Corrupt)")
        return []

    for table in tables:
        # 2. Identify Primary Key (Crucial for Provenance/Auditability)
        # We need to know WHICH row the evidence is in.
        pk_col = "rowid" # Default
        try:
            cursor.execute(f"PRAGMA table_info({table})")
            cols_info = cursor.fetchall()
            # Look for a column explicitly marked as PK
            for c in cols_info:
                if c['pk'] == 1:
                    pk_col = c['name']
                    break
        except:
            pass

        # 3. Scan Rows
        try:
            # Select all data + the explicit Row ID/Primary Key
            if pk_col == "rowid":
                cursor.execute(f"SELECT *, rowid as _pk_id FROM '{table}'")
            else:
                cursor.execute(f"SELECT *, {pk_col} as _pk_id FROM '{table}'")
            
            rows = cursor.fetchall()
        except sqlite3.OperationalError:
            # Skip virtual tables (fts3/fts4) that might error on SELECT *
            continue

        for row in rows:
            row_id = row['_pk_id']
            
            # Iterate through every column in this row
            for col_name in row.keys():
                if col_name == "_pk_id": continue
                
                val = row[col_name]
                if val is None: continue
                val_str = str(val)

                # --- CHECK CLASS I: IDENTIFIERS (Regex) ---
                # Only check text-like columns to save time
                for type_name, pattern in PATTERNS.items():
                    matches = re.findall(pattern, val_str)
                    for match in matches:
                        entities_found.append({
                            "class": "Identifier",
                            "type": type_name,
                            "value": match,
                            "provenance": {
                                "table": table,
                                "column": col_name,
                                "row_id": row_id
                            }
                        })

                # --- CHECK CLASS II: TEMPORAL ---
                # Match if column name suggests time OR value looks like a timestamp
                is_time_col = any(k in col_name.lower() for k in TIME_KEYWORDS)
                if is_time_col or (isinstance(val, (int, float)) and is_valid_timestamp(val)):
                    # Double check logic: if it's a "time" column, take it. 
                    # If it's a generic column, only take it if it passes the strict timestamp check.
                    if is_time_col or is_valid_timestamp(val):
                        entities_found.append({
                            "class": "Temporal",
                            "type": "timestamp",
                            "value": val,
                            "provenance": {
                                "table": table,
                                "column": col_name,
                                "row_id": row_id
                            }
                        })

                # --- CHECK CLASS III: RELATIONAL ---
                # Check if column name implies a relationship (Foreign Key)
                if any(k in col_name.lower() for k in RELATION_KEYWORDS):
                    # We treat the ID itself as the evidence of the relationship
                    entities_found.append({
                        "class": "Relational",
                        "type": "foreign_key_id",
                        "value": val,
                        "provenance": {
                            "table": table,
                            "column": col_name,
                            "row_id": row_id
                        }
                    })

    conn.close()
    return entities_found

def main():
    print(f"[*] Starting Recursive Gold Standard Scan in: {DB_FOLDER_PATH}")
    
    master_index = {}
    
    if not os.path.exists(DB_FOLDER_PATH):
        print(f"[!] Directory {DB_FOLDER_PATH} not found.")
        return

    # 1. Recursive File Search
    db_files = []
    for root, dirs, filenames in os.walk(DB_FOLDER_PATH):
        for f in filenames:
            if f.endswith('.db') or f.endswith('.sqlite'):
                db_files.append(os.path.join(root, f))
    
    if not db_files:
        print("[!] No .db files found.")
        return

    # 2. Process Files
    for full_path in db_files:
        # Use relative path as ID to handle duplicate names in different folders
        # e.g., "com.whatsapp/databases/msgstore.db"
        rel_path = os.path.relpath(full_path, DB_FOLDER_PATH)
        print(f"[*] Processing: {rel_path}...")
        
        entities = scan_database(full_path)
        
        # Calculate summary stats for the JSON header
        counts = {"Identifier": 0, "Temporal": 0, "Relational": 0}
        for e in entities:
            counts[e['class']] += 1
            
        master_index[rel_path] = {
            "summary": counts,
            "total_entities": len(entities),
            "artifacts": entities
        }

    # 3. Save Output
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(master_index, f, indent=2)
        
    print(f"\n[+] Gold Standard Generation Complete.")
    print(f"[+] Output saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
