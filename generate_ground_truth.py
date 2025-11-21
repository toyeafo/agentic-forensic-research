import sqlite3
import os
import re
import json
from datetime import datetime

# CONFIGURATION
DB_FOLDER = './forensic_images'  # Folder containing your .db or .sqlite files
OUTPUT_FILE = 'ground_truth_master.json'

# REGEX PATTERNS (The definitions of "Evidence")
PATTERNS = {
    "email": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    "phone": r'\b\+?1?[-.]?\(?\d{3}\)?[-.]?\d{3}[-.]?\d{4}\b',
    "ipv4": r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    "url": r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
}

def is_timestamp(value):
    """Heuristic to check if an integer is a likely recent Unix timestamp."""
    if isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
        val = int(value)
        # Check range: Jan 1, 2000 to Jan 1, 2030
        if 946684800 < val < 1893456000:
            return True
    return False

def scan_database(db_path):
    """Scans a single SQLite DB for all entities defined in PATTERNS."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    db_results = []
    
    # 1. Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [r['name'] for r in cursor.fetchall()]
    
    for table in tables:
        # Get Primary Key column name (usually rowid or declared PK)
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        pk_col = next((c['name'] for c in cols if c['pk'] == 1), "rowid")
        
        try:
            cursor.execute(f"SELECT *, {pk_col} as _pk_id FROM {table}")
            rows = cursor.fetchall()
        except sqlite3.OperationalError:
            continue # Skip tables we can't read (e.g. virtual tables)

        for row in rows:
            row_id = row['_pk_id']
            
            # Scan every column in this row
            for col_name in row.keys():
                val = row[col_name]
                if not val: continue
                
                val_str = str(val)
                
                # Check Regular Expressions (Emails, Phones, URLs)
                for entity_type, pattern in PATTERNS.items():
                    matches = re.findall(pattern, val_str)
                    for match in matches:
                        db_results.append({
                            "entity_type": entity_type,
                            "value": match,
                            "table": table,
                            "column": col_name,
                            "row_id": row_id,
                            "data_class": "Identifier"
                        })
                
                # Check Timestamp Heuristics
                if is_timestamp(val):
                     db_results.append({
                            "entity_type": "unix_timestamp",
                            "value": val,
                            "table": table,
                            "column": col_name,
                            "row_id": row_id,
                            "data_class": "Temporal"
                        })

    conn.close()
    return db_results

def main():
    master_index = {}
    
    if not os.path.exists(DB_FOLDER):
        print(f"Error: Directory '{DB_FOLDER}' not found.")
        return

    files = [f for f in os.listdir(DB_FOLDER) if f.endswith('.db') or f.endswith('.sqlite')]
    print(f"Found {len(files)} databases. Scanning...")

    for f in files:
        path = os.path.join(DB_FOLDER, f)
        print(f"Processing {f}...")
        entities = scan_database(path)
        master_index[f] = {
            "total_entities": len(entities),
            "entities": entities
        }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(master_index, f, indent=2)
    
    print(f"Done. Ground Truth saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
