#!/usr/bin/env python3
import argparse
import sqlite3
import re
import json
import csv
from datetime import datetime, timezone

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
UUID_RE = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b")
ISO8601_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?\b")

# Heuristic column name hints
TEXT_EMAIL_HINTS = {"email", "e_mail", "mail"}
TEXT_PHONE_HINTS = {"phone", "tel", "mobile", "msisdn"}
TEXT_UUID_HINTS = {"uuid", "guid"}
TIME_HINTS = {"time", "timestamp", "ts", "date", "datetime", "created_at", "updated_at"}

def connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def get_tables(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [r[0] for r in cur.fetchall()]

def get_table_info(conn, table):
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    cur = conn.execute(f"PRAGMA table_info('{table}')")
    cols = [dict(row) for row in cur.fetchall()]
    # Normalize type names
    for c in cols:
        t = (c["type"] or "").upper()
        c["type_norm"] = "TEXT" if "CHAR" in t or "TEXT" in t or "CLOB" in t else ("INT" if "INT" in t else ("REAL" if "REAL" in t or "FLOA" in t or "DOUB" in t else t))
    return cols

def pk_expression(table, cols):
    pk_cols = [c["name"] for c in cols if c["pk"]]
    if len(pk_cols) == 1:
        return pk_cols[0], "pk"
    elif len(pk_cols) > 1:
        # Composite PK: concatenate with '|'
        expr = " || '|' || ".join([f"CAST({c} AS TEXT)" for c in pk_cols])
        return expr, "composite_pk"
    else:
        # Fallback to ROWID (may not exist for WITHOUT ROWID tables)
        return "rowid", "rowid"

def fetch_distinct(conn, table, col, rid_expr, limit=None):
    lim = f" LIMIT {int(limit)}" if limit else ""
    sql = f"SELECT {rid_expr} AS __rid__, {col} AS __val__ FROM '{table}' WHERE {col} IS NOT NULL{lim}"
    try:
        cur = conn.execute(sql)
        for row in cur:
            yield row["__rid__"], row["__val__"]
    except sqlite3.OperationalError:
        # e.g., rowid missing in WITHOUT ROWID table when no PK; skip safely
        pass

def normalize_phone(s):
    digits = re.sub(r"\D", "", s or "")
    if len(digits) < 10:
        return None
    # Keep last 15 digits max (E.164)
    if len(digits) > 15:
        return None
    return "+" + digits if not s.strip().startswith("+") else "+" + digits

def looks_like_time_column(name):
    n = (name or "").lower()
    return any(h in n for h in TIME_HINTS)

def looks_like_email_column(name):
    n = (name or "").lower()
    return any(h in n for h in TEXT_EMAIL_HINTS)

def looks_like_phone_column(name):
    n = (name or "").lower()
    return any(h in n for h in TEXT_PHONE_HINTS)

def looks_like_uuid_column(name):
    n = (name or "").lower()
    return any(h in n for h in TEXT_UUID_HINTS)

def epoch_to_iso(ts):
    try:
        if ts is None:
            return None
        ts = int(ts)
        # Heuristic: ms vs s
        if ts > 1_000_000_000_000:  # likely ms
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        # sanity range 1970–2100
        if dt.year < 1970 or dt.year > 2100:
            return None
        return dt.isoformat()
    except Exception:
        return None

def extract_identifiers(conn, table, cols, rid_expr):
    results = []
    for c in cols:
        name = c["name"]
        t = c["type_norm"]
        if t != "TEXT" and not looks_like_email_column(name) and not looks_like_phone_column(name):
            continue
        for rid, val in fetch_distinct(conn, table, name, rid_expr):
            s = str(val)
            # Emails
            for m in EMAIL_RE.findall(s):
                results.append({
                    "entity_type": "Identifier",
                    "subtype": "Email",
                    "value": m,
                    "table": table,
                    "rowid": str(rid),
                    "column": name
                })
            # UUIDs
            for m in UUID_RE.findall(s):
                results.append({
                    "entity_type": "Identifier",
                    "subtype": "UUID",
                    "value": m.lower(),
                    "table": table,
                    "rowid": str(rid),
                    "column": name
                })
            # Phones
            norm = normalize_phone(s) if (looks_like_phone_column(name) or re.search(r"\d", s)) else None
            if norm:
                results.append({
                    "entity_type": "Identifier",
                    "subtype": "Phone",
                    "value": norm,
                    "table": table,
                    "rowid": str(rid),
                    "column": name
                })
    return results

def extract_temporal(conn, table, cols, rid_expr):
    results = []
    for c in cols:
        name = c["name"]
        t = c["type_norm"]
        # Integer-like: Unix seconds/ms
        if t == "INT" or looks_like_time_column(name):
            for rid, val in fetch_distinct(conn, table, name, rid_expr):
                try:
                    iso = epoch_to_iso(int(val))
                    if iso:
                        results.append({
                            "entity_type": "Temporal",
                            "subtype": "UnixEpoch",
                            "value": iso,
                            "raw": str(val),
                            "table": table,
                            "rowid": str(rid),
                            "column": name
                        })
                except Exception:
                    pass
        # Text ISO-8601
        if t == "TEXT" or looks_like_time_column(name):
            for rid, val in fetch_distinct(conn, table, name, rid_expr):
                s = str(val)
                if ISO8601_RE.search(s):
                    results.append({
                        "entity_type": "Temporal",
                        "subtype": "ISO8601",
                        "value": s,
                        "table": table,
                        "rowid": str(rid),
                        "column": name
                    })
    return dedupe(results, keys=("entity_type","subtype","value","table","rowid","column"))

def extract_relational(conn, table, cols, rid_expr):
    # Simple heuristic: look for rows with two id-like columns that imply a link
    # e.g., sender_id + recipient_id, from_id + to_id, user_id + peer_user_id
    id_cols = [c["name"] for c in cols if re.search(r"(?:^|_)(user|sender|from|src|author|owner|recipient|to|dst|peer).*_?id$", c["name"], re.I)]
    results = []
    if len(id_cols) >= 2:
        # Try all ordered pairs
        pairs = []
        for a in id_cols:
            for b in id_cols:
                if a != b:
                    pairs.append((a,b))
        # Sample the first plausible pair by name priority
        priority = ["sender", "from", "src", "author", "owner", "user"]
        recv_pri = ["recipient", "to", "dst", "peer", "user"]
        def score(col, keys): 
            n = col.lower()
            for i,k in enumerate(keys):
                if k in n:
                    return len(keys)-i
            return 0
        pairs.sort(key=lambda ab: (score(ab[0], priority)+score(ab[1], recv_pri)), reverse=True)
        # Keep top 2 pairs to avoid explosion
        pairs = pairs[:2]
        for a,b in pairs:
            sql = f"SELECT {rid_expr} AS __rid__, {a} AS __a__, {b} AS __b__ FROM '{table}' WHERE {a} IS NOT NULL AND {b} IS NOT NULL"
            try:
                cur = conn.execute(sql)
                for row in cur:
                    va, vb = row["__a__"], row["__b__"]
                    results.append({
                        "entity_type": "Relational",
                        "subtype": f"{a}->{b}",
                        "value": f"{va}->{vb}",
                        "table": table,
                        "rowid": str(row["__rid__"]),
                        "column": f"{a},{b}"
                    })
            except sqlite3.OperationalError:
                continue
    return results

def dedupe(items, keys):
    seen = set()
    out = []
    for it in items:
        k = tuple(it.get(k) for k in keys)
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out

def write_output(records, out_path, fmt):
    if fmt == "json":
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
    elif fmt == "csv":
        if not records:
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                f.write("")
            return
        fields = sorted({k for r in records for k in r.keys()})
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(records)
    else:
        raise ValueError("Unsupported format")

def main():
    ap = argparse.ArgumentParser(description="Extract ground truth (value + Table + RowID/PK) from a SQLite database.")
    ap.add_argument("db", help="Path to SQLite DB")
    ap.add_argument("--entities", default="all", help="Comma list: identifier,temporal,relational or 'all'")
    ap.add_argument("--out", default="ground_truth.json", help="Output file path (.json or .csv)")
    ap.add_argument("--limit", type=int, default=None, help="Optional per-column scan limit")
    args = ap.parse_args()

    fmt = "json" if args.out.lower().endswith(".json") else ("csv" if args.out.lower().endswith(".csv") else "json")
    targets = set(e.strip().lower() for e in args.entities.split(",")) if args.entities != "all" else {"identifier","temporal","relational"}

    conn = connect(args.db)
    all_records = []
    try:
        for table in get_tables(conn):
            cols = get_table_info(conn, table)
            rid_expr, rid_kind = pk_expression(table, cols)
            if "identifier" in targets:
                all_records.extend(extract_identifiers(conn, table, cols, rid_expr))
            if "temporal" in targets:
                all_records.extend(extract_temporal(conn, table, cols, rid_expr))
            if "relational" in targets:
                all_records.extend(extract_relational(conn, table, cols, rid_expr))
        # Ensure unique and stable
        all_records = dedupe(all_records, keys=("entity_type","subtype","value","table","rowid","column"))
        write_output(all_records, args.out, fmt)
        print(f"Wrote {len(all_records)} records to {args.out}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
