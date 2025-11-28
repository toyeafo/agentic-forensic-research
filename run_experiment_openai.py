#!/usr/bin/env python3
import argparse, os, json, sqlite3, textwrap, sys, time, csv, re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set

# pip install openai
from openai import OpenAI

DEFAULT_W1 = """Instructions: You are an expert Digital Forensics Investigator. Your goal is to answer the user's query by querying the attached SQLite database.
Protocol:
1) Plan which tables/columns might contain the evidence.
2) Use the execute_sqlite_query tool to inspect schema (e.g., SELECT name FROM sqlite_master WHERE type='table') and retrieve rows.
3) Return only values that appear in SQL results. No guessing."""

DEFAULT_W2 = """Instructions: You are a Constrained Forensic Agent. Validate formats before reporting.
Strict Protocol:
1) Start with schema inspection using the tool.
2) Validate formats in SQL when possible:
   - Emails: ensure exactly one '@' and a dot after it; length >= 6
   - Timestamps (integers): check plausible Unix seconds (>= 946684800 and <= 4102444800) or ms (>= 946684800000 and <= 4102444800000)
   - ISO timestamps: 'YYYY-MM-DD' prefix
3) Only output rows that pass the requested Entity Type's validation."""

DEFAULT_W3 = """Instructions: You are an Auditable Forensic Agent. Primary directive: Zero Hallucination, Perfect Provenance.
Mandatory: Every finding must include Value, Source Table, and Source RowID/PK from SQL results.
Process:
1) Inspect schema using the tool (tables + PRAGMA table_info).
2) Retrieval queries must select both the evidence column and the PK/ROWID (e.g., SELECT id, email FROM users).
3) Only report values returned by SQL and include the exact Table and RowID/PK for each."""

ENTITY_TASK_TEMPLATES = {
    "identifier": "Discover all identifier evidence (emails, phone numbers, UUIDs) in this database.",
    "temporal":   "Discover all temporal evidence (Unix/ISO timestamps) in this database.",
    "relational": "Discover relational links (e.g., sender_id -> recipient_id, user_id -> peer_user_id) in this database.",
}

# ---------- SQLite tool ----------
def is_select_sql(sql: str) -> bool:
    s = sql.strip().lower()
    return s.startswith("select") or s.startswith("pragma")

def run_sql(db_path: Path, sql: str, limit: int = 50) -> Dict[str, Any]:
    if not is_select_sql(sql):
        return {"ok": False, "error": "Only SELECT/PRAGMA statements are allowed."}
    if sql.strip().lower().startswith("select") and " limit " not in sql.lower():
        sql = sql.rstrip().rstrip(";") + f" LIMIT {limit};"
    try:
        con = sqlite3.connect(str(db_path))
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute(sql)
        if cur.description is None:
            rows = cur.fetchall()
            out = [[v for v in r] for r in rows]
            return {"ok": True, "columns": [], "rows": out, "rowcount": len(out)}
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        out = [[r[c] for c in cols] for r in rows]
        return {"ok": True, "columns": cols, "rows": out, "rowcount": len(out)}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        try:
            con.close()
        except Exception:
            pass

def find_dbs(input_path: Path) -> List[Path]:
    if input_path.is_file():
        return [input_path]
    exts = {".db", ".sqlite", ".sqlite3"}
    hits = []
    for dp, _, fns in os.walk(input_path):
        for fn in fns:
            p = Path(dp) / fn
            if p.suffix.lower() in exts:
                hits.append(p)
    return hits

# ---------- Prompts ----------
def load_prompt_file(path: Optional[str], fallback: str) -> str:
    if path and Path(path).exists():
        return Path(path).read_text(encoding="utf-8")
    return fallback

def make_system_prompt(workflow_text: str) -> str:
    return textwrap.dedent(f"""\
    You are an agent that must query a local SQLite database via a tool.
    Respond concisely. Return a JSON object with this schema:
    {{
      "findings": [
        {{"value": "<string>", "table": "<string>", "rowid": "<string>"}}
      ]
    }}
    If nothing is found, return {{"findings":[]}}.

    {workflow_text}
    """)

def make_user_prompt(entity_type: str) -> str:
    task = ENTITY_TASK_TEMPLATES[entity_type]
    guidance = ""
    if entity_type == "identifier":
        guidance = "Focus on emails/phones/UUIDs. Prefer selecting the evidence column and a PK/ROWID."
    elif entity_type == "temporal":
        guidance = "Check plausible ranges for Unix seconds/ms and ISO-8601 patterns."
    else:
        guidance = "Look for pairs of id-like columns within the same table (e.g., sender_id and recipient_id)."
    return f"{task}\n{guidance}\nOnly output the JSON object—no commentary."

# ---------- Ground truth loading ----------
def read_gt_json(path: Path) -> List[Dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))

def read_gt_csv(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({k.strip(): v for k, v in r.items()})
    return rows

def normalize_str(x: Any) -> str:
    return "" if x is None else str(x).strip()

def load_ground_truth_for_db(gt_file: Path, entity_type: str) -> Set[Tuple[str, str, str]]:
    if gt_file.suffix.lower() == ".json":
        data = read_gt_json(gt_file)
    elif gt_file.suffix.lower() == ".csv":
        data = read_gt_csv(gt_file)
    else:
        return set()

    triples = set()
    for r in data:
        et = normalize_str(r.get("entity_type") or r.get("EntityType")).lower()
        if et != entity_type:
            continue
        val = normalize_str(r.get("value") or r.get("Value"))
        table = normalize_str(r.get("table") or r.get("Table"))
        rowid = normalize_str(r.get("rowid") or r.get("RowID") or r.get("row_id") or r.get("pk"))
        if val and table and rowid:
            triples.add((val, table, rowid))
    return triples

def find_gt_file_for_db(gt_dir: Optional[Path], gt_manifest: Optional[Path], db_path: Path) -> Optional[Path]:
    # 1) Manifest CSV override
    if gt_manifest and gt_manifest.exists():
        with open(gt_manifest, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if Path(r["db"]).resolve() == db_path.resolve():
                    cand = Path(r["gt_path"])
                    return cand if cand.exists() else None
    # 2) Search in gt_dir by name
    if not gt_dir or not gt_dir.exists():
        return None
    stem = db_path.name
    candidates = []
    for p in gt_dir.glob("**/*"):
        if p.is_file() and p.suffix.lower() in {".json", ".csv"} and stem in p.name:
            candidates.append(p)
    # prefer files with 'ground_truth' in name
    candidates.sort(key=lambda p: (("ground_truth" not in p.name.lower()), len(p.name)))
    return candidates[0] if candidates else None

# ---------- Behavior metrics (RQ1 proxies) ----------
RE_TABLE = re.compile(r"\bfrom\s+([`\"[]?\w+[`\"[]?)", re.I)
def table_name_from_sql(sql: str) -> Optional[str]:
    m = RE_TABLE.search(sql or "")
    if m:
        return m.group(1).strip("`\"[]")
    return None

def is_schema_explore(sql: str) -> bool:
    s = (sql or "").lower()
    return "sqlite_master" in s or "pragma table_info" in s

def is_verification_like(sql: str) -> bool:
    s = (sql or "").lower()
    # simple signals: LIKE/INSTR/LENGTH/BETWEEN/DATE/DATETIME and ISO patterns
    return any(k in s for k in [" like ", " glob ", " instr(", " length(", " between ", " datetime(", " date("]) or "yyyy-mm-dd" in s

def count_self_corrections(sqls: List[str]) -> int:
    # very simple heuristic: repeating selects on same table where later query adds WHERE or extra constraints
    corrections = 0
    last_by_table: Dict[str, str] = {}
    for s in sqls:
        tbl = table_name_from_sql(s or "")
        if not tbl:
            continue
        prev = last_by_table.get(tbl)
        cur_has_where = " where " in (s or "").lower()
        if prev and cur_has_where and len(s) > len(prev) and (prev.lower() in s.lower()):
            corrections += 1
        last_by_table[tbl] = s
    return corrections

# ---------- Scoring ----------
def score_run(findings: List[Dict[str, str]], gt_set: Set[Tuple[str, str, str]]) -> Dict[str, Any]:
    preds = set()
    for f in findings:
        val = normalize_str(f.get("value"))
        table = normalize_str(f.get("table"))
        rowid = normalize_str(f.get("rowid"))
        if val and table and rowid:
            preds.add((val, table, rowid))

    tp = len(preds & gt_set)
    fp = len(preds - gt_set)
    fn = len(gt_set - preds)
    total_pred = len(preds)
    total_gt = len(gt_set)

    precision = (tp / (tp + fp)) if (tp + fp) > 0 else 0.0
    recall = (tp / (tp + fn)) if (tp + fn) > 0 else 0.0
    hallucination_rate = (fp / total_pred) if total_pred > 0 else 0.0
    provenance_completeness = (tp / total_pred) if total_pred > 0 else 0.0  # fraction of predictions with correct coords

    return {
        "TP": tp, "FP": fp, "FN": fn,
        "total_pred": total_pred, "total_gt": total_gt,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "hallucination_rate": round(hallucination_rate, 4),
        "provenance_completeness": round(provenance_completeness, 4),
    }

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Run agentic digital evidence experiments with OpenAI (with optional scoring).")
    ap.add_argument("input", help="Path to a SQLite DB or a folder of DBs")
    ap.add_argument("--entities", default="identifier,temporal,relational", help="Comma list: identifier,temporal,relational")
    ap.add_argument("--workflows", default="w1,w2,w3", help="Comma list: w1,w2,w3")
    ap.add_argument("--model", default="gpt-4o-mini", help="OpenAI model (gpt-4o-mini is low cost)")
    ap.add_argument("--max_steps", type=int, default=5, help="Max tool-call iterations per trial")
    ap.add_argument("--max_rows", type=int, default=50, help="Max rows returned per SQL call")
    ap.add_argument("--trials", type=int, default=1, help="Trials per (DB×Entity×Workflow)")
    ap.add_argument("--seed", type=int, default=None, help="Optional seed")
    ap.add_argument("--outdir", default="runs_out", help="Output directory")
    ap.add_argument("--prompt_w1", default=None, help="Path to prompt_w1.txt")
    ap.add_argument("--prompt_w2", default=None, help="Path to prompt_w2.txt")
    ap.add_argument("--prompt_w3", default=None, help="Path to prompt_w3.txt")
    # scoring inputs
    ap.add_argument("--gt_dir", default=None, help="Directory with ground truth files (JSON/CSV)")
    ap.add_argument("--gt_manifest", default=None, help="CSV with columns: db,gt_path")
    args = ap.parse_args()

    # Load workflow prompts (your files if provided)
    w1 = load_prompt_file(args.prompt_w1, DEFAULT_W1)
    w2 = load_prompt_file(args.prompt_w2, DEFAULT_W2)
    w3 = load_prompt_file(args.prompt_w3, DEFAULT_W3)
    WF = {"w1": w1, "w2": w2, "w3": w3}

    entities = [e.strip().lower() for e in args.entities.split(",") if e.strip()]
    workflows = [w.strip().lower() for w in args.workflows.split(",") if w.strip()]
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    metrics_csv = outdir / "metrics_summary.csv"
    if not metrics_csv.exists():
        with open(metrics_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "db","entity_type","workflow","trial","model",
                "TP","FP","FN","total_pred","total_gt",
                "precision","recall","hallucination_rate","provenance_completeness",
                "sql_calls","schema_exploration","verification_calls","verification_ratio",
                "self_corrections","total_tokens"
            ])

    gt_dir = Path(args.gt_dir) if args.gt_dir else None
    gt_manifest = Path(args.gt_manifest) if args.gt_manifest else None

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    tool_def = [{
        "type": "function",
        "function": {
            "name": "execute_sqlite_query",
            "description": "Execute a SELECT/PRAGMA on the active SQLite DB and return rows (limited).",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL statement (SELECT/PRAGMA only)"},
                },
                "required": ["sql"]
            }
        }
    }]

    dbs = find_dbs(Path(args.input))
    if not dbs:
        print("No databases found.", file=sys.stderr)
        sys.exit(2)

    for db in dbs:
        for entity in entities:
            for wf in workflows:
                wf_text = WF.get(wf, w1)
                system_prompt = make_system_prompt(wf_text)
                user_prompt = make_user_prompt(entity)

                for trial in range(1, args.trials + 1):
                    messages = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ]

                    total_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
                    tool_calls_used = 0
                    tool_log: List[Dict[str, Any]] = []
                    issued_sqls: List[str] = []

                    for step in range(args.max_steps):
                        resp = client.chat.completions.create(
                            model=args.model,
                            temperature=0,
                            max_tokens=350,
                            messages=messages,
                            tools=tool_def,
                            tool_choice="auto",
                            seed=args.seed
                        )
                        choice = resp.choices[0]
                        total_usage["prompt_tokens"] += getattr(resp.usage, "prompt_tokens", 0) or 0
                        total_usage["completion_tokens"] += getattr(resp.usage, "completion_tokens", 0) or 0
                        total_usage["total_tokens"] += getattr(resp.usage, "total_tokens", 0) or 0

                        if choice.finish_reason == "tool_calls" and choice.message.tool_calls:
                            for tc in choice.message.tool_calls:
                                if tc.function.name == "execute_sqlite_query":
                                    sql = json.loads(tc.function.arguments).get("sql", "")
                                    result = run_sql(db, sql, limit=args.max_rows)
                                    tool_calls_used += 1
                                    issued_sqls.append(sql)
                                    tool_log.append({
                                        "sql": sql, "ok": result.get("ok"),
                                        "rowcount": result.get("rowcount", 0),
                                        "error": result.get("error")
                                    })
                                    messages.append({"role": "assistant", "tool_calls": [tc]})
                                    messages.append({
                                        "role": "tool",
                                        "tool_call_id": tc.id,
                                        "name": "execute_sqlite_query",
                                        "content": json.dumps(result)[:8000]
                                    })
                                    if tool_calls_used >= args.max_steps:
                                        break
                            if tool_calls_used >= args.max_steps:
                                messages.append({"role": "user", "content": "Finalize now with the JSON findings only."})
                                continue
                        else:
                            content = choice.message.content or ""
                            findings = {"findings": []}
                            try:
                                start = content.find("{"); end = content.rfind("}")
                                if start != -1 and end != -1:
                                    findings = json.loads(content[start:end+1])
                            except Exception:
                                findings = {"findings": []}

                            # Behavior metrics (RQ1 proxies)
                            sql_calls = len(issued_sqls)
                            schema_exploration = sum(1 for s in issued_sqls if is_schema_explore(s))
                            verification_calls = sum(1 for s in issued_sqls if is_verification_like(s))
                            verification_ratio = (verification_calls / sql_calls) if sql_calls > 0 else 0.0
                            self_corr = count_self_corrections(issued_sqls)

                            # Optional scoring if GT available
                            gt_file = find_gt_file_for_db(gt_dir, gt_manifest, db) if (gt_dir or gt_manifest) else None
                            metrics = None
                            if gt_file:
                                gt_set = load_ground_truth_for_db(gt_file, entity)
                                metrics = score_run(findings.get("findings", []), gt_set)

                            record = {
                                "db": str(db),
                                "entity_type": entity,
                                "workflow": wf,
                                "model": args.model,
                                "trial": trial,
                                "timestamp": int(time.time()),
                                "usage": total_usage,
                                "tool_calls": tool_calls_used,
                                "tool_log": tool_log,
                                "behavior": {
                                    "sql_calls": sql_calls,
                                    "schema_exploration": schema_exploration,
                                    "verification_calls": verification_calls,
                                    "verification_ratio": round(verification_ratio, 4),
                                    "self_corrections": self_corr
                                },
                                "metrics": metrics,  # may be None if no GT
                                "result": findings
                            }

                            # Save JSON
                            relname = db.name
                            outname = f"{relname}.{entity}.{wf}.trial{trial}.json"
                            (outdir / outname).write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")

                            # Append CSV summary if metrics available
                            if metrics:
                                with open(metrics_csv, "a", newline="", encoding="utf-8") as f:
                                    w = csv.writer(f)
                                    w.writerow([
                                        str(db), entity, wf, trial, args.model,
                                        metrics["TP"], metrics["FP"], metrics["FN"],
                                        metrics["total_pred"], metrics["total_gt"],
                                        metrics["precision"], metrics["recall"],
                                        metrics["hallucination_rate"], metrics["provenance_completeness"],
                                        sql_calls, schema_exploration, verification_calls,
                                        round(verification_ratio,4), self_corr,
                                        total_usage["total_tokens"]
                                    ])

                            print(f"Wrote {outname} | tokens={total_usage['total_tokens']} tools={tool_calls_used} " +
                                  (f"| P={metrics['precision']} R={metrics['recall']} H={metrics['hallucination_rate']} Prov={metrics['provenance_completeness']}" if metrics else "| (no GT)"))
                            break

if __name__ == "__main__":
    main()
