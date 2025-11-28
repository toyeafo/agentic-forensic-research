#!/usr/bin/env python3
import argparse, json, csv, math, os
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, Any, List, Tuple, Optional, DefaultDict
from collections import defaultdict

# --------- Helpers ---------
def safe_mean(xs: List[float]) -> float:
    return mean(xs) if xs else 0.0

def safe_sd(xs: List[float]) -> float:
    return pstdev(xs) if len(xs) > 1 else 0.0

def pearson(x: List[float], y: List[float]) -> Optional[float]:
    if len(x) < 2 or len(y) < 2 or len(x) != len(y):
        return None
    mx, my = mean(x), mean(y)
    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    denx = math.sqrt(sum((a - mx) ** 2 for a in x))
    deny = math.sqrt(sum((b - my) ** 2 for b in y))
    if denx == 0 or deny == 0:
        return None
    return num / (denx * deny)

def f1(p: float, r: float) -> float:
    return (2 * p * r) / (p + r) if (p + r) > 0 else 0.0

def load_metrics_csv(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows

def try_parse_float(v, default=0.0):
    try:
        return float(v)
    except:
        return default

# --------- Load data (JSON runs + optional CSV summary) ---------
def load_runs(runs_dir: Path) -> List[Dict[str, Any]]:
    runs = []
    for p in runs_dir.glob("*.json"):
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            # Expected keys: db, entity_type, workflow, model, trial, usage, behavior, metrics, result
            runs.append(obj)
        except Exception:
            continue
    return runs

# --------- Aggregations ---------
def aggregate_quality(runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Aggregate by (entity_type, workflow)
    buckets: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in runs:
        ent = r.get("entity_type", "").lower()
        wf = r.get("workflow", "").lower()
        m = r.get("metrics")
        if not ent or not wf or not m:
            continue
        buckets[(ent, wf)].append(m)

    out = []
    for (ent, wf), ms in buckets.items():
        P = [try_parse_float(m.get("precision", 0)) for m in ms]
        R = [try_parse_float(m.get("recall", 0)) for m in ms]
        H = [try_parse_float(m.get("hallucination_rate", 0)) for m in ms]
        PV = [try_parse_float(m.get("provenance_completeness", 0)) for m in ms]
        out.append({
            "entity_type": ent,
            "workflow": wf,
            "n": len(ms),
            "precision_mean": round(safe_mean(P), 4),
            "precision_sd": round(safe_sd(P), 4),
            "recall_mean": round(safe_mean(R), 4),
            "recall_sd": round(safe_sd(R), 4),
            "hallucination_mean": round(safe_mean(H), 4),
            "hallucination_sd": round(safe_sd(H), 4),
            "provenance_mean": round(safe_mean(PV), 4),
            "provenance_sd": round(safe_sd(PV), 4),
            "f1_mean": round(safe_mean([f1(p, r) for p, r in zip(P, R)]), 4),
        })
    # Sort for readability
    out.sort(key=lambda d: (d["entity_type"], d["workflow"]))
    return out

def aggregate_behavior(runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Aggregate behavior by (entity_type, workflow)
    buckets: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in runs:
        ent = r.get("entity_type", "").lower()
        wf = r.get("workflow", "").lower()
        b = r.get("behavior") or {}
        if not ent or not wf or not b:
            continue
        buckets[(ent, wf)].append(b)

    out = []
    for (ent, wf), bs in buckets.items():
        sql_calls = [try_parse_float(b.get("sql_calls", 0)) for b in bs]
        schema_exp = [try_parse_float(b.get("schema_exploration", 0)) for b in bs]
        verif_calls = [try_parse_float(b.get("verification_calls", 0)) for b in bs]
        verif_ratio = [try_parse_float(b.get("verification_ratio", 0)) for b in bs]
        self_corr = [try_parse_float(b.get("self_corrections", 0)) for b in bs]
        out.append({
            "entity_type": ent,
            "workflow": wf,
            "n": len(bs),
            "sql_calls_mean": round(safe_mean(sql_calls), 3),
            "schema_explore_mean": round(safe_mean(schema_exp), 3),
            "verification_calls_mean": round(safe_mean(verif_calls), 3),
            "verification_ratio_mean": round(safe_mean(verif_ratio), 3),
            "self_corrections_mean": round(safe_mean(self_corr), 3),
        })
    out.sort(key=lambda d: (d["entity_type"], d["workflow"]))
    return out

def best_workflow_per_entity(quality_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Pick workflow with highest F1; tie-break by lower hallucination, then higher provenance
    by_ent: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in quality_rows:
        by_ent[r["entity_type"]].append(r)

    winners = []
    for ent, rows in by_ent.items():
        def key(r):
            return (
                r["f1_mean"],
                -r["hallucination_mean"],
                r["provenance_mean"]
            )
        best = sorted(rows, key=key, reverse=True)[0]
        winners.append({
            "entity_type": ent,
            "best_workflow": best["workflow"],
            "f1_mean": best["f1_mean"],
            "precision_mean": best["precision_mean"],
            "recall_mean": best["recall_mean"],
            "hallucination_mean": best["hallucination_mean"],
            "provenance_mean": best["provenance_mean"],
            "n": best["n"]
        })
    winners.sort(key=lambda d: d["entity_type"])
    return winners

def verify_vs_hallucination_correlation(runs: List[Dict[str, Any]]) -> Optional[float]:
    # Across all runs, correlate verification_ratio with hallucination_rate (per trial)
    xs, ys = [], []
    for r in runs:
        b = r.get("behavior") or {}
        m = r.get("metrics") or {}
        if "verification_ratio" in b and "hallucination_rate" in m:
            xs.append(try_parse_float(b["verification_ratio"]))
            ys.append(try_parse_float(m["hallucination_rate"]))
    return pearson(xs, ys)

def save_csv(rows: List[Dict[str, Any]], path: Path):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

def print_table(rows: List[Dict[str, Any]], title: str):
    print("\n" + title)
    if not rows:
        print("(no rows)")
        return
    fields = list(rows[0].keys())
    # Simple fixed-width print
    widths = {k: max(len(k), max(len(str(r.get(k,""))) for r in rows)) for k in fields}
    print(" | ".join(k.ljust(widths[k]) for k in fields))
    print("-+-".join("-" * widths[k] for k in fields))
    for r in rows:
        print(" | ".join(str(r.get(k,"")).ljust(widths[k]) for k in fields))

# --------- Main ---------
def main():
    ap = argparse.ArgumentParser(description="Analyze experiment runs for RQ1 (behavior) and RQ2 (quality).")
    ap.add_argument("--runs_dir", default="runs_out", help="Directory with run JSON files")
    ap.add_argument("--metrics_csv", default=None, help="Optional path to metrics_summary.csv (will be merged if present)")
    ap.add_argument("--outdir", default="analysis_out", help="Directory to write analysis CSVs")
    ap.add_argument("--by_db", action="store_true", help="Also output per-database summaries")
    args = ap.parse_args()

    runs_dir = Path(args.runs_dir)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    runs = load_runs(runs_dir)
    if not runs:
        print("No run JSON files found in", runs_dir)
        return

    # If metrics_summary.csv provided, optionally merge or just keep for reference
    metrics_rows = []
    if args.metrics_csv and Path(args.metrics_csv).exists():
        metrics_rows = load_metrics_csv(Path(args.metrics_csv))

    # RQ2: Quality by (entity_type, workflow)
    quality_rows = aggregate_quality(runs)
    save_csv(quality_rows, outdir / "rq2_quality_by_entity_workflow.csv")
    print_table(quality_rows, "RQ2: Quality by Entity Type × Workflow")

    # RQ1: Behavior by (entity_type, workflow)
    behavior_rows = aggregate_behavior(runs)
    save_csv(behavior_rows, outdir / "rq1_behavior_by_entity_workflow.csv")
    print_table(behavior_rows, "RQ1: Behavior by Entity Type × Workflow")

    # Best workflow per entity type (using F1 as primary criterion)
    winners = best_workflow_per_entity(quality_rows)
    save_csv(winners, outdir / "best_workflow_by_entity.csv")
    print_table(winners, "Best Workflow per Entity (by F1; tie-break: lower Hallucination, higher Provenance)")

    # Correlation: does verification reduce hallucination?
    corr = verify_vs_hallucination_correlation(runs)
    if corr is not None:
        print(f"\nCorrelation (Pearson) between Verification Ratio and Hallucination Rate: {corr:.3f} (negative suggests verification helps)")
        (outdir / "verification_vs_hallucination.txt").write_text(
            f"Pearson correlation: {corr:.6f}\n(negative suggests higher verification relates to lower hallucination)\n",
            encoding="utf-8"
        )
    else:
        print("\nNot enough data to compute correlation between verification ratio and hallucination rate.")

    # Optional: per-database summaries
    if args.by_db:
        by_db_q: DefaultDict[Tuple[str,str,str], List[Dict[str, Any]]] = defaultdict(list)
        by_db_b: DefaultDict[Tuple[str,str,str], List[Dict[str, Any]]] = defaultdict(list)
        for r in runs:
            db = r.get("db", "")
            ent = r.get("entity_type","").lower()
            wf = r.get("workflow","").lower()
            if r.get("metrics"):
                by_db_q[(db, ent, wf)].append(r["metrics"])
            if r.get("behavior"):
                by_db_b[(db, ent, wf)].append(r["behavior"])

        rows_q = []
        for (db, ent, wf), ms in by_db_q.items():
            P = [try_parse_float(m.get("precision", 0)) for m in ms]
            R = [try_parse_float(m.get("recall", 0)) for m in ms]
            H = [try_parse_float(m.get("hallucination_rate", 0)) for m in ms]
            PV = [try_parse_float(m.get("provenance_completeness", 0)) for m in ms]
            rows_q.append({
                "db": db, "entity_type": ent, "workflow": wf, "n": len(ms),
                "precision_mean": round(safe_mean(P),4),
                "recall_mean": round(safe_mean(R),4),
                "hallucination_mean": round(safe_mean(H),4),
                "provenance_mean": round(safe_mean(PV),4),
                "f1_mean": round(safe_mean([f1(p, r) for p, r in zip(P, R)]), 4)
            })
        rows_q.sort(key=lambda d: (d["db"], d["entity_type"], d["workflow"]))
        save_csv(rows_q, outdir / "rq2_quality_by_db_entity_workflow.csv")

        rows_b = []
        for (db, ent, wf), bs in by_db_b.items():
            sql_calls = [try_parse_float(b.get("sql_calls", 0)) for b in bs]
            schema_exp = [try_parse_float(b.get("schema_exploration", 0)) for b in bs]
            verif_calls = [try_parse_float(b.get("verification_calls", 0)) for b in bs]
            verif_ratio = [try_parse_float(b.get("verification_ratio", 0)) for b in bs]
            self_corr = [try_parse_float(b.get("self_corrections", 0)) for b in bs]
            rows_b.append({
                "db": db, "entity_type": ent, "workflow": wf, "n": len(bs),
                "sql_calls_mean": round(safe_mean(sql_calls),3),
                "schema_explore_mean": round(safe_mean(schema_exp),3),
                "verification_calls_mean": round(safe_mean(verif_calls),3),
                "verification_ratio_mean": round(safe_mean(verif_ratio),3),
                "self_corrections_mean": round(safe_mean(self_corr),3),
            })
        rows_b.sort(key=lambda d: (d["db"], d["entity_type"], d["workflow"]))
        save_csv(rows_b, outdir / "rq1_behavior_by_db_entity_workflow.csv")

    print(f"\nDone. CSV outputs written to: {outdir.resolve()}")
    print("Open rq2_quality_by_entity_workflow.csv and rq1_behavior_by_entity_workflow.csv for your paper tables.")

if __name__ == "__main__":
    main()
