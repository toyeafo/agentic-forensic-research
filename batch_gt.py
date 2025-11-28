#!/usr/bin/env python3
import argparse, os, sys, subprocess
from pathlib import Path

DB_EXTS = {".db", ".sqlite", ".sqlite3"}

def find_dbs(root: Path):
    if root.is_file() and root.suffix.lower() in DB_EXTS:
        yield root
        return
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            p = Path(dirpath) / fn
            if p.suffix.lower() in DB_EXTS:
                yield p

def main():
    ap = argparse.ArgumentParser(description="Recursively extract ground truth from SQLite DBs using gt_extract.py")
    ap.add_argument("input", help="Path to a DB file or a directory containing DBs")
    ap.add_argument("--outdir", default="gt_out", help="Directory to write outputs")
    ap.add_argument("--entities", default="all", help="identifier,temporal,relational or 'all'")
    ap.add_argument("--fmt", default="json", choices=["json","csv"], help="Output format per DB")
    ap.add_argument("--limit", type=int, default=None, help="Optional per-column scan limit passed to gt_extract.py")
    args = ap.parse_args()

    gt_script = Path(__file__).parent / "gt_extract.py"
    if not gt_script.exists():
        print("Error: gt_extract.py not found next to this script. Save your extractor as gt_extract.py.", file=sys.stderr)
        sys.exit(1)

    root = Path(args.input).resolve()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    dbs = list(find_dbs(root))
    if not dbs:
        print("No SQLite databases found.", file=sys.stderr)
        sys.exit(2)

    print(f"Found {len(dbs)} database(s). Writing outputs to {outdir}")

    successes, failures = 0, 0
    for db in dbs:
        rel = db.name if root.is_file() else str(db.relative_to(root)).replace(os.sep, "__")
        out_name = f"{rel}.ground_truth.{args.fmt}"
        out_path = outdir / out_name

        cmd = [sys.executable, str(gt_script), str(db), "--entities", args.entities, "--out", str(out_path)]
        if args.limit is not None:
            cmd += ["--limit", str(args.limit)]

        print(f"[RUN] {db} -> {out_path}")
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if res.returncode == 0:
                print(res.stdout.strip())
                successes += 1
            else:
                print(f"[ERROR] {db}:\n{res.stdout}\n{res.stderr}", file=sys.stderr)
                failures += 1
        except Exception as e:
            print(f"[EXCEPTION] {db}: {e}", file=sys.stderr)
            failures += 1

    print(f"Done. Success: {successes}, Failures: {failures}, Out dir: {outdir}")

if __name__ == "__main__":
    main()
