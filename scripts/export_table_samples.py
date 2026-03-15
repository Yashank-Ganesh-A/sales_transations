"""
Export sample rows from the SQLite tables to text files.
Run after: python pipeline/run_pipeline_sqlite.py
Use the files in outputs/table_samples/ to show tables in your video.
"""
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse.db"
OUT_DIR = PROJECT_ROOT / "outputs" / "table_samples"


def run():
    if not DB_PATH.exists():
        print("Run the pipeline first: python pipeline/run_pipeline_sqlite.py")
        return
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # List tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]

    for table in tables:
        cur.execute(f"SELECT * FROM {table} LIMIT 15")
        rows = cur.fetchall()
        names = [d[0] for d in cur.description]
        path = OUT_DIR / f"sample_{table}.txt"
        with open(path, "w") as f:
            f.write(f"Table: {table}\n")
            f.write("Columns: " + ", ".join(names) + "\n")
            f.write("-" * 60 + "\n")
            for row in rows:
                f.write("\t".join(str(c) for c in row) + "\n")
        print(f"Wrote {path}")

    conn.close()
    print(f"Done. Open files in {OUT_DIR} to show tables in your video.")


if __name__ == "__main__":
    run()
