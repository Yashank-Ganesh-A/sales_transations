"""
Show tables and sample rows from warehouse.db – no sqlite3 command needed.
Run: python scripts/show_tables.py
Works on Windows, Mac, Linux. Use this if sqlite3 is not installed.
"""
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse.db"


def main():
    if not DB_PATH.exists():
        print("Run the pipeline first: python pipeline/run_pipeline_sqlite.py")
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    print("Tables in warehouse.db:", ", ".join(tables))
    print()
    for table in tables:
        cur.execute(f"SELECT * FROM {table} LIMIT 8")
        rows = cur.fetchall()
        names = [d[0] for d in cur.description]
        print(f"--- {table} (columns: {', '.join(names)}) ---")
        for row in rows:
            print("  ", row)
        print()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
