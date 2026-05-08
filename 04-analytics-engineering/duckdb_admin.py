import os

import duckdb
import webbrowser

db_path = os.path.join('..', '06-batch', 'datawarehouse', 'duckdb', 'taxi.duckdb')

conn = duckdb.connect(db_path)
conn.sql("CALL start_ui_server()")
webbrowser.open("http://localhost:4213")

print("DuckDB UI running on http://localhost:4213 — Press Ctrl+C to stop")
try:
    while True:
        pass
except KeyboardInterrupt:
    print("\nStopped.")