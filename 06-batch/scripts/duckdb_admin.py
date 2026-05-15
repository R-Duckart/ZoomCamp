"""
DuckDB Admin UI Launcher

This script starts a local DuckDB UI server for interactive database exploration
and management. The UI provides a web-based interface to query and visualize data.

Usage:
    python duckdb_admin.py

The server runs on http://localhost:4213 and can be stopped with Ctrl+C.
"""

import os
import duckdb
import webbrowser

# Path to the DuckDB database file
# DuckDB stores all tables, views, and data in this single file
db_path = os.path.join('..', 'datawarehouse', 'duckdb', 'taxi.duckdb')

# Connect to the DuckDB database
# If the file doesn't exist, DuckDB will create it automatically
conn = duckdb.connect(db_path)

# Start the DuckDB UI server
# This launches a local web server for the interactive UI
conn.sql("CALL start_ui_server()")

# Automatically open the UI in the default web browser
webbrowser.open("http://localhost:4213")

# User feedback
print("DuckDB UI running on http://localhost:4213 — Press Ctrl+C to stop")

# Keep the server running until user interrupts (Ctrl+C)
try:
    while True:
        pass
except KeyboardInterrupt:
    print("\nStopped.")