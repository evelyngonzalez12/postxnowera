#!/usr/bin/env python3
"""Clear Accounts.locked_by_run_id / locked_at for the current RUN_ID."""
import json
import os
import sys
from pathlib import Path

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import gspread

path = os.environ.get("GOOGLE_OAUTH_PATH", "google_oauth.json")
spreadsheet_id = os.environ.get("SPREADSHEET_ID", "")
run_id = os.environ.get("RUN_ID", "")

if not spreadsheet_id or not run_id:
    print("SPREADSHEET_ID or RUN_ID missing — skip")
    sys.exit(0)

data = json.loads(Path(path).read_text(encoding="utf-8"))
scopes = list(
    set(data.get("scopes") or [])
    | {
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    }
)
creds = Credentials(
    token=data.get("token"),
    refresh_token=data.get("refresh_token"),
    token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
    client_id=data.get("client_id"),
    client_secret=data.get("client_secret"),
    scopes=scopes,
)
if (not creds.valid) and creds.refresh_token:
    try:
        creds.refresh(Request())
    except Exception as e:
        print(f"Token refresh warning: {e}")

gc = gspread.authorize(creds)
sh = gc.open_by_key(spreadsheet_id)
ws = sh.worksheet("Accounts")
rows = ws.get_all_records()
headers = [h.strip() for h in ws.row_values(1)]

if "locked_by_run_id" not in headers:
    print("No locked_by_run_id column — nothing to clear")
    sys.exit(0)

col = headers.index("locked_by_run_id") + 1
lat_col = headers.index("locked_at") + 1 if "locked_at" in headers else None
cleared = 0
for i, r in enumerate(rows, start=2):
    if str(r.get("locked_by_run_id", "")).strip() == run_id:
        ws.update_cell(i, col, "")
        if lat_col is not None:
            ws.update_cell(i, lat_col, "")
        cleared += 1
print(f"Cleared {cleared} account lock(s) for run {run_id}")
