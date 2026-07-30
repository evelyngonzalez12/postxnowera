#!/usr/bin/env python3
"""Write + validate GOOGLE_SERVICE_ACCOUNT_JSON (or legacy GOOGLE_OAUTH_JSON) into google_creds.json"""
import json
import os
import sys

# Prefer new name; fall back to old secret name for compatibility
raw = (
    os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    or os.environ.get("GOOGLE_OAUTH_JSON")
    or ""
).strip()

if not raw:
    sys.exit(
        "ERROR: secret is empty. Set GOOGLE_SERVICE_ACCOUNT_JSON "
        "(full service-account JSON with type/private_key/client_email)."
    )

try:
    data = json.loads(raw)
except json.JSONDecodeError as e:
    sys.exit(f"Invalid JSON in Google credentials secret: {e}")

out_path = "google_creds.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

print("Credential keys:", sorted(data.keys()))
print("  type:", data.get("type") or "MISSING")
print("  client_email:", data.get("client_email") or data.get("client_id") or "MISSING")
print("  project_id:", data.get("project_id") or "(n/a)")
print("  private_key:", "OK" if data.get("private_key") else "MISSING")

if data.get("type") == "service_account":
    missing = [k for k in ("private_key", "client_email", "token_uri") if not data.get(k)]
    if missing:
        sys.exit("Service account JSON missing: " + ", ".join(missing))
    print("Service-account secret looks OK")
    print(f"Share your Google Sheet with Editor: {data.get('client_email')}")
else:
    # legacy user OAuth still accepted
    missing = [k for k in ("client_id", "client_secret", "refresh_token", "token_uri") if not data.get(k)]
    if missing:
        sys.exit(
            "Not a service_account JSON and OAuth fields incomplete: "
            + ", ".join(missing)
            + ". Prefer type=service_account JSON (the one that works in your googleauth.py test)."
        )
    print("User-OAuth secret looks OK (legacy)")
