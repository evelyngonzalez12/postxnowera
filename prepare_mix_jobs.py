#!/usr/bin/env python3
"""
prepare_mix_jobs.py
- Reads Google Sheet (Settings, Accounts, Captions, CustomCaptions, Threads, Urls, MediaInventory)
- Unlocks stale account locks
- Claims free accounts (storage_state_json rows) for this RUN_ID
- Builds a dynamic job list (image/video/thread/link) — NO LockQueue sheet
- Writes job_plan.json artifact + matrix for parallel workers
- Single secret: GOOGLE_OAUTH_JSON (OAuth user credentials)
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path

import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

SHEETS_RPM = 55
SHEETS_WINDOW = 60.0

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
POST_COUNT = int(os.environ.get("POST_COUNT", "10"))
IMAGE_PERCENT = float(os.environ.get("IMAGE_PERCENT", "30"))
VIDEO_PERCENT = float(os.environ.get("VIDEO_PERCENT", "25"))
THREAD_PERCENT = float(os.environ.get("THREAD_PERCENT", "20"))
LINK_PERCENT = float(os.environ.get("LINK_PERCENT", "25"))
PARALLEL_JOBS = max(1, min(10, int(os.environ.get("PARALLEL_JOBS", "3"))))
CAPTION_SOURCE = os.environ.get("CAPTION_SOURCE", "filename").strip().lower()
SHUFFLE = os.environ.get("SHUFFLE_ORDER", "true").lower() == "true"
RUN_ID = os.environ.get("RUN_ID", "local")
MEGA_SOURCE_OVERRIDE = os.environ.get("MEGA_SOURCE_FOLDER_OVERRIDE", "").strip()
MEGA_CLAIMED_OVERRIDE = os.environ.get("MEGA_CLAIMED_FOLDER_OVERRIDE", "").strip()
UNLOCK_STALE_HOURS = float(os.environ.get("UNLOCK_STALE_HOURS", "6"))
GOOGLE_CREDS_PATH = os.environ.get("GOOGLE_CREDS_PATH") or os.environ.get("GOOGLE_OAUTH_PATH") or "google_creds.json"


def dbg(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class RateLimitedSheets:
    def __init__(self, client: gspread.Client, rpm: int = SHEETS_RPM):
        self.client = client
        self.rpm = rpm
        self._ts: deque[float] = deque()

    def _wait_slot(self) -> None:
        now = time.time()
        while self._ts and now - self._ts[0] >= SHEETS_WINDOW:
            self._ts.popleft()
        if len(self._ts) >= self.rpm:
            sleep_for = SHEETS_WINDOW - (now - self._ts[0]) + 0.2
            dbg(f"  [RATE] {len(self._ts)}/{self.rpm} — sleep {sleep_for:.1f}s")
            time.sleep(max(0.1, sleep_for))
            now = time.time()
            while self._ts and now - self._ts[0] >= SHEETS_WINDOW:
                self._ts.popleft()
        self._ts.append(time.time())

    def _call(self, fn, *args, **kwargs):
        for attempt in range(1, 15):
            self._wait_slot()
            try:
                return fn(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                resp = getattr(e, "response", None)
                code = resp.status_code if resp is not None else 0
                if code == 429 or "RATE_LIMIT" in str(e) or "Quota exceeded" in str(e):
                    wait = 60.0 - (time.time() % 60) + 1.5
                    dbg(f"  [RATE] 429 — wait {wait:.1f}s (try {attempt})")
                    time.sleep(wait)
                    self._ts.clear()
                    continue
                raise
        raise RuntimeError("Sheets rate-limit retries exhausted")

    def open_by_key(self, key):
        return self._call(self.client.open_by_key, key)

    def worksheet(self, sh, title):
        return self._call(sh.worksheet, title)

    def get_all_records(self, ws):
        return self._call(ws.get_all_records)

    def get_all_values(self, ws):
        return self._call(ws.get_all_values)

    def update_cell(self, ws, row, col, value):
        return self._call(ws.update_cell, row, col, value)

    def row_values(self, ws, row):
        return self._call(ws.row_values, row)



def load_google_creds(path: str):
    """Load service-account (preferred) or user-OAuth credentials from JSON file."""
    from datetime import datetime, timezone
    from google.oauth2.credentials import Credentials
    from google.oauth2.service_account import Credentials as SACredentials
    from google.auth.transport.requests import Request
    from google.auth.exceptions import RefreshError

    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Credentials file not found: {path}")
    raw = p.read_text(encoding="utf-8").strip()
    if not raw:
        raise SystemExit(
            "Credentials file is EMPTY. Set secret GOOGLE_SERVICE_ACCOUNT_JSON "
            "to your full service-account JSON (type/private_key/client_email)."
        )
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SystemExit(f"Credentials JSON invalid: {e}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if data.get("type") == "service_account":
        dbg(f"Using service account: {data.get('client_email')}")
        return SACredentials.from_service_account_info(data, scopes=scopes)

    # Legacy user OAuth path
    required = ["client_id", "client_secret", "refresh_token", "token_uri"]
    missing = [k for k in required if not (data.get(k) or "").strip()]
    if missing:
        raise SystemExit(
            "Not service_account and OAuth missing: "
            + ", ".join(missing)
            + ". Use the working service-account JSON (type=service_account)."
        )
    needed = set(scopes)
    scopes = list(set(data.get("scopes") or []) | needed)
    expiry = None
    exp_raw = data.get("expiry") or data.get("expires_at")
    if exp_raw:
        try:
            s = str(exp_raw).replace("Z", "+00:00")
            expiry = datetime.fromisoformat(s)
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
        except Exception:
            expiry = None
    creds = Credentials(
        token=data.get("token") or None,
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=scopes,
        expiry=expiry,
    )
    if not creds.valid:
        try:
            creds.refresh(Request())
            dbg("OAuth access token refreshed OK")
        except RefreshError as e:
            raise SystemExit(
                f"OAuth refresh failed: {e}. Prefer service-account JSON instead."
            ) from e
    return creds


def build_oauth_client(path: str):
    creds = load_google_creds(path)
    return gspread.authorize(creds)


def load_settings(rl, sh) -> dict:
    out = {}
    try:
        ws = rl.worksheet(sh, "Settings")
        for r in rl.get_all_records(ws):
            k = (r.get("Key") or r.get("key") or r.get("Setting") or "").strip()
            v = (r.get("Value") or r.get("value") or "").strip()
            if k:
                out[k] = v
    except Exception as e:
        dbg(f"Settings: {e}")
    return out


def ensure_account_lock_columns(rl, ws, headers: list[str]) -> list[str]:
    """Add locked_by_run_id / locked_at headers if missing."""
    changed = False
    headers = list(headers)
    for col_name in ("locked_by_run_id", "locked_at"):
        if col_name not in [h.strip().lower() for h in headers]:
            # append header in next empty column
            col = len(headers) + 1
            rl.update_cell(ws, 1, col, col_name)
            headers.append(col_name)
            changed = True
            dbg(f"Added column {col_name} to Accounts")
    return headers


def parse_locked_at(val: str):
    if not val:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(val.strip()[:19].replace("Z", ""), "%Y-%m-%dT%H:%M:%S" if "T" in val else fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        return None


def claim_accounts(rl, sh, need: int) -> list[dict]:
    """
    Auto-detect accounts from rows that have non-empty storage_state_json.
    Unlock stale locks, then claim up to `need` free accounts for this RUN_ID.
    """
    ws = rl.worksheet(sh, "Accounts")
    values = rl.get_all_values(ws)
    if not values:
        sys.exit("Accounts tab is empty")
    headers = [h.strip() for h in values[0]]
    headers = ensure_account_lock_columns(rl, ws, headers)
    # re-read after possible header add
    values = rl.get_all_values(ws)
    headers = [h.strip() for h in values[0]]

    def idx(name, default=None):
        name_l = name.lower()
        for i, h in enumerate(headers):
            if h.lower() == name_l:
                return i
        return default

    i_id = idx("account_id", 0)
    i_en = idx("enabled")
    i_state = idx("storage_state_json")
    i_lock = idx("locked_by_run_id")
    i_lat = idx("locked_at")

    if i_state is None:
        sys.exit("Accounts tab must have storage_state_json column")

    now = datetime.now(timezone.utc)
    stale_before = now - timedelta(hours=UNLOCK_STALE_HOURS)
    candidates = []

    for row_num, row in enumerate(values[1:], start=2):
        while len(row) < len(headers):
            row.append("")
        state = (row[i_state] or "").strip()
        # Treat empty / demo placeholders as missing
        placeholder = state in (
            "",
            "{}",
            '{"cookies":[],"origins":[]}',
            "{'cookies': [], 'origins': []}",
        )
        if placeholder or (state.startswith("{") and '"cookies"' in state and len(state) < 40):
            dbg(f"Skip Accounts row {row_num}: storage_state_json empty or demo placeholder")
            continue
        enabled = "true"
        if i_en is not None:
            enabled = str(row[i_en] or "true").strip().lower()
        if enabled not in ("1", "true", "yes", "y"):
            continue

        lock_run = (row[i_lock] or "").strip() if i_lock is not None else ""
        lock_at = parse_locked_at(row[i_lat] or "") if i_lat is not None else None

        # unlock stale
        if lock_run and lock_at and lock_at < stale_before:
            dbg(f"Unlocking stale account row {row_num} (locked by {lock_run})")
            if i_lock is not None:
                rl.update_cell(ws, row_num, i_lock + 1, "")
            if i_lat is not None:
                rl.update_cell(ws, row_num, i_lat + 1, "")
            lock_run = ""

        if lock_run and lock_run != RUN_ID:
            dbg(f"Skip account row {row_num}: locked by run {lock_run}")
            continue

        aid = (row[i_id] or f"acc_row{row_num}").strip() if i_id is not None else f"acc_row{row_num}"
        candidates.append({
            "account_id": aid,
            "storage_state_json": state,
            "row_number": row_num,
            "_i_lock": i_lock,
            "_i_lat": i_lat,
            "_ws": ws,
        })

    if not candidates:
        dbg("HINT: Each enabled Accounts row needs a REAL Playwright storage_state JSON")
        dbg("      in column storage_state_json (not empty, not just {\"cookies\":[],\"origins\":[]}).")
        dbg("      Also check locked_by_run_id is empty (or stale locks older than unlock_stale_hours).")
        dbg("      Share the sheet with the service-account email as Editor.")
        sys.exit(
            "No free accounts with storage_state_json. "
            "Fill Accounts.storage_state_json with a real X login state, "
            "or clear locked_by_run_id / wait for stale unlock."
        )

    # Prefer accounts not yet locked by us, shuffle for fairness
    random.shuffle(candidates)
    claimed = candidates[: max(need, 1)]
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    for acc in claimed:
        if acc["_i_lock"] is not None:
            rl.update_cell(acc["_ws"], acc["row_number"], acc["_i_lock"] + 1, RUN_ID)
        if acc["_i_lat"] is not None:
            rl.update_cell(acc["_ws"], acc["row_number"], acc["_i_lat"] + 1, now_iso)
        dbg(f"Claimed account {acc['account_id']} (row {acc['row_number']})")

    # strip internal keys for plan
    clean = []
    for a in claimed:
        clean.append({
            "account_id": a["account_id"],
            "storage_state_json": a["storage_state_json"],
            "row_number": a["row_number"],
        })
    return clean


def compute_counts(post_count, image_pct, video_pct, thread_pct, link_pct):
    total = image_pct + video_pct + thread_pct + link_pct
    if total <= 0:
        sys.exit("All percentages are 0")
    n_img = round(post_count * image_pct / total) if image_pct > 0 else 0
    n_vid = round(post_count * video_pct / total) if video_pct > 0 else 0
    n_thr = round(post_count * thread_pct / total) if thread_pct > 0 else 0
    n_lnk = post_count - n_img - n_vid - n_thr
    if link_pct <= 0:
        n_lnk = 0
        # redistribute remainder into largest bucket
        rem = post_count - n_img - n_vid - n_thr
        if rem > 0 and n_img >= n_vid and n_img >= n_thr:
            n_img += rem
        elif rem > 0 and n_vid >= n_thr:
            n_vid += rem
        elif rem > 0:
            n_thr += rem
    if n_lnk < 0:
        n_lnk = 0
    return n_img, n_vid, n_thr, n_lnk


def load_caption_map(rl, sh) -> dict:
    out = {}
    try:
        ws = rl.worksheet(sh, "Captions")
        for r in rl.get_all_records(ws):
            fname = (r.get("file_name") or r.get("filename") or r.get("File") or "").strip()
            if not fname:
                continue
            out[fname.lower()] = r
            out[fname.rsplit(".", 1)[0].lower()] = r
    except Exception as e:
        dbg(f"Captions: {e}")
    return out


def load_custom_captions(rl, sh) -> list[str]:
    out = []
    try:
        ws = rl.worksheet(sh, "CustomCaptions")
        for r in rl.get_all_records(ws):
            t = (r.get("caption") or r.get("Caption") or r.get("text") or "").strip()
            if t:
                out.append(t)
    except Exception as e:
        dbg(f"CustomCaptions: {e}")
    return out


def load_threads(rl, sh) -> list[dict]:
    out = []
    try:
        ws = rl.worksheet(sh, "Threads")
        records = rl.get_all_records(ws)
        for i, r in enumerate(records, start=2):
            text = (r.get("text") or r.get("caption") or r.get("thread") or r.get("Text") or "").strip()
            if not text:
                for v in r.values():
                    if str(v).strip():
                        text = str(v).strip()
                        break
            if text:
                out.append({"row_number": i, "text": text})
    except Exception as e:
        dbg(f"Threads: {e}")
    return out


def load_urls(rl, sh) -> list[dict]:
    """Urls tab: url, caption, hashtags (hashtags optional)."""
    out = []
    try:
        ws = rl.worksheet(sh, "Urls")
        for i, r in enumerate(rl.get_all_records(ws), start=2):
            url = (r.get("url") or r.get("URL") or r.get("link") or r.get("Link") or "").strip()
            if not url:
                continue
            caption = (r.get("caption") or r.get("Caption") or "").strip()
            hashtags = (r.get("hashtags") or r.get("Hashtags") or "").strip()
            out.append({"row_number": i, "url": url, "caption": caption, "hashtags": hashtags})
    except Exception as e:
        dbg(f"Urls tab: {e}")
    return out


def load_media(rl, sh):
    images, videos = [], []
    try:
        ws = rl.worksheet(sh, "MediaInventory")
        for r in rl.get_all_records(ws):
            status = str(r.get("status", "available")).lower()
            if status not in ("available", "", "ready"):
                continue
            name = (r.get("file_name") or r.get("filename") or r.get("name") or "").strip()
            if not name:
                continue
            kind = str(r.get("kind") or r.get("type") or "").lower()
            entry = {"file_name": name, "kind": kind}
            if kind == "video" or name.lower().endswith((".mp4", ".mov", ".avi", ".mpeg", ".webm", ".m4v")):
                videos.append(entry)
            else:
                images.append(entry)
    except Exception as e:
        dbg(f"MediaInventory: {e}")
    return images, videos


def build_filename_caption(row: dict, file_name: str) -> str:
    if row:
        parts = [
            str(row.get("action_caption") or row.get("Action Caption") or "").strip(),
            str(row.get("caption") or row.get("Caption") or "").strip(),
            str(row.get("hashtags") or row.get("Hashtags") or "").strip(),
        ]
        return "\n".join(p for p in parts if p)
    stem = file_name.rsplit(".", 1)[0].replace("_", " ").replace("-", " ")
    return stem


def build_link_text(url_row: dict) -> str:
    parts = []
    cap = (url_row.get("caption") or "").strip()
    tags = (url_row.get("hashtags") or "").strip()
    if cap:
        parts.append(cap)
    if tags:
        parts.append(tags)
    parts.append(url_row["url"])
    return "\n\n".join(parts) if len(parts) > 1 else parts[0]


def main():
    dbg("=" * 60)
    dbg("prepare_mix_jobs.py")
    dbg(f"POST_COUNT={POST_COUNT} PARALLEL={PARALLEL_JOBS} CAPTION_SOURCE={CAPTION_SOURCE}")
    dbg("=" * 60)

    rl = RateLimitedSheets(build_oauth_client(GOOGLE_CREDS_PATH))
    sh = rl.open_by_key(SPREADSHEET_ID)

    settings = load_settings(rl, sh)
    mega_source = MEGA_SOURCE_OVERRIDE or settings.get("mega_source_folder", "Source")
    mega_claimed = MEGA_CLAIMED_OVERRIDE or settings.get("mega_claimed_folder", "Claimed")
    thread_delimiter = settings.get("thread_delimiter", "---")

    n_img, n_vid, n_thr, n_lnk = compute_counts(
        POST_COUNT, IMAGE_PERCENT, VIDEO_PERCENT, THREAD_PERCENT, LINK_PERCENT
    )
    dbg(f"Planned: images={n_img} videos={n_vid} threads={n_thr} links={n_lnk}")

    # Claim accounts — at least as many as parallel workers, ideally cover post_count
    accounts = claim_accounts(rl, sh, need=max(PARALLEL_JOBS, min(POST_COUNT, 20)))
    dbg(f"Using {len(accounts)} account(s)")

    caption_map = load_caption_map(rl, sh)
    custom_caps = load_custom_captions(rl, sh)
    threads = load_threads(rl, sh)
    urls = load_urls(rl, sh)
    media_images, media_videos = load_media(rl, sh)

    if CAPTION_SOURCE == "custom" and not custom_caps and (n_img or n_vid):
        dbg("WARN: caption_source=custom but CustomCaptions empty — falling back to filename/stem")

    if media_images:
        n_img = min(n_img, len(media_images))
    if media_videos:
        n_vid = min(n_vid, len(media_videos))
    n_thr = min(n_thr, len(threads)) if threads else (0 if n_thr else 0)
    if not threads and n_thr:
        dbg("No thread rows — capping threads to 0")
        n_thr = 0
    n_lnk = min(n_lnk, len(urls)) if urls else (0 if n_lnk else 0)
    if not urls and n_lnk:
        dbg("No Urls rows — capping links to 0")
        n_lnk = 0

    jobs = []

    if media_images and n_img:
        chosen = random.sample(media_images, n_img) if SHUFFLE else media_images[:n_img]
        for m in chosen:
            jobs.append({"kind": "image", "file_name": m["file_name"]})
    else:
        for _ in range(n_img):
            jobs.append({"kind": "image", "file_name": ""})

    if media_videos and n_vid:
        chosen = random.sample(media_videos, n_vid) if SHUFFLE else media_videos[:n_vid]
        for m in chosen:
            jobs.append({"kind": "video", "file_name": m["file_name"]})
    else:
        for _ in range(n_vid):
            jobs.append({"kind": "video", "file_name": ""})

    if threads and n_thr:
        chosen = random.sample(threads, n_thr) if SHUFFLE else threads[:n_thr]
        for t in chosen:
            jobs.append({"kind": "thread", "thread_row": t["row_number"], "text": t["text"]})

    if urls and n_lnk:
        chosen = random.sample(urls, n_lnk) if SHUFFLE else urls[:n_lnk]
        for u in chosen:
            jobs.append({
                "kind": "link",
                "url": u["url"],
                "caption": u["caption"],
                "hashtags": u["hashtags"],
                "url_row": u["row_number"],
            })

    # captions for image/video
    for job in jobs:
        if job["kind"] not in ("image", "video"):
            continue
        fn = job.get("file_name") or ""
        if CAPTION_SOURCE == "custom" and custom_caps:
            job["caption_text"] = random.choice(custom_caps)
        elif fn:
            key = fn.lower()
            stem = fn.rsplit(".", 1)[0].lower()
            row = caption_map.get(key) or caption_map.get(stem)
            job["caption_text"] = build_filename_caption(row, fn)
        else:
            job["caption_text"] = random.choice(custom_caps) if custom_caps else ""

    for job in jobs:
        if job["kind"] == "link":
            job["caption_text"] = build_link_text(job)

    if SHUFFLE:
        random.shuffle(jobs)

    if not jobs:
        dbg("No jobs planned")
        Path("job_plan.json").write_text(json.dumps({"jobs": [], "accounts": [], "settings": {}, "workers": {}}))
        Path("worker_meta.json").write_text(json.dumps({"n_workers": 0, "worker_ids": []}))
        return

    # Round-robin accounts onto jobs (storage state lives only under plan["accounts"])
    for i, job in enumerate(jobs):
        acc = accounts[i % len(accounts)]
        job["account_id"] = acc["account_id"]

    n_workers = min(PARALLEL_JOBS, len(jobs))
    # Split jobs across workers
    buckets = [[] for _ in range(n_workers)]
    for i, job in enumerate(jobs):
        buckets[i % n_workers].append(job)

    plan = {
        "run_id": RUN_ID,
        "settings": {
            "mega_source_folder": mega_source,
            "mega_claimed_folder": mega_claimed,
            "thread_delimiter": thread_delimiter,
        },
        "accounts": accounts,
        "workers": {
            str(i): buckets[i] for i in range(n_workers)
        },
    }
    Path("job_plan.json").write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    dbg(f"Wrote job_plan.json — {len(jobs)} jobs across {n_workers} workers")

    worker_ids = list(range(n_workers))
    meta = {"n_workers": n_workers, "worker_ids": worker_ids}
    Path("worker_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    dbg(f"Wrote worker_meta.json n_workers={n_workers} ids={worker_ids}")
    dbg("prepare done")


if __name__ == "__main__":
    main()
