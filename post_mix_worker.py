cat > /home/workdir/artifacts/post_mix_worker.py << 'PYEOF'
#!/usr/bin/env python3
"""
post_mix_worker.py
------------------
One GitHub Actions matrix worker.
- Claims jobs from LockQueue tab (atomic-ish via status flip + rate-limited retries)
- Downloads media from mega.nz (source folder) / moves to claimed folder after success
- Posts images, videos, or threads to X using Playwright + per-account storage state
- Caption modes:
    filename  → use caption_text already filled by prepare (from Captions tab + file name)
    custom    → caption_text is a random CustomCaptions entry (also pre-filled)
- All Sheet traffic goes through RateLimitedSheets (≤55 req/min, sleep+retry on 429)
- Locks ensure workers never double-claim the same row
"""
from __future__ import annotations

import json
import os
import random
import sys
import tempfile
import time
from collections import deque
from pathlib import Path
from typing import Any, Optional

import gspread
from google.oauth2.service_account import Credentials as SACredentials
from google.oauth2.credentials import Credentials as UserCredentials
from playwright.sync_api import sync_playwright

# mega.py
try:
    from mega import Mega
except ImportError:
    Mega = None  # type: ignore

# ── Env ──────────────────────────────────────────────────────────────────────
SHEET_CREDS_JSON = os.environ.get("SHEET_CREDENTIALS_JSON", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
WORKER_ID = os.environ.get("WORKER_ID", "0")
RUN_ID = os.environ.get("RUN_ID", "local")
CAPTION_SOURCE = os.environ.get("CAPTION_SOURCE", "filename").strip().lower()
INTERVAL_MINUTES = float(os.environ.get("INTERVAL_MINUTES", "10"))
INTERVAL_SECONDS = int(INTERVAL_MINUTES * 60)
RUN_BUDGET_MINUTES = float(os.environ.get("RUN_BUDGET_MINUTES", "355"))
MEGA_EMAIL_FALLBACK = os.environ.get("MEGA_EMAIL", "")
MEGA_PASSWORD_FALLBACK = os.environ.get("MEGA_PASSWORD", "")

SHEETS_RPM = 55
SHEETS_WINDOW = 60.0
MAX_CONSECUTIVE_SESSION_FAILURES = 2
SESSION_ERROR_KEYWORDS = ("login", "session", "restriction", "graduated-access")

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mpeg", ".mpg", ".webm", ".m4v"}

SCREENSHOT_DIR = Path("debug_screenshots")
SCREENSHOT_DIR.mkdir(exist_ok=True)
step_counter = [0]


def dbg(msg: str) -> None:
    print(f"[W{WORKER_ID} {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def screenshot(page, label: str) -> None:
    step_counter[0] += 1
    path = SCREENSHOT_DIR / f"{step_counter[0]:03d}_w{WORKER_ID}_{label}.png"
    try:
        page.screenshot(path=str(path), full_page=False)
        dbg(f" 📸 {path}")
    except Exception as e:
        dbg(f" 📸 failed ({label}): {e}")


# ── Rate-limited Sheets ──────────────────────────────────────────────────────
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
            dbg(f"  [RATE] Sheets {len(self._ts)}/{self.rpm} — sleep {sleep_for:.1f}s")
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
                    dbg(f"  [RATE] 429 — wait {wait:.1f}s until minute completes (try {attempt})")
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

    def update(self, ws, range_name, values, **kw):
        return self._call(ws.update, range_name, values, **kw)

    def update_cell(self, ws, row, col, value):
        return self._call(ws.update_cell, row, col, value)

    def find(self, ws, query):
        return self._call(ws.find, query)

    def col_values(self, ws, col):
        return self._call(ws.col_values, col)

    def delete_rows(self, ws, row):
        return self._call(ws.delete_rows, row)


def build_creds(creds_json_str, scopes):
    if not creds_json_str.strip():
        sys.exit("SHEET_CREDENTIALS_JSON empty")
    data = json.loads(creds_json_str)
    if data.get("type") == "service_account":
        return SACredentials.from_service_account_info(data, scopes=scopes)
    return UserCredentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes", scopes),
    )


# ── Lock claim / release ─────────────────────────────────────────────────────
LOCK_HEADERS = [
    "lock_id", "run_id", "kind", "status", "claimed_by", "claimed_at",
    "file_name", "mega_node_id", "account_id", "caption_text",
    "thread_row", "result", "tweet_id", "error",
]


def _header_index(headers: list[str], name: str) -> int:
    name_l = name.lower()
    for i, h in enumerate(headers):
        if h.strip().lower() == name_l:
            return i
    raise KeyError(name)


def claim_next_job(rl: RateLimitedSheets, lock_ws) -> Optional[dict]:
    """
    Scan LockQueue for first row where run_id==RUN_ID and status==available.
    Flip status → claimed + claimed_by=WORKER_ID in one update.
    Because multiple workers race, we re-read the cell after write; if another
    worker won, we skip and try the next row. Retries respect rate limits.
    """
    values = rl.get_all_values(lock_ws)
    if not values or len(values) < 2:
        return None
    headers = [h.strip() for h in values[0]]
    try:
        idx_run = _header_index(headers, "run_id")
        idx_status = _header_index(headers, "status")
        idx_claimed_by = _header_index(headers, "claimed_by")
        idx_claimed_at = _header_index(headers, "claimed_at")
    except KeyError as e:
        dbg(f"LockQueue missing column: {e}")
        return None

    for row_num, row in enumerate(values[1:], start=2):
        # pad short rows
        while len(row) < len(headers):
            row.append("")
        if row[idx_run] != RUN_ID:
            continue
        if row[idx_status].strip().lower() != "available":
            continue

        # Attempt claim
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        # Update status + claimed_by + claimed_at (3 cells)
        # Use a single range update for atomicity-ish
        start_col = chr(ord("A") + idx_status)
        end_col = chr(ord("A") + max(idx_status, idx_claimed_by, idx_claimed_at))
        # Build full row slice for the three fields — simpler: update each with rate limit
        try:
            rl.update_cell(lock_ws, row_num, idx_status + 1, "claimed")
            rl.update_cell(lock_ws, row_num, idx_claimed_by + 1, f"worker-{WORKER_ID}")
            rl.update_cell(lock_ws, row_num, idx_claimed_at + 1, now_iso)
        except Exception as e:
            dbg(f"Claim write failed row {row_num}: {e}")
            continue

        # Re-read to confirm we still own it
        time.sleep(0.3)
        confirm = rl.get_all_values(lock_ws)
        if row_num - 1 >= len(confirm):
            continue
        conf_row = confirm[row_num - 1]
        while len(conf_row) < len(headers):
            conf_row.append("")
        if conf_row[idx_status].strip().lower() != "claimed":
            continue
        if conf_row[idx_claimed_by].strip() != f"worker-{WORKER_ID}":
            dbg(f"Row {row_num} claimed by someone else — skip")
            continue

        # Build job dict
        job = {headers[i]: conf_row[i] if i < len(conf_row) else "" for i in range(len(headers))}
        job["_row_number"] = row_num
        dbg(f"✓ Claimed lock_id={job.get('lock_id')} kind={job.get('kind')} file={job.get('file_name')}")
        return job

    return None


def mark_job(rl: RateLimitedSheets, lock_ws, job: dict, result: str, tweet_id: str = "", error: str = ""):
    row_num = job["_row_number"]
    values = rl.get_all_values(lock_ws)
    headers = [h.strip() for h in values[0]]
    updates = {
        "status": "posted" if result == "SUCCESS" else "failed",
        "result": result,
        "tweet_id": tweet_id or "",
        "error": (error or "")[:500],
    }
    for col_name, val in updates.items():
        try:
            col = _header_index(headers, col_name) + 1
            rl.update_cell(lock_ws, row_num, col, val)
        except Exception as e:
            dbg(f"mark_job {col_name} failed: {e}")


# ── mega.nz helpers ──────────────────────────────────────────────────────────
def mega_login(email: str, password: str):
    if Mega is None:
        sys.exit("mega.py not installed")
    if not email or not password:
        sys.exit("mega credentials missing (Accounts tab or MEGA_EMAIL / MEGA_PASSWORD secrets)")
    dbg(f"Logging into mega.nz as {email[:3]}***…")
    m = Mega()
    return m.login(email, password)


def mega_find_folder(m, folder_name: str):
    """Return node handle for folder (first match)."""
    found = m.find(folder_name, exclude_deleted=True)
    if not found:
        # try recursive path style
        files = m.get_files()
        for h, meta in files.items():
            if meta.get("a", {}).get("n") == folder_name and meta.get("t") == 1:
                return h
        raise RuntimeError(f"mega folder not found: {folder_name!r}")
    # find returns (handle, meta) or list
    if isinstance(found, list):
        return found[0]
    return found[0] if isinstance(found, tuple) else found


def mega_list_in_folder(m, folder_handle, kind: str) -> list[dict]:
    """List files whose parent is folder_handle and extension matches kind."""
    files = m.get_files()
    out = []
    for h, meta in files.items():
        if meta.get("t") != 0:  # 0 = file
            continue
        parent = meta.get("p")
        if parent != folder_handle:
            continue
        name = meta.get("a", {}).get("n", "")
        if not name:
            continue
        ext = Path(name).suffix.lower()
        if kind == "image" and ext not in IMAGE_EXTS:
            continue
        if kind == "video" and ext not in VIDEO_EXTS:
            continue
        out.append({"handle": h, "name": name, "meta": meta})
    return out


def mega_download(m, file_handle, dest_path: str, max_attempts: int = 4):
    for attempt in range(1, max_attempts + 1):
        dbg(f" mega download attempt {attempt}/{max_attempts} → {dest_path}")
        try:
            # mega.py download accepts (file, dest_path)
            # file can be handle or the tuple from find
            m.download({"h": file_handle} if isinstance(file_handle, str) else file_handle,
                       dest_path if dest_path.endswith(os.sep) else str(Path(dest_path).parent) + os.sep)
            # library saves with original name; rename if needed
            # Simpler approach: download to cwd then move
            return
        except Exception as e:
            dbg(f" download error: {e}")
            if attempt < max_attempts:
                time.sleep(5 * attempt)
            else:
                raise


def mega_download_by_name(m, folder_handle, file_name: str, dest_dir: str) -> str:
    """Download a specific file by name from folder; return local path."""
    files = mega_list_in_folder(m, folder_handle, "image") + mega_list_in_folder(m, folder_handle, "video")
    target = None
    for f in files:
        if f["name"] == file_name or f["name"].lower() == file_name.lower():
            target = f
            break
    if target is None:
        # try find globally
        found = m.find(file_name, exclude_deleted=True)
        if found:
            target = {"handle": found[0] if isinstance(found, (list, tuple)) else found, "name": file_name}
        else:
            raise RuntimeError(f"File {file_name!r} not found in mega source folder")

    dest_dir_path = Path(dest_dir)
    dest_dir_path.mkdir(parents=True, exist_ok=True)
    # mega.py download writes into dest path using original filename
    m.download(target["handle"] if isinstance(target["handle"], dict) else target["handle"],
               str(dest_dir_path) + os.sep)
    local = dest_dir_path / target["name"]
    if not local.exists():
        # some versions put file in cwd
        cwd_file = Path(target["name"])
        if cwd_file.exists():
            cwd_file.rename(local)
    if not local.exists():
        raise RuntimeError(f"Download finished but file missing: {local}")
    return str(local)


def mega_move_to_claimed(m, file_handle, claimed_folder_handle, file_name: str):
    try:
        m.move(file_handle, claimed_folder_handle)
        dbg(f" ✓ Moved '{file_name}' → claimed folder")
    except Exception as e:
        dbg(f" ⚠ move failed for '{file_name}': {e}")


# ── Playwright / X posting (same robust selectors as original) ───────────────
TEXTBOX_SELECTORS = [
    '[data-testid="tweetTextarea_0"]',
    '[data-testid="tweetTextarea_0EditorContainer"] div[contenteditable="true"]',
    'div[contenteditable="true"][data-testid]',
    'div[contenteditable="true"][aria-label]',
    'div[contenteditable="true"]',
    '[aria-label="Post text"]',
    '[placeholder="What is happening?!"]',
    '[placeholder*="happening"]',
]
POST_BUTTON_SELECTORS = [
    '[data-testid="tweetButton"]',
    '[data-testid="tweetButtonInline"]',
    'button[data-testid*="tweet"]',
    'div[data-testid="tweetButton"]',
    'button:has-text("Post")',
    'button:has-text("Tweet")',
]
ADD_THREAD_TWEET_SELECTORS = [
    '[data-testid="addButton"]',
    'div[aria-label="Add post"]',
    'button[aria-label="Add post"]',
]
PREVIEW_SELECTORS = [
    '[data-testid="attachments"] video',
    '[data-testid="videoComponent"]',
    '[data-testid="tweetPhoto"]',
    '[data-testid="attachments"] img',
    '[data-testid="attachments"] [role="progressbar"]',
    '[data-testid="attachments"]',
    'img[src*="blob:"]',
    'video[src*="blob:"]',
]
VIDEO_MIMES = {"video/mp4", "video/quicktime", "video/x-msvideo", "video/mpeg"}


def find_element_multi(page, selectors, label, timeout=15_000):
    for sel in selectors:
        try:
            el = page.locator(sel).first
            el.wait_for(state="visible", timeout=timeout // len(selectors))
            dbg(f" ✓ Found {label}: {sel}")
            return el
        except Exception:
            pass
    return None


def _textbox_visible(page):
    for sel in TEXTBOX_SELECTORS[:3]:
        try:
            if page.locator(sel).first.is_visible():
                return True
        except Exception:
            pass
    return False


def _is_genuinely_logged_out(page):
    try:
        page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=20_000)
        page.wait_for_timeout(2_500)
        current = page.url
        if "login" in current or "signin" in current or "graduated-access" in current:
            return True
        try:
            page.locator('[data-testid="AppTabBar_Home_Link"], [data-testid="SideNav_NewTweet_Button"]').first.wait_for(
                state="visible", timeout=8_000
            )
            return False
        except Exception:
            return True
    except Exception:
        return True


def navigate_to_compose(page, post_index, attempt=1):
    for url in ["https://x.com/compose/post", "https://twitter.com/compose/tweet"]:
        dbg(f" [NAV] attempt {attempt} → {url}")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(3_000)
            current = page.url
            screenshot(page, f"p{post_index}_nav{attempt}")
            if "login" in current or "signin" in current or "graduated-access" in current:
                if _is_genuinely_logged_out(page):
                    raise RuntimeError("Session expired or account restricted (confirmed).")
                continue
            if "compose" in current or _textbox_visible(page):
                return True
        except RuntimeError:
            raise
        except Exception as e:
            dbg(f" [NAV] {e}")
    try:
        page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(3_000)
        btn = page.locator(
            'a[href="/compose/post"], [data-testid="SideNav_NewTweet_Button"], [aria-label="Post"]'
        ).first
        btn.wait_for(state="visible", timeout=10_000)
        btn.click()
        page.wait_for_timeout(3_000)
        if _textbox_visible(page):
            return True
    except Exception as e:
        dbg(f" [NAV] home fallback: {e}")
    return False


def type_into_textbox(page, locator, text):
    locator.scroll_into_view_if_needed()
    locator.click()
    page.wait_for_timeout(400)
    page.keyboard.type(text, delay=15)
    page.wait_for_timeout(400)


def _wait_for_preview(page, timeout, post_index, label):
    for sel in PREVIEW_SELECTORS:
        try:
            page.wait_for_selector(sel, timeout=timeout // len(PREVIEW_SELECTORS))
            try:
                page.wait_for_selector('[role="progressbar"]', state="detached", timeout=timeout)
            except Exception:
                pass
            page.wait_for_timeout(1_200)
            screenshot(page, f"p{post_index}_preview_{label}")
            return True
        except Exception:
            continue
    return False


def attach_media_robust(page, media_path, is_video, post_index):
    upload_timeout = 120_000 if is_video else 45_000
    file_inputs = page.locator('input[type="file"]')
    count = file_inputs.count()
    dbg(f" [ATTACH] {count} file input(s)")
    if count > 0:
        for idx in range(count):
            inp = file_inputs.nth(idx)
            accept = inp.get_attribute("accept") or ""
            if (is_video and ("video" in accept or accept == "")) or (
                not is_video and ("image" in accept or accept == "")
            ):
                try:
                    inp.set_input_files(media_path)
                    if _wait_for_preview(page, upload_timeout, post_index, f"slot{idx}"):
                        return True
                except Exception as e:
                    dbg(f" [ATTACH] input #{idx}: {e}")
        for idx in range(count):
            try:
                file_inputs.nth(idx).set_input_files(media_path)
                if _wait_for_preview(page, upload_timeout, post_index, f"fb{idx}"):
                    return True
            except Exception:
                pass
    for btn_sel in ['[data-testid="addMedia"]', '[aria-label*="edia"]', '[aria-label*="hoto"]']:
        try:
            btn = page.locator(btn_sel).first
            if btn.is_visible():
                btn.click()
                page.wait_for_timeout(1_000)
                fi2 = page.locator('input[type="file"]')
                if fi2.count() > 0:
                    fi2.first.set_input_files(media_path)
                    if _wait_for_preview(page, upload_timeout, post_index, "toolbar"):
                        return True
        except Exception:
            pass
    screenshot(page, f"p{post_index}_attach_failed")
    raise RuntimeError("All media attachment strategies failed.")


def add_thread_tweet_box(page, post_index, tweet_index):
    btn = find_element_multi(page, ADD_THREAD_TWEET_SELECTORS, "add-thread", timeout=10_000)
    if btn is None:
        raise RuntimeError("Could not find Add post button")
    btn.click()
    page.wait_for_timeout(800)
    locator = page.locator(f'[data-testid="tweetTextarea_{tweet_index}"]').first
    try:
        locator.wait_for(state="visible", timeout=10_000)
        return locator
    except Exception:
        all_ce = page.locator('div[contenteditable="true"][data-testid]')
        return all_ce.nth(all_ce.count() - 1)


def click_post_button(page, post_index):
    post_btn = find_element_multi(page, POST_BUTTON_SELECTORS, "post button", timeout=15_000)
    if post_btn is None:
        raise RuntimeError("Post button not found")
    if post_btn.is_disabled():
        for _ in range(3):
            page.wait_for_timeout(5_000)
            if not post_btn.is_disabled():
                break
    try:
        post_btn.click()
    except Exception:
        page.evaluate(
            """(sel) => { for (const s of sel) { const el = document.querySelector(s); if (el) { el.click(); return s; } } return null; }""",
            POST_BUTTON_SELECTORS,
        )


def post_with_network_confirmation(page, post_index, click_timeout=25_000):
    result = {"sent": None, "tweet_id": None}

    def on_response(response):
        if "CreateTweet" not in response.url:
            return
        try:
            data = response.json()
        except Exception:
            return
        if isinstance(data, dict) and data.get("errors"):
            result["sent"] = False
            return
        try:
            tweet_id = data["data"]["create_tweet"]["tweet_results"]["result"]["rest_id"]
            result["sent"] = True
            result["tweet_id"] = tweet_id
        except (KeyError, TypeError):
            result["sent"] = False

    page.on("response", on_response)
    try:
        click_post_button(page, post_index)
        waited = 0
        while waited < click_timeout:
            page.wait_for_timeout(500)
            waited += 500
            if result["sent"] is False:
                break
        page.wait_for_timeout(1_500)
    finally:
        page.remove_listener("response", on_response)
    screenshot(page, f"p{post_index}_after_click")
    if result["sent"]:
        return True, result["tweet_id"]
    return False, None


def post_one_job(page, job_payload, post_index, max_attempts=3):
    session_error = False
    for attempt in range(1, max_attempts + 1):
        dbg(f" ─── [{job_payload['kind'].upper()}] attempt {attempt}/{max_attempts} ───")
        try:
            navigate_to_compose(page, post_index, attempt)
            first_box = find_element_multi(page, TEXTBOX_SELECTORS, "textbox", timeout=20_000)
            if first_box is None:
                raise RuntimeError("Could not find first tweet textbox.")
            type_into_textbox(page, first_box, job_payload["tweets"][0])
            screenshot(page, f"p{post_index}_a{attempt}_t1")
            if job_payload.get("media_path"):
                is_video = job_payload["kind"] == "video"
                attach_media_robust(page, job_payload["media_path"], is_video, post_index)
            for idx in range(1, len(job_payload["tweets"])):
                box = add_thread_tweet_box(page, post_index, idx)
                type_into_textbox(page, box, job_payload["tweets"][idx])
            sent, tweet_id = post_with_network_confirmation(page, post_index)
            if sent:
                dbg(f" SUCCESS tweet_id={tweet_id}")
                return True, False, tweet_id
            if attempt < max_attempts:
                page.wait_for_timeout(5_000)
        except RuntimeError as e:
            dbg(f" RuntimeError: {e}")
            if any(k in str(e).lower() for k in SESSION_ERROR_KEYWORDS):
                session_error = True
                if attempt < max_attempts:
                    page.wait_for_timeout(15_000)
                    continue
                return False, True, None
            if attempt < max_attempts:
                page.wait_for_timeout(10_000)
            else:
                return False, False, None
        except Exception as e:
            dbg(f" Unexpected: {e}")
            if attempt < max_attempts:
                page.wait_for_timeout(10_000)
            else:
                return False, False, None
    return False, session_error, None


def split_into_tweets(text: str, delimiter: str) -> list[str]:
    parts = [p.strip() for p in text.split(delimiter)]
    parts = [p for p in parts if p]
    return parts if parts else [text.strip()]


def load_account_map(rl, sh) -> dict[str, dict]:
    out = {}
    try:
        ws = rl.worksheet(sh, "Accounts")
        for r in rl.get_all_records(ws):
            aid = str(r.get("account_id") or r.get("Account") or "").strip()
            if aid:
                out[aid] = r
    except Exception as e:
        dbg(f"Accounts load: {e}")
    return out


def load_settings(rl, sh) -> dict[str, str]:
    out = {}
    try:
        ws = rl.worksheet(sh, "Settings")
        for r in rl.get_all_records(ws):
            k = (r.get("Key") or r.get("key") or r.get("Setting") or "").strip()
            v = (r.get("Value") or r.get("value") or "").strip()
            if k:
                out[k] = v
    except Exception:
        pass
    return out


def write_storage_state(account_row: dict, path: str) -> str:
    """
    Prefer storage_state_json column (full JSON string).
    Fallback: storage_state_secret name is not resolvable here — use file on disk if present.
    """
    raw = (account_row.get("storage_state_json") or account_row.get("StorageState") or "").strip()
    if raw:
        Path(path).write_text(raw, encoding="utf-8")
        return path
    # fallback shared state from env written by workflow (optional)
    env_state = os.environ.get("X_STORAGE_STATE_JSON", "")
    if env_state:
        Path(path).write_text(env_state, encoding="utf-8")
        return path
    if Path("x_storage_state.json").exists():
        return "x_storage_state.json"
    raise RuntimeError(
        f"No storage state for account {account_row.get('account_id')}. "
        "Put JSON in Accounts.storage_state_json or set secret."
    )


def main():
    run_start = time.time()
    run_deadline = run_start + RUN_BUDGET_MINUTES * 60
    dbg("=" * 60)
    dbg(f"Worker {WORKER_ID} starting  RUN_ID={RUN_ID}")
    dbg("=" * 60)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = build_creds(SHEET_CREDS_JSON, scopes)
    rl = RateLimitedSheets(gspread.authorize(creds))
    sh = rl.open_by_key(SPREADSHEET_ID)
    lock_ws = rl.worksheet(sh, "LockQueue")
    settings = load_settings(rl, sh)
    accounts = load_account_map(rl, sh)

    mega_source_name = settings.get("mega_source_folder", "Source")
    mega_claimed_name = settings.get("mega_claimed_folder", "Claimed")
    thread_delimiter = settings.get("thread_delimiter", "---")

    # mega login once (credentials from first account that has them, else secrets)
    mega_email = MEGA_EMAIL_FALLBACK
    mega_password = MEGA_PASSWORD_FALLBACK
    for acc in accounts.values():
        if acc.get("mega_email") and acc.get("mega_password"):
            mega_email = acc["mega_email"]
            mega_password = acc["mega_password"]
            break
    m = None
    source_handle = claimed_handle = None
    try:
        m = mega_login(mega_email, mega_password)
        source_handle = mega_find_folder(m, mega_source_name)
        claimed_handle = mega_find_folder(m, mega_claimed_name)
        dbg(f"mega source={mega_source_name} claimed={mega_claimed_name}")
    except Exception as e:
        dbg(f"⚠ mega login/folder setup failed: {e} — image/video jobs may fail")

    consecutive_session_failures = 0
    posts_done = 0
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        # We may switch storage state per account — create context per job
        page = None
        current_account_id = None
        context = None

        while time.time() < run_deadline:
            job = claim_next_job(rl, lock_ws)
            if job is None:
                dbg("No more available jobs for this RUN_ID — worker exiting.")
                break

            posts_done += 1
            kind = job.get("kind", "").lower()
            account_id = job.get("account_id", "default")
            dbg(f"{'='*50}")
            dbg(f"POST #{posts_done} kind={kind} account={account_id} lock={job.get('lock_id')}")
            dbg(f"{'='*50}")

            # Switch browser context if account changed
            if account_id != current_account_id or context is None:
                if context:
                    context.close()
                acc_row = accounts.get(account_id, {})
                state_path = f"x_state_w{WORKER_ID}_{account_id}.json"
                try:
                    write_storage_state(acc_row, state_path)
                except Exception as e:
                    dbg(f"Storage state error: {e}")
                    mark_job(rl, lock_ws, job, "FAILED", error=str(e))
                    continue
                context = browser.new_context(
                    storage_state=state_path,
                    viewport={"width": 1280, "height": 900},
                    user_agent=(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                    ),
                )
                context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
                )
                page = context.new_page()
                current_account_id = account_id

            media_path = None
            tmp_to_clean = None
            tweets: list[str] = []
            file_handle = None
            file_name = job.get("file_name", "")

            try:
                if kind in ("image", "video"):
                    caption = (job.get("caption_text") or "").strip()
                    if not caption and CAPTION_SOURCE == "custom":
                        caption = f"Post {job.get('lock_id')}"
                    tweets = [caption] if caption else [file_name or " "]
                    if not m or source_handle is None:
                        raise RuntimeError("mega not available")
                    # If no pre-assigned file, pick one live from folder
                    if not file_name:
                        listing = mega_list_in_folder(m, source_handle, kind)
                        if not listing:
                            raise RuntimeError(f"No {kind} files left in mega source folder")
                        pick = random.choice(listing)
                        file_name = pick["name"]
                        file_handle = pick["handle"]
                        # update lock row with chosen name
                        try:
                            headers = [h.strip() for h in rl.get_all_values(lock_ws)[0]]
                            col = _header_index(headers, "file_name") + 1
                            rl.update_cell(lock_ws, job["_row_number"], col, file_name)
                        except Exception:
                            pass
                    dest_dir = tempfile.mkdtemp(prefix="mega_")
                    media_path = mega_download_by_name(m, source_handle, file_name, dest_dir)
                    tmp_to_clean = media_path
                    # resolve handle for later move
                    if file_handle is None:
                        listing = mega_list_in_folder(m, source_handle, kind)
                        for f in listing:
                            if f["name"] == file_name:
                                file_handle = f["handle"]
                                break
                else:  # thread
                    text = job.get("caption_text") or ""
                    if not text and job.get("thread_row"):
                        # re-read from Threads if needed
                        try:
                            thr = rl.worksheet(sh, "Threads")
                            vals = rl.get_all_values(thr)
                            ridx = int(job["thread_row"]) - 1
                            if 0 <= ridx < len(vals):
                                text = " ".join(c for c in vals[ridx] if c).strip()
                        except Exception as e:
                            dbg(f"thread re-read: {e}")
                    tweets = split_into_tweets(text, thread_delimiter)

                for j, t in enumerate(tweets, 1):
                    dbg(f" Tweet {j}: {t[:80]}{'…' if len(t) > 80 else ''}")

                payload = {
                    "kind": kind,
                    "tweets": tweets,
                    "media_path": media_path,
                }
                posted, session_err, tweet_id = post_one_job(page, payload, post_index=posts_done)

                if posted:
                    mark_job(rl, lock_ws, job, "SUCCESS", tweet_id=tweet_id or "")
                    results.append((posts_done, kind, "SUCCESS"))
                    consecutive_session_failures = 0
                    if kind in ("image", "video") and m and file_handle and claimed_handle:
                        mega_move_to_claimed(m, file_handle, claimed_handle, file_name)
                    # optional: delete thread row
                    if kind == "thread" and job.get("thread_row"):
                        try:
                            thr = rl.worksheet(sh, "Threads")
                            rl.delete_rows(thr, int(job["thread_row"]))
                        except Exception as e:
                            dbg(f"thread row delete: {e}")
                elif session_err:
                    consecutive_session_failures += 1
                    mark_job(rl, lock_ws, job, "FAILED", error="session")
                    results.append((posts_done, kind, "FAILED (session)"))
                    if consecutive_session_failures >= MAX_CONSECUTIVE_SESSION_FAILURES:
                        dbg("Too many consecutive session failures — stopping worker")
                        break
                else:
                    mark_job(rl, lock_ws, job, "FAILED", error="post failed after retries")
                    results.append((posts_done, kind, "FAILED"))
                    consecutive_session_failures = 0

            except Exception as e:
                dbg(f"EXCEPTION: {e}")
                mark_job(rl, lock_ws, job, "FAILED", error=str(e)[:500])
                results.append((posts_done, kind, f"EXCEPTION: {e}"))
            finally:
                if tmp_to_clean:
                    try:
                        Path(tmp_to_clean).unlink(missing_ok=True)
                        Path(tmp_to_clean).parent.rmdir()
                    except Exception:
                        pass

            if time.time() + INTERVAL_SECONDS >= run_deadline:
                dbg("Next sleep would exceed budget — stopping")
                break
            dbg(f"Sleeping {INTERVAL_SECONDS}s before next claim…")
            time.sleep(INTERVAL_SECONDS)

        if context:
            context.close()
        browser.close()

    dbg("")
    dbg("=" * 60)
    dbg(f"Worker {WORKER_ID} complete. Summary:")
    for idx, kind, status in results:
        dbg(f"  Post {idx:>2} [{kind:<6}]: {status}")
    dbg(f"Total run time: {(time.time() - run_start) / 60:.1f} min")
    dbg("=" * 60)


if __name__ == "__main__":
    main()
PYEOF
echo "post_mix_worker.py written"
