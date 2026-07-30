#!/usr/bin/env python3
"""
post_mix_worker.py
- Reads job_plan.json (jobs assigned to this worker_id)
- Media via rclone (remote "mega") — no mega_node_id
- Posts image / video / thread / link-preview to X
- No LockQueue sheet; account locks released by cleanup job
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

from playwright.sync_api import sync_playwright

WORKER_ID = os.environ.get("WORKER_ID", "0")
RUN_ID = os.environ.get("RUN_ID", "local")
CAPTION_SOURCE = os.environ.get("CAPTION_SOURCE", "filename").strip().lower()
INTERVAL_MINUTES = float(os.environ.get("INTERVAL_MINUTES", "10"))
INTERVAL_SECONDS = int(INTERVAL_MINUTES * 60)
RUN_BUDGET_MINUTES = float(os.environ.get("RUN_BUDGET_MINUTES", "355"))
RCLONE_REMOTE = os.environ.get("RCLONE_REMOTE", "mega")

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
        dbg(f" snapshot {path}")
    except Exception as e:
        dbg(f" snapshot failed ({label}): {e}")


# ── rclone helpers ───────────────────────────────────────────────────────────
def rclone(*args, check=True) -> subprocess.CompletedProcess:
    cmd = ["rclone", "--retries", "3", "--low-level-retries", "5", *args]
    dbg(f" rclone {' '.join(args[:6])}...")
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def rclone_list_files(folder: str, kind: str) -> list[str]:
    """List file names in mega:folder (non-recursive)."""
    remote = f"{RCLONE_REMOTE}:{folder}"
    try:
        r = rclone("lsf", remote, "--files-only", check=False)
    except Exception as e:
        dbg(f"rclone lsf error: {e}")
        return []
    if r.returncode != 0:
        dbg(f"rclone lsf stderr: {r.stderr[:300]}")
        return []
    names = [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]
    out = []
    for n in names:
        ext = Path(n).suffix.lower()
        if kind == "image" and ext in IMAGE_EXTS:
            out.append(n)
        elif kind == "video" and ext in VIDEO_EXTS:
            out.append(n)
    return out


def rclone_download(folder: str, file_name: str, dest_dir: str) -> str:
    remote = f"{RCLONE_REMOTE}:{folder}/{file_name}"
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    local = dest / file_name
    r = rclone("copyto", remote, str(local), check=False)
    if r.returncode != 0 or not local.exists():
        raise RuntimeError(f"rclone download failed: {r.stderr[:400]}")
    return str(local)


def rclone_move_to_claimed(source_folder: str, claimed_folder: str, file_name: str) -> None:
    src = f"{RCLONE_REMOTE}:{source_folder}/{file_name}"
    dst = f"{RCLONE_REMOTE}:{claimed_folder}/{file_name}"
    r = rclone("moveto", src, dst, check=False)
    if r.returncode != 0:
        dbg(f" rclone move warning: {r.stderr[:300]}")
    else:
        dbg(f" Moved {file_name} -> {claimed_folder}")


# ── Playwright / X ───────────────────────────────────────────────────────────
TEXTBOX_SELECTORS = [
    '[data-testid="tweetTextarea_0"]',
    '[data-testid="tweetTextarea_0EditorContainer"] div[contenteditable="true"]',
    'div[contenteditable="true"][data-testid]',
    'div[contenteditable="true"]',
    '[placeholder="What is happening?!"]',
]
POST_BUTTON_SELECTORS = [
    '[data-testid="tweetButton"]', '[data-testid="tweetButtonInline"]',
    'button[data-testid*="tweet"]', 'button:has-text("Post")',
]
ADD_THREAD_TWEET_SELECTORS = ['[data-testid="addButton"]', 'div[aria-label="Add post"]']
PREVIEW_SELECTORS = [
    '[data-testid="attachments"] video', '[data-testid="videoComponent"]',
    '[data-testid="tweetPhoto"]', '[data-testid="attachments"] img',
    '[data-testid="attachments"]', 'img[src*="blob:"]', 'video[src*="blob:"]',
]


def find_element_multi(page, selectors, label, timeout=15000):
    for sel in selectors:
        try:
            el = page.locator(sel).first
            el.wait_for(state="visible", timeout=timeout // max(len(selectors), 1))
            dbg(f" Found {label}: {sel}")
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
        page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(2500)
        current = page.url
        if "login" in current or "signin" in current or "graduated-access" in current:
            return True
        try:
            page.locator(
                '[data-testid="AppTabBar_Home_Link"], [data-testid="SideNav_NewTweet_Button"]'
            ).first.wait_for(state="visible", timeout=8000)
            return False
        except Exception:
            return True
    except Exception:
        return True


def navigate_to_compose(page, post_index, attempt=1):
    for url in ["https://x.com/compose/post", "https://twitter.com/compose/tweet"]:
        dbg(f" [NAV] attempt {attempt} -> {url}")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)
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
        page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)
        btn = page.locator(
            'a[href="/compose/post"], [data-testid="SideNav_NewTweet_Button"], [aria-label="Post"]'
        ).first
        btn.wait_for(state="visible", timeout=10000)
        btn.click()
        page.wait_for_timeout(3000)
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
            page.wait_for_selector(sel, timeout=timeout // max(len(PREVIEW_SELECTORS), 1))
            try:
                page.wait_for_selector('[role="progressbar"]', state="detached", timeout=timeout)
            except Exception:
                pass
            page.wait_for_timeout(1200)
            screenshot(page, f"p{post_index}_preview_{label}")
            return True
        except Exception:
            continue
    return False


def attach_media_robust(page, media_path, is_video, post_index):
    upload_timeout = 120000 if is_video else 45000
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
    for btn_sel in ['[data-testid="addMedia"]', '[aria-label*="edia"]']:
        try:
            btn = page.locator(btn_sel).first
            if btn.is_visible():
                btn.click()
                page.wait_for_timeout(1000)
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
    btn = find_element_multi(page, ADD_THREAD_TWEET_SELECTORS, "add-thread", timeout=10000)
    if btn is None:
        raise RuntimeError("Could not find Add post button")
    btn.click()
    page.wait_for_timeout(800)
    locator = page.locator(f'[data-testid="tweetTextarea_{tweet_index}"]').first
    try:
        locator.wait_for(state="visible", timeout=10000)
        return locator
    except Exception:
        all_ce = page.locator('div[contenteditable="true"][data-testid]')
        return all_ce.nth(all_ce.count() - 1)


def click_post_button(page, post_index):
    post_btn = find_element_multi(page, POST_BUTTON_SELECTORS, "post button", timeout=15000)
    if post_btn is None:
        raise RuntimeError("Post button not found")
    if post_btn.is_disabled():
        for _ in range(3):
            page.wait_for_timeout(5000)
            if not post_btn.is_disabled():
                break
    try:
        post_btn.click()
    except Exception:
        page.evaluate(
            """(sel) => { for (const s of sel) { const el = document.querySelector(s); if (el) { el.click(); return s; } } return null; }""",
            POST_BUTTON_SELECTORS,
        )


def post_with_network_confirmation(page, post_index, click_timeout=25000):
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
        page.wait_for_timeout(1500)
    finally:
        page.remove_listener("response", on_response)
    screenshot(page, f"p{post_index}_after_click")
    if result["sent"]:
        return True, result["tweet_id"]
    return False, None


def post_one_job(page, job_payload, post_index, max_attempts=3):
    session_error = False
    for attempt in range(1, max_attempts + 1):
        dbg(f" --- [{job_payload['kind'].upper()}] attempt {attempt}/{max_attempts} ---")
        try:
            navigate_to_compose(page, post_index, attempt)
            first_box = find_element_multi(page, TEXTBOX_SELECTORS, "textbox", timeout=20000)
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
            # link-preview: wait for social card or enabled Post button
            if job_payload["kind"] == "link":
                card_sels = [
                    '[data-testid="card.wrapper"]',
                    '[data-testid="card.layoutLarge.media"]',
                    '[data-testid="card.layoutSmall.media"]',
                    'div[data-testid="card.wrapper"]',
                    'a[href][rel="noopener noreferrer"]',
                ]
                card_ok = False
                for sel in card_sels:
                    try:
                        page.wait_for_selector(sel, timeout=8_000)
                        card_ok = True
                        dbg(f" Link card appeared via {sel}")
                        break
                    except Exception:
                        continue
                if not card_ok:
                    dbg(" Link card not detected — waiting extra 5s before post")
                    page.wait_for_timeout(5_000)
                else:
                    page.wait_for_timeout(1_500)
                screenshot(page, f"p{post_index}_link_card")
            sent, tweet_id = post_with_network_confirmation(page, post_index)
            if sent:
                dbg(f" SUCCESS tweet_id={tweet_id}")
                return True, False, tweet_id
            if attempt < max_attempts:
                page.wait_for_timeout(5000)
        except RuntimeError as e:
            dbg(f" RuntimeError: {e}")
            if any(k in str(e).lower() for k in SESSION_ERROR_KEYWORDS):
                session_error = True
                if attempt < max_attempts:
                    page.wait_for_timeout(15000)
                    continue
                return False, True, None
            if attempt < max_attempts:
                page.wait_for_timeout(10000)
            else:
                return False, False, None
        except Exception as e:
            dbg(f" Unexpected: {e}")
            if attempt < max_attempts:
                page.wait_for_timeout(10000)
            else:
                return False, False, None
    return False, session_error, None


def split_into_tweets(text: str, delimiter: str) -> list[str]:
    parts = [p.strip() for p in text.split(delimiter)]
    parts = [p for p in parts if p]
    return parts if parts else [text.strip()]


def write_storage_state(state_json: str, path: str) -> str:
    import json as _json
    raw = state_json
    if raw is None:
        raise RuntimeError("Empty storage_state_json")
    if isinstance(raw, dict):
        data = raw
    else:
        raw = str(raw).strip()
        if not raw:
            raise RuntimeError("Empty storage_state_json")
        try:
            data = _json.loads(raw)
        except _json.JSONDecodeError as e:
            raise RuntimeError(f"storage_state_json is not valid JSON: {e}") from e
    cookies = data.get("cookies") or []
    names = {c.get("name") for c in cookies if isinstance(c, dict)}
    if "auth_token" not in names and "auth_token" not in str(cookies):
        dbg("WARNING: storage_state has no auth_token cookie — may not be logged in")
    Path(path).write_text(_json.dumps(data), encoding="utf-8")
    return path


def main():
    run_start = time.time()
    run_deadline = run_start + RUN_BUDGET_MINUTES * 60
    dbg("=" * 60)
    dbg(f"Worker {WORKER_ID} starting RUN_ID={RUN_ID}")
    dbg("=" * 60)

    plan_path = Path("job_plan.json")
    if not plan_path.exists():
        sys.exit("job_plan.json missing — download-artifact step failed?")
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    settings = plan.get("settings") or {}
    mega_source = settings.get("mega_source_folder", "Source")
    mega_claimed = settings.get("mega_claimed_folder", "Claimed")
    thread_delimiter = settings.get("thread_delimiter", "---")

    my_jobs = (plan.get("workers") or {}).get(str(WORKER_ID)) or []
    dbg(f"Assigned {len(my_jobs)} job(s)")
    if not my_jobs:
        dbg("Nothing to do")
        return

    consecutive_session_failures = 0
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-blink-features=AutomationControlled"],
        )
        context = None
        page = None
        current_account_id = None

        for i, job in enumerate(my_jobs, start=1):
            if time.time() >= run_deadline:
                dbg("Budget exhausted — stopping")
                break

            kind = job.get("kind", "").lower()
            account_id = job.get("account_id", "default")
            dbg(f"{'=' * 50}")
            dbg(f"POST {i}/{len(my_jobs)} kind={kind} account={account_id}")
            dbg(f"{'=' * 50}")

            if account_id != current_account_id or context is None:
                if context:
                    context.close()
                state_path = f"x_state_w{WORKER_ID}_{account_id}.json"
                # Prefer per-job state, else look up from plan accounts list
                state_raw = job.get("storage_state_json") or ""
                if not state_raw:
                    for acc in (plan.get("accounts") or []):
                        if str(acc.get("account_id")) == str(account_id):
                            state_raw = acc.get("storage_state_json") or ""
                            break
                try:
                    write_storage_state(state_raw, state_path)
                except Exception as e:
                    dbg(f"Storage state error: {e}")
                    results.append((i, kind, f"FAILED: {e}"))
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
                # Warm session: open home and confirm we are logged in
                try:
                    page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=30_000)
                    page.wait_for_timeout(3_000)
                    cur = page.url
                    dbg(f" Session check URL: {cur}")
                    screenshot(page, f"session_check_{account_id}")
                    if "login" in cur or "i/flow" in cur:
                        raise RuntimeError(
                            f"Not logged in after loading storage_state (url={cur}). "
                            "Re-export storage_state_json while logged into x.com."
                        )
                except RuntimeError:
                    raise
                except Exception as e:
                    dbg(f" Session warm-up warning: {e}")

            media_path = None
            tmp_dir = None
            tweets: list[str] = []
            file_name = job.get("file_name") or ""

            try:
                if kind in ("image", "video"):
                    caption = (job.get("caption_text") or "").strip() or file_name or " "
                    tweets = [caption]
                    if not file_name:
                        listing = rclone_list_files(mega_source, kind)
                        if not listing:
                            raise RuntimeError(f"No {kind} files in mega:{mega_source}")
                        file_name = random.choice(listing)
                        job["file_name"] = file_name
                        dbg(f"Picked live file: {file_name}")
                    tmp_dir = tempfile.mkdtemp(prefix="rclone_")
                    media_path = rclone_download(mega_source, file_name, tmp_dir)

                elif kind == "thread":
                    text = job.get("text") or job.get("caption_text") or ""
                    tweets = split_into_tweets(text, thread_delimiter)

                elif kind == "link":
                    # caption + optional hashtags + URL (X generates social card)
                    text = (job.get("caption_text") or "").strip()
                    if not text:
                        parts = []
                        if (job.get("caption") or "").strip():
                            parts.append(job["caption"].strip())
                        if (job.get("hashtags") or "").strip():
                            parts.append(job["hashtags"].strip())
                        url = (job.get("url") or "").strip()
                        if url:
                            parts.append(url)
                        text = "\n\n".join(parts)
                    if not text.strip():
                        raise RuntimeError("Link job has empty url/caption — nothing to post")
                    # Ensure URL is present for card generation
                    url = (job.get("url") or "").strip()
                    if url and url not in text:
                        text = text.rstrip() + "\n\n" + url
                    tweets = [text]
                    dbg(f" Link post text length={len(text)} url={url[:60]}")

                else:
                    raise RuntimeError(f"Unknown kind: {kind}")

                for j, t in enumerate(tweets, 1):
                    dbg(f" Tweet {j}: {t[:90]}{'...' if len(t) > 90 else ''}")

                payload = {"kind": kind, "tweets": tweets, "media_path": media_path}
                posted, session_err, tweet_id = post_one_job(page, payload, post_index=i)

                if posted:
                    results.append((i, kind, "SUCCESS"))
                    consecutive_session_failures = 0
                    if kind in ("image", "video") and file_name:
                        rclone_move_to_claimed(mega_source, mega_claimed, file_name)
                elif session_err:
                    consecutive_session_failures += 1
                    results.append((i, kind, "FAILED (session)"))
                    if consecutive_session_failures >= MAX_CONSECUTIVE_SESSION_FAILURES:
                        dbg("Too many session failures — stopping worker")
                        break
                else:
                    results.append((i, kind, "FAILED"))
                    consecutive_session_failures = 0

            except Exception as e:
                dbg(f"EXCEPTION: {e}")
                results.append((i, kind, f"EXCEPTION: {e}"))
            finally:
                if tmp_dir:
                    try:
                        for f in Path(tmp_dir).iterdir():
                            f.unlink(missing_ok=True)
                        Path(tmp_dir).rmdir()
                    except Exception:
                        pass

            if i < len(my_jobs):
                if time.time() + INTERVAL_SECONDS >= run_deadline:
                    dbg("Next sleep would exceed budget — stopping")
                    break
                dbg(f"Sleeping {INTERVAL_SECONDS}s...")
                time.sleep(INTERVAL_SECONDS)

        if context:
            context.close()
        browser.close()

    dbg("")
    dbg("=" * 60)
    dbg(f"Worker {WORKER_ID} complete:")
    for idx, kind, status in results:
        dbg(f"  Post {idx:>2} [{kind:<6}]: {status}")
    dbg(f"Runtime: {(time.time() - run_start) / 60:.1f} min")
    dbg("=" * 60)


if __name__ == "__main__":
    main()
