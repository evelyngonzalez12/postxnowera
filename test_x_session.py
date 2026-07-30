#!/usr/bin/env python3
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

SCREENSHOT_DIR = Path("debug_screenshots")
SCREENSHOT_DIR.mkdir(exist_ok=True)

def main():
    print("Starting session test (no posts)...")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        context = browser.new_context(
            storage_state="x_storage_state.json",
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(3000)
        print("URL after /home:", page.url)
        page.screenshot(path=str(SCREENSHOT_DIR / "01_home.png"))

        if "login" in page.url or "signin" in page.url:
            print("RESULT: LOGGED OUT / session invalid")
            browser.close()
            raise SystemExit(1)

        page.goto("https://x.com/compose/post", wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(3000)
        print("URL after compose:", page.url)
        page.screenshot(path=str(SCREENSHOT_DIR / "02_compose.png"))

        box = page.locator('[data-testid="tweetTextarea_0"]').first
        try:
            box.wait_for(state="visible", timeout=15_000)
            print("RESULT: LOGGED IN — compose textbox visible")
        except Exception:
            print("RESULT: compose textbox NOT found")
            browser.close()
            raise SystemExit(1)

        browser.close()
        print("Session test finished (no post made).")

if __name__ == "__main__":
    main()
