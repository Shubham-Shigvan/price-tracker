
import os
import re
import time
from datetime import datetime

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

INPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTCB90G6TS6YWsg7Q80l_Viki8B2JldYXyIH-5RS9VZ9t_H7m-Ns-aiz8dn6uzNlmU8EWl9ewSFwOXm/pub?gid=1627290347&single=true&output=csv"
LATEST_FILE = "latest_prices.csv"
HISTORY_FILE = "price_history.csv"
ERROR_FILE = "error_log.csv"

HEADLESS = True
PAGE_TIMEOUT_MS = 45000
WAIT_AFTER_LOAD_SEC = 3


def clean_price(text):
    if not text:
        return None
    text = str(text).replace(",", "").strip()
    match = re.search(r"(\d+(?:\.\d{1,2})?)", text)
    return float(match.group(1)) if match else None


def calc_discount(mrp, live_price):
    if mrp is None or live_price is None or mrp <= 0:
        return None
    return round(((mrp - live_price) / mrp) * 100, 2)


def safe_inner_text(page, selectors, timeout=3000):
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if locator.count() > 0:
                value = locator.inner_text(timeout=timeout).strip()
                if value:
                    return value
        except Exception:
            continue
    return None


def scrape_amazon(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    time.sleep(WAIT_AFTER_LOAD_SEC)

    live_price_text = safe_inner_text(page, [
        "span.a-price span.a-offscreen",
        "#corePrice_feature_div span.a-price span.a-offscreen",
        ".a-price .a-offscreen",
    ])

    mrp_text = safe_inner_text(page, [
        "span.a-price.a-text-price span.a-offscreen",
        "span[data-a-strike='true'] .a-offscreen",
        ".basisPrice .a-offscreen",
    ])

    return clean_price(live_price_text), clean_price(mrp_text)


def scrape_flipkart(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    time.sleep(WAIT_AFTER_LOAD_SEC)

    live_price_text = safe_inner_text(page, [
        "div.Nx9bqj.CxhGGd",
        "div._30jeq3",
        "div[class*='Nx9bqj']",
    ])

    mrp_text = safe_inner_text(page, [
        "div.yRaY8j.A6+E6v",
        "div._3I9_wc",
        "div[class*='yRaY8j']",
    ])

    return clean_price(live_price_text), clean_price(mrp_text)


def scrape_nykaa(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    time.sleep(WAIT_AFTER_LOAD_SEC)

    live_price_text = safe_inner_text(page, [
        "[data-testid='price-final']",
        "span.css-1jczs19",
        "span[class*='price']",
    ])

    mrp_text = safe_inner_text(page, [
        "[data-testid='price-mrp']",
        "span.css-111z9ua",
        "span[class*='mrp']",
    ])

    return clean_price(live_price_text), clean_price(mrp_text)


def scrape_platform(page, platform, url):
    platform = platform.strip().lower()

    if platform == "amazon":
        return scrape_amazon(page, url)
    if platform == "flipkart":
        return scrape_flipkart(page, url)
    if platform == "nykaa":
        return scrape_nykaa(page, url)

    raise ValueError(f"Unsupported platform: {platform}")


def append_csv(file_path, df, keep_days=None):
    if df.empty:
        return

    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path)
        combined = pd.concat([old_df, df], ignore_index=True)
    else:
        combined = df.copy()

    if keep_days is not None and "timestamp" in combined.columns:
        combined["timestamp"] = pd.to_datetime(combined["timestamp"], errors="coerce")
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=keep_days)
        combined = combined[combined["timestamp"] >= cutoff_date]
        combined = combined.sort_values("timestamp")

    combined.to_csv(file_path, index=False)


def main():


    input_df = pd.read_csv(INPUT_SHEET_URL)

    required_cols = {"sku", "platform", "url", "active"}
    missing = required_cols - set(input_df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    latest_rows = []
    error_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        for _, row in input_df.iterrows():
            active = str(row["active"]).strip().lower()
            if active != "yes":
                continue

            sku = str(row["sku"]).strip()
            platform = str(row["platform"]).strip()
            url = str(row["url"]).strip()

            try:
                live_price, mrp = scrape_platform(page, platform, url)

                latest_rows.append({
                    "sku": sku,
                    "platform": platform,
                    "live_price": live_price,
                    "mrp": mrp,
                    "discount %": calc_discount(mrp, live_price),
                    "timestamp": run_timestamp,
                })

                if live_price is None:
                    error_rows.append({
                        "sku": sku,
                        "platform": platform,
                        "url": url,
                        "error": "Price not found",
                        "timestamp": run_timestamp,
                    })

            except PlaywrightTimeoutError:
                error_rows.append({
                    "sku": sku,
                    "platform": platform,
                    "url": url,
                    "error": "Timeout while loading page",
                    "timestamp": run_timestamp,
                })
            except Exception as exc:
                error_rows.append({
                    "sku": sku,
                    "platform": platform,
                    "url": url,
                    "error": str(exc),
                    "timestamp": run_timestamp,
                })

        browser.close()

    latest_df = pd.DataFrame(latest_rows)
    error_df = pd.DataFrame(error_rows)
    history_df = latest_df.copy()

    if latest_df.empty:
        latest_df = pd.DataFrame(columns=[
            "sku", "platform", "live_price", "mrp", "discount %", "timestamp"
        ])

    if error_df.empty:
        error_df = pd.DataFrame(columns=[
            "sku", "platform", "url", "error", "timestamp"
        ])

latest_df.to_csv(LATEST_FILE, index=False)
append_csv(HISTORY_FILE, history_df, keep_days=60)
append_csv(ERROR_FILE, error_df, keep_days=60)

    print("Done.")
    print(f"Created: {LATEST_FILE}")
    print(f"Updated: {HISTORY_FILE}")
    print(f"Updated: {ERROR_FILE}")


if __name__ == "__main__":
    main()
