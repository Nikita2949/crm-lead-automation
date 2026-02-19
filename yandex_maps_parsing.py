# -*- coding: utf-8 -*-

"""
CRM Lead Scraper

This script collects business listings from a map-based web platform,
parses structured data (name, address, phone, website, working hours),
and exports the results to an Excel file.

Features:
- Dynamic scrolling
- Anti-blocking delays
- Address filtering by city
- Working hours normalization
- Automatic file naming with transliteration
"""

import time
import random
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import List

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import re


# =============================
# CONFIGURATION
# =============================

# Delay between scroll actions
SCROLL_PAUSE = 1.2

# Maximum consecutive scroll attempts without new results
MAX_SCROLL_ERRORS = 5

# Output directory for Excel files
OUTPUT_DIR = Path("yandex_result")
OUTPUT_DIR.mkdir(exist_ok=True)


# =============================
# TRANSLITERATION
# =============================

# Russian → Latin transliteration map
TRANSLIT_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
    "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i",
    "й": "y", "к": "k", "л": "l", "м": "m", "н": "n",
    "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "h", "ц": "ts", "ч": "ch",
    "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "",
    "э": "e", "ю": "yu", "я": "ya"
}


def transliterate(text: str) -> str:
    """
    Converts Cyrillic text into Latin characters
    for safe file naming.
    """
    return "".join(TRANSLIT_MAP.get(ch, ch) for ch in text.lower())


# =============================
# DRIVER SETUP
# =============================

def create_driver(headless=False) -> webdriver.Chrome:
    """
    Creates and configures a Chrome WebDriver instance.

    Parameters:
        headless (bool): Run browser in headless mode if True.

    Returns:
        webdriver.Chrome
    """
    options = Options()

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service(ChromeDriverManager().install())

    return webdriver.Chrome(service=service, options=options)


# =============================
# SCROLL AND LINK COLLECTION
# =============================

def collect_links(driver: webdriver.Chrome) -> List[str]:
    """
    Scrolls through the results panel and collects
    unique organization links.

    Returns:
        List[str]: List of unique business URLs
    """
    links = set()
    errors = 0

    # Wait for the scrollable panel to appear
    try:
        slider = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "scroll__scrollbar-thumb"))
        )
    except TimeoutException:
        print("❌ Scrollbar not found")
        return []

    actions = ActionChains(driver)

    # Scroll until no new links are found
    while errors < MAX_SCROLL_ERRORS:
        cards = driver.find_elements(By.CSS_SELECTOR, "a.link-overlay[href*='/org/']")
        new = 0

        for c in cards:
            try:
                href = c.get_attribute("href")
            except StaleElementReferenceException:
                continue

            if not href:
                continue

            clean = href.split("?")[0]

            if clean not in links:
                links.add(clean)
                new += 1

        errors = errors + 1 if new == 0 else 0

        try:
            actions.click_and_hold(slider).move_by_offset(0, 160).release().perform()
        except Exception:
            pass

        time.sleep(SCROLL_PAUSE + random.uniform(0, 0.4))

    return list(links)


# =============================
# ADDRESS FILTERING
# =============================

def address_matches(address: str, city: str) -> bool:
    """
    Checks whether the city name is present in the address.
    """
    return re.search(rf"\b{re.escape(city.lower())}\b", address.lower()) is not None


# =============================
# WORKING HOURS PARSING
# =============================

def parse_working_hours(soup: BeautifulSoup):
    """
    Extracts and normalizes working hours from the page.
    """
    status = soup.select_one("div.business-working-status-view")

    # Check for 24/7 operation
    if status and "круглосуточ" in status.get_text(strip=True).lower():
        return "24/7"

    metas = soup.select("meta[itemprop='openingHours']")
    if not metas:
        return None

    entries = [m.get("content") for m in metas if m.get("content")]
    return normalize_hours(entries)


def normalize_hours(entries: List[str]) -> str | None:
    """
    Converts structured opening hours into a readable format.
    """
    day_map = {
        "Mo": "Mon", "Tu": "Tue", "We": "Wed",
        "Th": "Thu", "Fr": "Fri", "Sa": "Sat", "Su": "Sun"
    }

    parsed = []

    for e in entries:
        parts = e.split()
        if len(parts) == 2:
            day, hours = parts
            parsed.append((day_map.get(day, day), hours))

    if not parsed:
        return None

    hours_set = {h for _, h in parsed}

    if len(hours_set) == 1:
        return f"{parsed[0][0]}–{parsed[-1][0]} {parsed[0][1]}"

    return "; ".join(f"{d} {h}" for d, h in parsed)


# =============================
# CARD PARSING
# =============================

def parse_cards(driver, links, city: str, category: str, filter_address: bool) -> List[dict]:
    """
    Visits each collected URL and extracts business data.
    """
    rows = []

    for i, url in enumerate(links, 1):
        print(f"[{i}/{len(links)}] {url}")

        driver.get(url)
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, "lxml")

        name_el = soup.select_one("h1")
        addr_el = soup.select_one("div.business-contacts-view__address a")
        phone_el = soup.select_one("div.orgpage-phones-view__phone-number")
        site_el = soup.select_one("a.business-urls-view__link")

        address_text = addr_el.get_text(strip=True) if addr_el else None

        # Optional city-based filtering
        if filter_address and address_text and not address_matches(address_text, city):
            print(f"⛔ Skipped (outside city): {address_text}")
            continue

        rows.append({
            "Category": category,
            "City": city,
            "Name": name_el.get_text(strip=True) if name_el else None,
            "Address": address_text,
            "Phone": phone_el.get_text(strip=True) if phone_el else None,
            "Website": site_el.get_text(strip=True) if site_el else None,
            "Working Hours": parse_working_hours(soup),
            "URL": url
        })

    return rows


# =============================
# MAIN EXECUTION
# =============================

def main():
    """
    Main workflow:
    - Collect user input
    - Iterate over categories and cities
    - Scrape data
    - Export results to Excel
    """

    raw_categories = input("Enter categories separated by comma: ").strip()
    categories = [c.strip() for c in raw_categories.split(",") if c.strip()]

    raw_cities = input("Enter cities separated by comma: ").strip()
    cities = [c.strip() for c in raw_cities.split(",") if c.strip()]

    filter_input = input("Enable city-based filtering? (Yes/No): ").strip().lower()
    filter_address = filter_input.startswith("y")

    all_rows = []

    driver = create_driver(headless=False)

    try:
        for category in categories:
            for city in cities:

                print("===================================")
                print(f"City: {city} | Category: {category}")

                query = f"{category} {city}"
                url = f"https://yandex.ru/maps/?text={urllib.parse.quote(query)}"

                driver.get(url)
                time.sleep(6)

                links = collect_links(driver)
                print(f"Collected links: {len(links)}")

                if links:
                    rows = parse_cards(driver, links, city, category, filter_address)
                    all_rows.extend(rows)
                else:
                    print("No results found")

    finally:
        driver.quit()

    if not all_rows:
        print("No data collected")
        return

    date = datetime.now().strftime("%d.%m.%Y")

    # Dynamic file naming
    if len(cities) == 1:
        filename = f"{transliterate(cities[0])}_{date}.xlsx"
    else:
        filename = f"map_results_{date}.xlsx"

    out = OUTPUT_DIR / filename

    pd.DataFrame(all_rows).to_excel(out, index=False)

    print(f"Saved {len(all_rows)} records → {out}")


if __name__ == "__main__":
    main()
