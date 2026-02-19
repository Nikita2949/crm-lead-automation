# -*- coding: utf-8 -*-
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
# НАСТРОЙКИ
# =============================
SCROLL_PAUSE = 1.2
MAX_SCROLL_ERRORS = 5

OUTPUT_DIR = Path("yandex_result")
OUTPUT_DIR.mkdir(exist_ok=True)

# =============================
# ТРАНСЛИТЕРАЦИЯ
# =============================
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
    return "".join(TRANSLIT_MAP.get(ch, ch) for ch in text.lower())

# =============================
# DRIVER
# =============================
def create_driver(headless=False) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

# =============================
# SCROLL + LINKS
# =============================
def collect_links(driver: webdriver.Chrome) -> List[str]:
    links = set()
    errors = 0

    try:
        slider = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "scroll__scrollbar-thumb"))
        )
    except TimeoutException:
        print("❌ scrollbar не найден")
        return []

    actions = ActionChains(driver)

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
# ФИЛЬТР ПО АДРЕСУ
# =============================
def address_matches(address: str, city: str) -> bool:
    return re.search(rf"\b{re.escape(city.lower())}\b", address.lower()) is not None

# =============================
# ЧАСЫ РАБОТЫ
# =============================
def parse_working_hours(soup: BeautifulSoup):
    status = soup.select_one("div.business-working-status-view")
    if status and "круглосуточ" in status.get_text(strip=True).lower():
        return "Круглосуточно"

    metas = soup.select("meta[itemprop='openingHours']")
    if not metas:
        return None

    entries = [m.get("content") for m in metas if m.get("content")]
    return normalize_hours(entries)

def normalize_hours(entries: List[str]) -> str | None:
    day_map = {
        "Mo": "Пн", "Tu": "Вт", "We": "Ср",
        "Th": "Чт", "Fr": "Пт", "Sa": "Сб", "Su": "Вс"
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
# PARSE CARDS
# =============================
def parse_cards(driver, links, city: str, category: str, filter_address: bool) -> List[dict]:
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

        if filter_address and address_text and not address_matches(address_text, city):
            print(f"⛔ Пропущено: {address_text}")
            continue

        rows.append({
            "Категория": category,
            "Населённый пункт": city,
            "Название": name_el.get_text(strip=True) if name_el else None,
            "Адрес": address_text,
            "Телефон": phone_el.get_text(strip=True) if phone_el else None,
            "Сайт": site_el.get_text(strip=True) if site_el else None,
            "Часы работы": parse_working_hours(soup),
            "URL": url
        })

    return rows

# =============================
# MAIN
# =============================
def main():
    raw_categories = input("Введите категории через запятую: ").strip()
    categories = [c.strip() for c in raw_categories.split(",") if c.strip()]

    raw_cities = input("Введите города через запятую: ").strip()
    cities = [c.strip() for c in raw_cities.split(",") if c.strip()]


    filter_input = input("Включить фильтрацию по населенному пункту? (Да/Нет): ").strip().lower()
    filter_address = filter_input.startswith("д")  # да/Да/ДА → True

    multiple = len(cities) > 1
    all_rows = []

    driver = create_driver(headless=False)

    try:
        for category in categories:
            for city in cities:
                print("===================================")
                print(f"🏙 Город: {city} | 📂 Категория: {category}")

                query = f"{category} {city}"

                url = f"https://yandex.ru/maps/?text={urllib.parse.quote(query)}"
                driver.get(url)
                time.sleep(6)

                links = collect_links(driver)
                print(f"🔗 Найдено ссылок: {len(links)}")

                if links:
                    rows = parse_cards(driver, links, city, category, filter_address)
                    all_rows.extend(rows)
                else:
                    print("⚠️ Ничего не найдено")

    finally:
        driver.quit()

    if not all_rows:
        print("❌ Нет данных для сохранения")
        return

    date = datetime.now().strftime("%d.%m.%Y")

    # Формат имени файла
    if len(cities) == 1:
        filename = f"{transliterate(cities[0])}_{date}.xlsx"
    elif 2 <= len(cities) <= 3:
        prefix = "_".join(transliterate(c[:2]).capitalize() for c in cities)
        filename = f"{prefix}_{date}.xlsx"
    else:
        filename = f"Yandex_maps_result_{date}.xlsx"

    out = OUTPUT_DIR / filename
    pd.DataFrame(all_rows).to_excel(out, index=False)
    print(f"✅ Сохранено: {len(all_rows)} → {out}")

if __name__ == "__main__":
    main()
