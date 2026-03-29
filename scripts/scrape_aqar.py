#!/usr/bin/env python3
"""Minimal Aqar.fm crawler: fetch area pages and extract neighborhood links + counts."""

import argparse
import random
import time

import requests
from bs4 import BeautifulSoup

AREAS = [
    "north-of-riyadh",
    "south-of-riyadh",
    "east-of-riyadh",
    "west-of-riyadh",
    "center-of-riyadh",
]

BASE = "https://sa.aqar.fm/en/store-for-rent/riyadh"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
]


def fetch_area(area: str) -> list[dict]:
    url = f"{BASE}/{area}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    results = []
    for link in soup.select("a[href]"):
        href = link.get("href", "")
        if "/en/store-for-rent/riyadh/" not in href or href.endswith(f"/{area}"):
            continue
        text = link.get_text(strip=True)
        # Extract count from text like "Al Olaya (42)"
        count = 0
        if "(" in text and text.endswith(")"):
            parts = text.rsplit("(", 1)
            text = parts[0].strip()
            count = int(parts[1].rstrip(")").replace(",", "") or 0)
        if text:
            full_url = href if href.startswith("http") else f"https://sa.aqar.fm{href}"
            results.append({"neighborhood": text, "url": full_url, "count": count})
    return results


def main():
    parser = argparse.ArgumentParser(description="Crawl Aqar.fm Riyadh store-for-rent listings")
    parser.add_argument("--area", choices=AREAS, help="Limit to a single area")
    args = parser.parse_args()
    areas = [args.area] if args.area else AREAS
    for i, area in enumerate(areas):
        if i > 0:
            time.sleep(2)
        print(f"\n=== {area} ===")
        for row in fetch_area(area):
            print(f"  {row['neighborhood']:30s}  {row['count']:>5d}  {row['url']}")


if __name__ == "__main__":
    main()
