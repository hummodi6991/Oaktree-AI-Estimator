from __future__ import annotations

import httpx
import urllib.robotparser as rp
from urllib.parse import urlparse


def robots_allows(url: str, user_agent: str = "oaktree-estimator") -> bool:
    parts = urlparse(url)
    robots = f"{parts.scheme}://{parts.netloc}/robots.txt"
    try:
        r = httpx.get(robots, timeout=5)
        r.raise_for_status()
    except Exception:
        return True  # if no robots, default allow (still honor ToS)
    p = rp.RobotFileParser()
    p.parse(r.text.splitlines())
    return p.can_fetch(user_agent, url)


def safe_get_json(url: str) -> dict:
    if not robots_allows(url):
        raise RuntimeError(f"robots.txt disallows: {url}")
    r = httpx.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def safe_get_bytes(url: str) -> bytes:
    if not robots_allows(url):
        raise RuntimeError(f"robots.txt disallows: {url}")
    r = httpx.get(url, timeout=30)
    r.raise_for_status()
    return r.content
