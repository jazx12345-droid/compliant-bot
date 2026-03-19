"""
Regulatory monitoring fetcher module.

Fetch strategies:
  rss        - feedparser via requests (handles custom UA requirements)
  html       - requests + BeautifulSoup, site-specific parsers
  playwright - headless Chromium for JS-rendered / WAF-protected pages

Entry point: fetch_all()
"""

import logging
import re
import time
from datetime import datetime
from typing import Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

from db import deduplicate
from sources import SOURCES

logger = logging.getLogger(__name__)

REQUEST_DELAY = 1.5  # seconds between requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _normalize_date(raw: Optional[str]) -> str:
    if not raw or not raw.strip():
        return datetime.utcnow().strftime("%Y-%m-%d")
    try:
        return dateutil_parser.parse(raw, fuzzy=True).strftime("%Y-%m-%d")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d")


def _item(title: str, url: str, date: str, source: dict) -> dict:
    return {
        "title": title.strip(),
        "url": url.strip(),
        "date": _normalize_date(date),
        "source": source["name"],
        "jurisdiction": source["jurisdiction"],
        "tags": source.get("tags", []),
    }


def _get(url: str, **kwargs) -> Optional[requests.Response]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20, **kwargs)
        resp.raise_for_status()
        return resp
    except Exception as exc:
        logger.error("GET %s failed: %s", url, exc)
        return None


def _soup(url: str, **kwargs) -> Optional[BeautifulSoup]:
    resp = _get(url, **kwargs)
    return BeautifulSoup(resp.text, "html.parser") if resp else None


def _abs(href: str, base: str) -> str:
    if href.startswith("http"):
        return href
    return base.rstrip("/") + "/" + href.lstrip("/")


# ── RSS (via requests to handle custom UA requirements) ────────────────────────

def fetch_rss(source: dict) -> list[dict]:
    resp = _get(source["url"])
    if not resp:
        return []
    feed = feedparser.parse(resp.content)
    items = []
    for entry in feed.entries:
        title = entry.get("title", "").strip()
        url = entry.get("link", "").strip()
        date = entry.get("published", entry.get("updated", ""))
        if title and url:
            items.append(_item(title, url, date, source))
    logger.info("[rss] %-42s %d items", source["name"], len(items))
    return items


# ── HTML parsers ───────────────────────────────────────────────────────────────

def _parse_labuan_fsa(source: dict) -> list[dict]:
    """
    Labuan FSA: the index page at /regulations/guidelines contains category
    sub-links (insurance, capital-markets, etc.). Crawl each subcategory and
    collect guideline PDF/page links with their dates.
    """
    index = _soup(source["url"])
    if not index:
        return []

    BASE = "https://www.labuanfsa.gov.my"
    # Collect subcategory URLs from the index
    cat_hrefs = []
    for a in index.select("a[href]"):
        href = a["href"]
        if "regulations/guidelines/" in href and not href.endswith("/guidelines"):
            cat_hrefs.append(_abs(href, BASE))

    seen_urls: set = set()
    items = []
    for cat_url in cat_hrefs:
        time.sleep(0.5)
        page = _soup(cat_url)
        if not page:
            continue
        main = page.find("main") or page.body
        if not main:
            continue
        for a in main.find_all("a", href=True):
            href = a["href"]
            title = a.get_text(strip=True)
            if len(title) < 8 or href in ("javascript:void(0)", "#", ""):
                continue
            full_href = _abs(href, BASE)
            if full_href in seen_urls:
                continue
            seen_urls.add(full_href)
            # Date is often in a sibling <a> with a javascript:void(0) href
            parent = a.parent
            date = ""
            if parent:
                for sib in parent.find_all("a", href=True):
                    if "void" in sib.get("href", ""):
                        raw = sib.get_text(strip=True)
                        try:
                            dateutil_parser.parse(raw, fuzzy=True)
                            date = raw
                            break
                        except Exception:
                            pass
            items.append(_item(title, full_href, date, source))

    logger.info("[html] %-42s %d items", source["name"], len(items))
    return items


def _parse_ssm(source: dict) -> list[dict]:
    """
    SSM press releases — SharePoint list at /Pages/Publication/Press_Release/
    Table rows: cell[0]=title+link, cell[1]=category, cell[2]=date (DD/MM/YYYY)
    """
    soup = _soup(source["url"])
    if not soup:
        return []

    BASE = "https://www.ssm.com.my"
    items = []
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        a = row.find("a", href=True)
        if not a or "javascript" in a["href"]:
            continue
        title = a.get_text(strip=True)
        href = _abs(a["href"], BASE)
        date = cells[2].get_text(strip=True)  # DD/MM/YYYY
        if title and len(title) > 5:
            items.append(_item(title, href, date, source))

    logger.info("[html] %-42s %d items", source["name"], len(items))
    return items


def _parse_ofac(source: dict) -> list[dict]:
    """
    OFAC recent actions — Drupal views list.
    Each item: div.views-row > div.font-sans-lg > a  (title+link)
                             > div.font-sans-2xs     (date - type)
    """
    soup = _soup(source["url"])
    if not soup:
        return []

    BASE = "https://ofac.treasury.gov"
    items = []
    for row in soup.select("div.views-row"):
        a = row.select_one("div.font-sans-lg a, div.font-sans-lg a[href]")
        if not a:
            continue
        title = a.get_text(strip=True)
        href = _abs(a["href"], BASE)
        date_el = row.select_one("div.font-sans-2xs")
        date = date_el.get_text(strip=True).split("-")[0].strip() if date_el else ""
        if title:
            items.append(_item(title, href, date, source))

    logger.info("[html] %-42s %d items", source["name"], len(items))
    return items


def _parse_fincen_news(source: dict) -> list[dict]:
    """
    FinCEN news — cards at div.fincen-news-article.
    Title: div.fincen-news-article__title > a
    Date:  time[datetime] inside the same card
    """
    soup = _soup(source["url"])
    if not soup:
        return []

    BASE = "https://www.fincen.gov"
    items = []
    for card in soup.select("div.fincen-news-article"):
        a = card.select_one("div.fincen-news-article__title a[href]")
        if not a:
            continue
        title = a.get_text(strip=True)
        href = _abs(a["href"], BASE)
        time_el = card.find("time")
        date = time_el.get("datetime", time_el.get_text(strip=True)) if time_el else ""
        if title:
            items.append(_item(title, href, date, source))

    logger.info("[html] %-42s %d items", source["name"], len(items))
    return items


def _parse_fincen_advisories(source: dict) -> list[dict]:
    """
    FinCEN advisories/bulletins — mixed list of /resources/advisories/ links
    and direct PDF links, each with a sibling <time datetime> element.
    """
    soup = _soup(source["url"])
    if not soup:
        return []

    BASE = "https://www.fincen.gov"
    seen: set = set()
    items = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        title = a.get_text(strip=True)
        if len(title) < 5:
            continue
        if not any(k in href for k in ("/resources/advisories/", "/system/files/", "FIN-")):
            continue
        full = _abs(href, BASE)
        if full in seen:
            continue
        seen.add(full)
        # Walk up the DOM to find a <time> sibling
        date = ""
        node = a.parent
        for _ in range(6):
            if not node:
                break
            t = node.find("time")
            if t:
                date = t.get("datetime", t.get_text(strip=True))
                break
            node = node.parent
        items.append(_item(title, full, date, source))

    logger.info("[html] %-42s %d items", source["name"], len(items))
    return items


def _parse_hkex_circulars(source: dict) -> list[dict]:
    """
    HKEX Participant/Member Circulars — accessible via plain requests.
    Layout: div.whats_on_tdy_row
      - Date in div.whats_on_tdy_ball (day number + "Mon YYYY")
      - Title+PDF link in div.whats_on_tdy_text_2 > a
    """
    soup = _soup(source["url"])
    if not soup:
        return []

    BASE = "https://www.hkex.com.hk"
    items = []
    for row in soup.select("div.whats_on_tdy_row"):
        # Extract date from ball div: "<N>Mar 2026" → "18 Mar 2026"
        ball = row.select_one("div.whats_on_tdy_ball")
        if ball:
            day_el = ball.select_one("div.whats_on_tdy_ball_number div")
            day = day_el.get_text(strip=True) if day_el else ""
            # The next text node in whats_on_tdy_ball is "Mon YYYY"
            month_year = ball.get_text(strip=True).replace(day, "").strip()
            date = f"{day} {month_year}".strip()
        else:
            date = ""

        # Circular links are in whats_on_tdy_text_2
        for a in row.select("div.whats_on_tdy_text_2 a[href]"):
            href = _abs(a["href"], BASE)
            title = a.get_text(strip=True).replace("pdf", "").strip()
            if title and len(title) > 5:
                items.append(_item(title, href, date, source))

    logger.info("[html] %-42s %d items", source["name"], len(items))
    return items


_HTML_PARSERS = {
    "Labuan FSA Guidelines":       _parse_labuan_fsa,
    "SSM Announcements":           _parse_ssm,
    "OFAC Recent Actions":         _parse_ofac,
    "FinCEN News":                 _parse_fincen_news,
    "FinCEN Advisories":           _parse_fincen_advisories,
    "HKEX Circulars and Notices":  _parse_hkex_circulars,
}


def fetch_html(source: dict) -> list[dict]:
    fn = _HTML_PARSERS.get(source["name"])
    if not fn:
        logger.warning("No HTML parser registered for: %s", source["name"])
        return []
    return fn(source)


# ── SEC EDGAR JSON API ─────────────────────────────────────────────────────────

def fetch_sec_edgar(source: dict) -> list[dict]:
    """
    SEC EDGAR full-text search API.
    Returns hits with entity_name, file_date, and a link to the filing.
    """
    resp = _get(source["url"])
    if not resp:
        return []
    try:
        data = resp.json()
    except Exception as exc:
        logger.error("SEC EDGAR JSON parse error [%s]: %s", source["name"], exc)
        return []

    items = []
    hits = data.get("hits", {}).get("hits", [])
    for hit in hits:
        src = hit.get("_source", {})
        title = src.get("display_names", src.get("entity_name", source["name"]))
        if isinstance(title, list):
            title = ", ".join(t.get("name", "") for t in title if t.get("name"))
        filing_id = hit.get("_id", "")
        url = f"https://efts.sec.gov/LATEST/search-index?q={filing_id}" if filing_id else source["url"]
        date = src.get("file_date", src.get("period_of_report", ""))
        if title:
            items.append(_item(str(title), url, str(date), source))

    logger.info("[sec_edgar] %-42s %d items", source["name"], len(items))
    return items


# ── Federal Register JSON API ──────────────────────────────────────────────────

def fetch_federal_register(source: dict) -> list[dict]:
    """
    Federal Register public JSON API.
    https://api.federalregister.gov/v1/documents.json?...
    """
    resp = _get(source["url"])
    if not resp:
        return []
    try:
        data = resp.json()
    except Exception as exc:
        logger.error("Federal Register JSON parse error: %s", exc)
        return []

    items = []
    for doc in data.get("results", []):
        title = doc.get("title", "").strip()
        url = doc.get("html_url", "").strip()
        date = doc.get("publication_date", "")
        if title and url:
            items.append(_item(title, url, date, source))

    logger.info("[fed_reg] %-42s %d items", source["name"], len(items))
    return items


# ── Playwright (JS-rendered / WAF-protected) ───────────────────────────────────

def _pw_soup(page, wait_selector: str, timeout: int = 15_000) -> BeautifulSoup:
    """Wait for selector then return full rendered page as BS4 soup."""
    try:
        page.wait_for_selector(wait_selector, timeout=timeout)
    except Exception:
        pass  # parse whatever is loaded
    return BeautifulSoup(page.content(), "html.parser")


def _playwright_sc_malaysia(page, source: dict) -> list[dict]:
    """SC Malaysia — custom CMS, content loaded via JS.
    Article URLs follow: /resources/media/media-release/<slug>
    Navigation links like /development/.../media-release-and-related are excluded.
    """
    soup = _pw_soup(page, "a[href*='/media-release/']", 20_000)
    items = []
    BASE = "https://www.sc.com.my"
    seen: set = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Only capture actual article slugs, not navigation pages
        if "/resources/media/media-release/" not in href.replace(BASE, ""):
            # Allow relative paths too
            if not href.startswith("resources/media/media-release/"):
                continue
        title = a.get_text(strip=True)
        if len(title) < 8:
            continue
        full = _abs(href, BASE)
        if full in seen:
            continue
        seen.add(full)
        parent = a.parent
        date_el = parent.find(class_=lambda c: c and "date" in c.lower()) if parent else None
        date = date_el.get_text(strip=True) if date_el else ""
        items.append(_item(title, full, date, source))
    return items


def _playwright_bnm(page, source: dict) -> list[dict]:
    """BNM — AWS WAF protected; Playwright passes JS challenge."""
    soup = _pw_soup(page, "a[href], .publication-item, .card", 20_000)
    items = []
    BASE = "https://www.bnm.gov.my"
    seen: set = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        title = a.get_text(strip=True)
        if len(title) < 8:
            continue
        # Only capture links that go deeper into BNM (not nav/external)
        if not (href.startswith("/") or "bnm.gov.my" in href):
            continue
        if any(skip in href for skip in ["/about", "/contact", "/career", "#"]):
            continue
        full = _abs(href, BASE)
        if full in seen or full == source["url"]:
            continue
        seen.add(full)
        parent = a.parent
        date_el = parent.find(class_=lambda c: c and "date" in c.lower()) if parent else None
        date = date_el.get_text(strip=True) if date_el else ""
        items.append(_item(title, full, date, source))
    return items


def _playwright_bursa(page, source: dict) -> list[dict]:
    """Bursa Malaysia listing amendments — table layout."""
    soup = _pw_soup(page, "table, .amendment, a[href*='amendment']", 20_000)
    items = []
    BASE = "https://www.bursamalaysia.com"
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        a = row.find("a", href=True)
        if not a:
            continue
        title = a.get_text(strip=True)
        href = _abs(a["href"], BASE)
        date = cells[0].get_text(strip=True) if cells else ""
        if title and len(title) > 5:
            items.append(_item(title, href, date, source))
    return items


def _playwright_hkma_press(page, source: dict) -> list[dict]:
    """HKMA press releases — JS-rendered list.
    Actual press release URLs match /press-releases/YYYY/MM/YYYYMMDD-N/
    Date is extracted from the URL slug (e.g. 20260318 → 2026-03-18).
    """
    soup = _pw_soup(page, "a[href*='/press-releases/20']", 20_000)
    items = []
    BASE = "https://www.hkma.gov.hk"
    seen: set = set()
    _date_re = re.compile(r"/press-releases/(\d{4})/\d{2}/(\d{8})")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Only dated press release pages, not the listing page itself
        m = _date_re.search(href)
        if not m:
            continue
        title = a.get_text(strip=True)
        if len(title) < 8:
            continue
        full = _abs(href, BASE)
        if full in seen:
            continue
        seen.add(full)
        raw_date = m.group(2)  # e.g. "20260318"
        date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        items.append(_item(title, full, date, source))
    return items


def _playwright_hkma_brdr(page, source: dict) -> list[dict]:
    """HKMA Banking Regulatory Document Repository.
    Only capture actual document pages (/eng/doc-ldg/); skip filter/topic nav links.
    """
    soup = _pw_soup(page, "a[href*='/doc-ldg/']", 15_000)
    items = []
    BASE = "https://brdr.hkma.gov.hk"
    seen: set = set()
    for a in soup.find_all("a", href=True):
        href = _abs(a["href"], BASE)
        if "/doc-ldg/" not in href:
            continue
        title = a.get_text(strip=True)
        if len(title) < 6 or href in seen:
            continue
        seen.add(href)
        items.append(_item(title, href, "", source))
    return items


def _playwright_hkex(page, source: dict) -> list[dict]:
    """HKEX Regulatory News — JS-rendered."""
    soup = _pw_soup(page, ".news-item, table tr, li", 15_000)
    items = []
    BASE = "https://www.hkex.com.hk"
    for row in soup.select(".news-item, table tr"):
        a = row.find("a", href=True)
        if not a:
            continue
        title = a.get_text(strip=True)
        href = _abs(a["href"], BASE)
        date_el = row.find(class_=lambda c: c and "date" in c.lower())
        if not date_el:
            tds = row.find_all("td")
            date_el = tds[0] if tds else None
        date = date_el.get_text(strip=True) if date_el else ""
        if title:
            items.append(_item(title, href, date, source))
    return items


def _playwright_sec_rules(page, source: dict) -> list[dict]:
    """SEC proposed/final rules and litigation releases pages."""
    soup = _pw_soup(page, "table, .article-list, h3 a, h4 a", 20_000)
    items = []
    BASE = "https://www.sec.gov"
    seen: set = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        title = a.get_text(strip=True)
        if len(title) < 10:
            continue
        # Target only rule/litigation document links, not nav
        if not any(k in href for k in ("/rules/", "/litigation/", "/newsroom/")):
            continue
        full = _abs(href, BASE)
        if full in seen or full == source["url"]:
            continue
        seen.add(full)
        parent = a.parent
        date_el = parent.find(class_=lambda c: c and "date" in c.lower()) if parent else None
        if not date_el and parent:
            date_el = parent.find("time")
        date = (date_el.get("datetime", date_el.get_text(strip=True))
                if date_el else "")
        items.append(_item(title, full, date, source))
    return items


def _playwright_federal_register(page, source: dict) -> list[dict]:
    """Federal Register SEC documents page."""
    soup = _pw_soup(page, "article, li.document, h3 a", 20_000)
    items = []
    BASE = "https://www.federalregister.gov"
    seen: set = set()
    for row in soup.select("article, li.document, div.document"):
        a = row.find("a", href=True)
        if not a:
            continue
        title = a.get_text(strip=True)
        href = _abs(a["href"], BASE)
        if len(title) < 10 or href in seen:
            continue
        seen.add(href)
        time_el = row.find("time")
        date = (time_el.get("datetime", time_el.get_text(strip=True))
                if time_el else "")
        items.append(_item(title, href, date, source))
    return items


_PLAYWRIGHT_PARSERS = {
    "SC Malaysia Media Releases":        _playwright_sc_malaysia,
    "BNM Press Releases":                _playwright_bnm,
    "BNM Legislation":                   _playwright_bnm,
    "Bursa Malaysia Listing Amendments": _playwright_bursa,
    "HKMA Press Releases":               _playwright_hkma_press,
    "HKMA Banking Regulatory Repository": _playwright_hkma_brdr,
    "SEC Proposed Rules":                _playwright_sec_rules,
    "SEC Final Rules":                   _playwright_sec_rules,
    "SEC Litigation Releases":           _playwright_sec_rules,
    "Federal Register - SEC":            _playwright_federal_register,
}


def fetch_playwright(source: dict) -> list[dict]:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except ImportError:
        logger.error("playwright not installed. Run: pip install playwright && playwright install chromium")
        return []

    fn = _PLAYWRIGHT_PARSERS.get(source["name"])
    if not fn:
        logger.warning("No Playwright parser registered for: %s", source["name"])
        return []

    items = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=HEADERS["User-Agent"])
        page = ctx.new_page()
        try:
            page.goto(source["url"], wait_until="networkidle", timeout=45_000)
            items = fn(page, source)
        except Exception as exc:
            logger.error("Playwright error [%s]: %s", source["name"], exc)
        finally:
            browser.close()

    logger.info("[playwright] %-42s %d items", source["name"], len(items))
    return items


# ── Orchestrator ───────────────────────────────────────────────────────────────

_DISPATCH = {
    "rss":        fetch_rss,
    "html":       fetch_html,
    "playwright": fetch_playwright,
}


def fetch_source(source: dict) -> list[dict]:
    fn = _DISPATCH.get(source.get("type", "html"), fetch_html)
    try:
        return fn(source)
    except Exception as exc:
        logger.error("Unhandled error [%s]: %s", source["name"], exc)
        return []


def fetch_all(
    sources: Optional[list] = None,
    *,
    dedupe: bool = True,
    delay: float = REQUEST_DELAY,
) -> list[dict]:
    """
    Fetch all sources; optionally deduplicate against SQLite store.

    Returns list of {title, url, date, source, jurisdiction, tags}.
    When dedupe=True, only items not previously seen are returned.
    """
    if sources is None:
        sources = SOURCES

    all_items: list = []
    for i, source in enumerate(sources):
        items = fetch_source(source)
        all_items.extend(items)
        if i < len(sources) - 1:
            time.sleep(delay)

    if not dedupe:
        logger.info("Fetched %d items (deduplication skipped)", len(all_items))
        return all_items

    new_items, skipped = deduplicate(all_items)
    logger.info(
        "Fetched %d total — %d new, %d duplicates skipped",
        len(all_items), len(new_items), skipped,
    )
    return new_items


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Run the regulatory fetcher")
    ap.add_argument("--jurisdiction", "-j", help="Filter to one jurisdiction (MY, HK, US, MY-LABUAN)")
    ap.add_argument("--source", "-s", help="Filter to a single source name (substring match)")
    ap.add_argument("--no-dedupe", action="store_true", help="Skip SQLite deduplication")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-8s %(name)s: %(message)s",
    )

    filtered = SOURCES
    if args.jurisdiction:
        filtered = [s for s in filtered if s["jurisdiction"] == args.jurisdiction]
    if args.source:
        filtered = [s for s in filtered if args.source.lower() in s["name"].lower()]

    results = fetch_all(filtered, dedupe=not args.no_dedupe)

    for item in results:
        print(f"[{item['jurisdiction']}] {item['date']}  {item['source']}")
        print(f"  {item['title']}")
        print(f"  {item['url']}")
        print()

    print(f"─── {len(results)} item(s) returned ───")
