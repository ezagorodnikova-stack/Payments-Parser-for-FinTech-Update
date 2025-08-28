#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NewsScraper v4 (Python 3.12) — stdlib-only, robust, with presets & diagnostics.

Features
- Finds RSS/Atom feeds automatically + optional curated presets for popular fintech media.
- Robust date parsing using email.utils.parsedate_to_datetime + ISO/RFC fallbacks.
- Filters by date window (--days OR --start/--end).
- Fallback to sitemap.xml when no feeds or to enrich with lastmod dates.
- Fetches short description from feed or article <meta name/og/twitter:description> as fallback.
- Outputs combined CSV/TXT + per-site CSVs.
- Verbose diagnostics: per-site counts + reasons why nothing was collected.
- Flags:
  --accept-undated  Include items without a parsed date (treated as end_dt to pass the filter).
  --max-items       Cap items per site (after filtering & dedup).
  --verbose         Print extra debug.
  --presets         Add curated feeds for known fintech media (Finextra, TechCrunch/Fintech, PYMNTS, The Paypers).

Usage examples
  python news_scraper.py --days 30 --sites https://www.finextra.com,https://techcrunch.com --presets --verbose
  python news_scraper.py --start 2025-07-01 --end 2025-08-27 --sites https://www.thepaypers.com --presets
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import html
from html.parser import HTMLParser
import os
import re
import sys
import time
import urllib.parse as urlparse
import urllib.request as urlrequest
import ssl
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple

DEFAULT_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# --------------- HTTP ---------------
SSL_CONTEXT = None  # set in main() based on --insecure/--cafile
def http_get(url: str, timeout: int = 20, headers: Optional[Dict[str, str]] = None) -> bytes:
    hdrs = {"User-Agent": DEFAULT_UA, "Accept": "*/*"}
    if headers:
        hdrs.update(headers)
    req = urlrequest.Request(url, headers=hdrs)
    with urlrequest.urlopen(req, timeout=timeout) as resp:
        return resp.read()

# --------------- Date parsing ---------------
ISO_PATTERNS = [
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
]

def parse_datetime(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # Try email.utils (robust for RFC822/RFC2822)
    try:
        dt = parsedate_to_datetime(s)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    except Exception:
        pass
    # Try ISO formats
    for pat in ISO_PATTERNS:
        try:
            dt = datetime.strptime(s, pat)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    # Normalize trailing +0000 → +00:00
    m = re.search(r"([+-]\d{4})$", s)
    if m:
        fixed = s[:-5] + m.group(1)[:3] + ":" + m.group(1)[3:]
        for pat in ISO_PATTERNS:
            try:
                dt = datetime.strptime(fixed, pat)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
    return None

# --------------- Presets ---------------
PRESET_FEEDS: Dict[str, List[str]] = {
    # Domain -> list of full feed URLs
    "www.finextra.com": [
        "https://www.finextra.com/rss/allnews.aspx",
        "https://www.finextra.com/rss/payments",
        "https://www.finextra.com/rss/retail",
    ],
    "finextra.com": [
        "https://www.finextra.com/rss/allnews.aspx",
        "https://www.finextra.com/rss/payments",
        "https://www.finextra.com/rss/retail",
    ],
    "techcrunch.com": [
        "https://techcrunch.com/category/fintech/feed/",
        "https://techcrunch.com/tag/payments/feed/",
        "https://techcrunch.com/tag/banking/feed/",
    ],
    "www.pymnts.com": [
        "https://www.pymnts.com/feed/",
    ],
    "www.thepaypers.com": [
        "https://www.thepaypers.com/rss",
    ],
}

# --------------- Feed discovery ---------------
COMMON_FEED_PATHS = [
    "/feed",
    "/rss",
    "/rss.xml",
    "/feed.xml",
    "/atom.xml",
    "/feeds/posts/default?alt=rss",
]

class LinkFeedFinder(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.feeds: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "link":
            return
        d = {k.lower(): v for k, v in attrs}
        rel = d.get("rel", "").lower()
        type_ = d.get("type", "").lower()
        href = d.get("href", "")
        if "alternate" in rel and ("rss" in type_ or "atom" in type_ or "xml" in type_):
            url = urlparse.urljoin(self.base_url, href)
            self.feeds.append(url)

def discover_feeds(site_url: str, throttle: float = 0.6, verbose: bool = False) -> List[str]:
    feeds: List[str] = []
    # Try common paths
    for path in COMMON_FEED_PATHS:
        candidate = urlparse.urljoin(site_url.rstrip("/") + "/", path.lstrip("/"))
        try:
            data = http_get(candidate, timeout=10)
            if data and (b"<rss" in data or b"<feed" in data):
                feeds.append(candidate)
                if verbose: print(f"  [+] feed found: {candidate}")
        except Exception:
            pass
        time.sleep(throttle)
    # Parse homepage for <link rel="alternate">
    try:
        home = http_get(site_url, timeout=10)
        parser = LinkFeedFinder(site_url)
        parser.feed(home.decode("utf-8", errors="ignore"))
        for f in parser.feeds:
            if f not in feeds:
                try:
                    data = http_get(f, timeout=10)
                    if data and (b"<rss" in data or b"<feed" in data):
                        feeds.append(f)
                        if verbose: print(f"  [+] alt feed: {f}")
                except Exception:
                    pass
                time.sleep(throttle)
    except Exception:
        pass
    return feeds

# --------------- Feed parsing ---------------
def strip_html(s: str) -> str:
    s = re.sub(r"(?is)<script.*?>.*?</script>", " ", s)
    s = re.sub(r"(?is)<style.*?>.*?</style>", " ", s)
    s = re.sub(r"(?is)<.*?>", " ", s)
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

@dataclass
class Article:
    source: str
    title: str
    link: str
    date: Optional[datetime]  # UTC
    description: str

def parse_rss_atom(feed_xml: bytes, source_domain: str) -> List[Article]:
    items: List[Article] = []
    root = ET.fromstring(feed_xml)

    # RSS 2.0
    if root.tag.lower().endswith("rss") or root.find("channel") is not None:
        channel = root.find("channel")
        if channel is not None:
            for item in channel.findall("item"):
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                desc = (item.findtext("description") or "").strip()
                pub = (item.findtext("pubDate") or
                       item.findtext("{http://purl.org/dc/elements/1.1/}date") or "")
                date = parse_datetime(pub) if pub else None
                if not desc:
                    c = item.find("{http://purl.org/rss/1.0/modules/content/}encoded")
                    if c is not None and c.text:
                        desc = strip_html(c.text)[:400]
                items.append(Article(source_domain, title, link, date, strip_html(desc)[:400]))
        return items

    # Atom
    if root.tag.endswith("feed") or root.find("{http://www.w3.org/2005/Atom}entry") is not None:
        for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
            title = (entry.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
            href = ""
            # prefer link rel="alternate"
            for link_el in entry.findall("{http://www.w3.org/2005/Atom}link"):
                rel = (link_el.get("rel") or "").lower()
                if rel in ("", "alternate"):
                    href = link_el.get("href", "") or href
            desc = (entry.findtext("{http://www.w3.org/2005/Atom}summary") or "").strip()
            pub = (entry.findtext("{http://www.w3.org/2005/Atom}updated") or
                   entry.findtext("{http://www.w3.org/2005/Atom}published") or "")
            date = parse_datetime(pub) if pub else None
            items.append(Article(source_domain, title, href, date, strip_html(desc)[:400]))
        return items

    return items

# --------------- Meta description (fallback) ---------------
class MetaDescFinder(HTMLParser):
    def __init__(self):
        super().__init__()
        self.desc = ""

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "meta":
            return
        d = {k.lower(): v for k, v in attrs}
        name = (d.get("name") or d.get("property") or "").lower()
        if name in ("description", "og:description", "twitter:description"):
            content = d.get("content", "")
            if content and not self.desc:
                self.desc = content

def fetch_meta_description(url: str, timeout: int = 15) -> str:
    try:
        html_bytes = http_get(url, timeout=timeout)
        parser = MetaDescFinder()
        parser.feed(html_bytes.decode("utf-8", errors="ignore"))
        return strip_html(parser.desc)[:400]
    except Exception:
        return ""

# --------------- Sitemap parsing ---------------
def discover_sitemaps(site_url: str) -> List[str]:
    sitemaps: List[str] = []
    robots = ""
    try:
        robots = http_get(urlparse.urljoin(site_url.rstrip('/') + '/', "/robots.txt"),
                          timeout=10).decode("utf-8", errors="ignore")
    except Exception:
        robots = ""
    for line in robots.splitlines():
        if line.lower().startswith("sitemap:"):
            sm = line.split(":", 1)[1].strip()
            sitemaps.append(sm)
    sitemaps.append(urlparse.urljoin(site_url.rstrip('/') + '/', "/sitemap.xml"))
    uniq: List[str] = []
    seen = set()
    for s in sitemaps:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq

def parse_sitemap(sitemap_url: str, max_urls: int = 5000) -> List[Tuple[str, Optional[datetime]]]:
    urls: List[Tuple[str, Optional[datetime]]] = []
    try:
        data = http_get(sitemap_url, timeout=15)
    except Exception:
        return urls
    try:
        root = ET.fromstring(data)
    except Exception:
        return urls
    if root.tag.endswith("sitemapindex"):
        for sm in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap"):
            loc = sm.findtext("{http://www.sitemaps.org/schemas/sitemap/0.9}loc") or ""
            urls.extend(parse_sitemap(loc, max_urls))
        return urls
    count = 0
    for url_el in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
        loc = url_el.findtext("{http://www.sitemaps.org/schemas/sitemap/0.9}loc") or ""
        lastmod_text = url_el.findtext("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod") or ""
        dt = parse_datetime(lastmod_text) if lastmod_text else None
        if loc:
            urls.append((loc, dt))
            count += 1
            if count >= max_urls:
                break
    return urls

# --------------- Core workflow ---------------
def domain_from_url(u: str) -> str:
    try:
        return urlparse.urlparse(u).netloc
    except Exception:
        return u

def gather_from_site(site_url: str, start_dt_utc: datetime, end_dt_utc: datetime,
                     throttle: float = 0.6, accept_undated: bool = False, verbose: bool = False,
                     use_presets: bool = False, max_items: int = 1000) -> Tuple[List[Article], List[str]]:
    domain = domain_from_url(site_url)
    collected: List[Article] = []
    notes: List[str] = []

    # Presets
    feeds: List[str] = []
    if use_presets and domain in PRESET_FEEDS:
        feeds.extend(PRESET_FEEDS[domain])
        if verbose: print(f"  [*] presets: {len(feeds)} feeds")

    # Discovered feeds
    discovered = discover_feeds(site_url, throttle=throttle, verbose=verbose)
    for f in discovered:
        if f not in feeds:
            feeds.append(f)

    if verbose:
        print(f"  feeds total: {len(feeds)}")

    # Parse feeds
    for f in feeds:
        try:
            data = http_get(f, timeout=20)
            arts = parse_rss_atom(data, domain)
            for a in arts:
                adate = a.date
                if adate is None and accept_undated:
                    # treat undated as end_dt to include it
                    adate = end_dt_utc
                if adate is None:
                    continue
                if start_dt_utc <= adate <= end_dt_utc:
                    if (not a.description) and a.link:
                        a.description = fetch_meta_description(a.link)
                    collected.append(a)
        except Exception as e:
            notes.append(f"feed error {f}: {e}")
        time.sleep(throttle)

    # Fallback to sitemaps if nothing
    if not collected:
        sitemaps = discover_sitemaps(site_url)
        if verbose: print(f"  sitemaps: {len(sitemaps)}")
        for sm in sitemaps:
            try:
                pairs = parse_sitemap(sm)
            except Exception as e:
                notes.append(f"sitemap error {sm}: {e}")
                continue
            for loc, dt in pairs:
                adate = dt or (end_dt_utc if accept_undated else None)
                if adate and (start_dt_utc <= adate <= end_dt_utc):
                    title_guess = loc.rsplit("/", 1)[-1].replace("-", " ").title()
                    desc = fetch_meta_description(loc)
                    collected.append(Article(domain, title_guess, loc, adate, desc))
            time.sleep(throttle)

    # Deduplicate by link and clamp
    seen = set()
    def sort_key(x: Article): return x.date or datetime.min.replace(tzinfo=timezone.utc)
    uniq: List[Article] = []
    for a in sorted([a for a in collected if a.link], key=sort_key, reverse=True):
        if a.link in seen:
            continue
        seen.add(a.link)
        uniq.append(a)
        if len(uniq) >= max_items:
            break

    if not uniq:
        if not feeds and not discovered:
            notes.append("no feeds discovered")
        notes.append("no items matched the date filter")
    return uniq, notes

def ensure_dirs():
    os.makedirs("output", exist_ok=True)
    os.makedirs(os.path.join("output", "sites"), exist_ok=True)

def write_outputs(all_articles: List[Article]):
    ensure_dirs()
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    combined_csv = os.path.join("output", f"news_{ts}.csv")
    combined_txt = os.path.join("output", f"news_{ts}.txt")

    with open(combined_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source", "date_utc", "title", "link", "description"])
        for a in all_articles:
            w.writerow([a.source, a.date.isoformat() if a.date else "", a.title, a.link, a.description])

    with open(combined_txt, "w", encoding="utf-8") as f:
        for a in all_articles:
            date_str = a.date.strftime("%Y-%m-%d %H:%M:%SZ") if a.date else ""
            f.write(f"# {a.title}\n")
            f.write(f"Source: {a.source}\n")
            f.write(f"Date:   {date_str}\n")
            f.write(f"Link:   {a.link}\n")
            if a.description:
                f.write(f"Desc:   {a.description}\n")
            f.write("\n")

    by_site: Dict[str, List[Article]] = {}
    for a in all_articles:
        by_site.setdefault(a.source, []).append(a)

    for site, items in by_site.items():
        path = os.path.join("output", "sites", f"{site.replace(':','_')}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date_utc", "title", "link", "description"])
            for a in items:
                w.writerow([a.date.isoformat() if a.date else "", a.title, a.link, a.description])

    return combined_csv, combined_txt

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Fetch recent news from multiple sites (RSS/Atom/sitemaps).")
    p.add_argument("--sites", help="Comma-separated list of site URLs (e.g., https://finextra.com,https://techcrunch.com)")
    p.add_argument("--sites-file", help="Path to a text file with one site URL per line")
    p.add_argument("--days", type=int, default=None, help="Last N days window (e.g., 30).")
    p.add_argument("--start", help="Start date (YYYY-MM-DD) in UTC; overrides --days if provided.")
    p.add_argument("--end", help="End date (YYYY-MM-DD) in UTC; default: now UTC.")
    p.add_argument("--throttle", type=float, default=0.6, help="Seconds to sleep between requests.")
    p.add_argument("--accept-undated", action="store_true", help="Include undated items (treated as end_dt).")
    p.add_argument("--max-items", type=int, default=1000, help="Max items per site after filtering/dedup.")
    p.add_argument("--verbose", action="store_true", help="Verbose diagnostics.")
    p.add_argument("--presets", action="store_true", help="Add curated feeds for known fintech media.")
    p.add_argument("--insecure", action="store_true", help="Disable TLS certificate verification (not recommended).")
    p.add_argument("--cafile", help="Path to a custom CA bundle (PEM).")
    return p.parse_args(argv)

def parse_date_ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def main(argv=None):
    global SSL_CONTEXT
    args = parse_args(argv)
    # Initialize SSL context
    if args.insecure:
        SSL_CONTEXT = ssl._create_unverified_context()
    else:
        if args.cafile:
            SSL_CONTEXT = ssl.create_default_context(cafile=args.cafile)
        else:
            SSL_CONTEXT = ssl.create_default_context()
    sites: List[str] = []
    if args.sites:
        sites.extend([s.strip() for s in args.sites.split(",") if s.strip()])
    if args.sites_file and os.path.exists(args.sites_file):
        with open(args.sites_file, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    sites.append(s)
    if not sites:
        print("No sites provided. Use --sites or --sites-file.")
        return 2

    now_utc = datetime.now(timezone.utc)
    if args.start:
        start_dt = parse_date_ymd(args.start)
    elif args.days:
        start_dt = now_utc - timedelta(days=int(args.days))
    else:
        start_dt = now_utc - timedelta(days=30)
    end_dt = parse_date_ymd(args.end) if args.end else now_utc

    print(f"Collecting from {len(sites)} site(s) between {start_dt.isoformat()} and {end_dt.isoformat()} ...")

    all_articles: List[Article] = []
    for site in sites:
        print(f"→ {site} ...")
        arts, notes = gather_from_site(
            site, start_dt, end_dt,
            throttle=args.throttle,
            accept_undated=args.accept_undated,
            verbose=args.verbose,
            use_presets=args.presets,
            max_items=args.max_items
        )
        print(f"  collected {len(arts)} item(s)")
        for n in notes:
            print(f"    note: {n}")
        all_articles.extend(arts)

    all_articles.sort(key=lambda a: (a.date or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
    csv_path, txt_path = write_outputs(all_articles)
    print(f"Done. Combined CSV: {csv_path}")
    print(f"Combined TXT: {txt_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
