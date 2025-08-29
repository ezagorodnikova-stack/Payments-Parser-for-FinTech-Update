# tg_channel_parser_bot.py
# ДВА РЕЖИМА:
# 1) «Парсинг тг каналов» — фильтр по датам/ключам и HTML (первые 2 абзаца).
# 2) «Парсинг сайтов» — RSS/Atom/Sitemaps, только ссылки пользователя (без пресетов), надёжный парс дат + content:encoded, HTML-вывод.

import os
import re
import sys
import csv
import shlex
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple, Optional, Set, Dict
from html import escape as html_escape

from dotenv import load_dotenv
from jinja2 import Template
from bs4 import BeautifulSoup

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    UsernameNotOccupiedError, UsernameInvalidError,
    ChannelPrivateError, ChatAdminRequiredError,
)
from telethon.tl.types import Message

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ConversationHandler,
    ContextTypes, CallbackQueryHandler, filters
)

# ---------- ЛОГИ ----------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("parser-bot")

# ---------- СОСТОЯНИЯ ----------
(MENU, LINK, PERIOD, KEYWORDS, SITE_SITES, SITE_PERIOD, SITE_CONFIRM) = range(7)

# ---------- КОНФИГ ----------
load_dotenv()
BOT_TOKEN       = os.getenv("BOT_TOKEN")
API_ID          = int(os.getenv("API_ID", "0"))
API_HASH        = os.getenv("API_HASH")
SESSION_STRING  = os.getenv("TELETHON_SESSION")
DEFAULT_DAYS    = int(os.getenv("DEFAULT_DAYS", "30"))
RESULTS_LIMIT   = int(os.getenv("RESULTS_LIMIT", "5000"))
OUTPUT_DIR      = Path(os.getenv("OUTPUT_DIR", "output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

WORK_ROOT       = Path("work")  # рабочая папка для режима «сайты»
WORK_ROOT.mkdir(parents=True, exist_ok=True)

if not (BOT_TOKEN and API_ID and API_HASH and SESSION_STRING):
    raise SystemExit("ERROR: BOT_TOKEN, API_ID, API_HASH, TELETHON_SESSION обязательны в .env")

# ---------- Telethon клиент ----------
tg_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ---------- HTML-шаблоны ----------
HTML_TEMPLATE_TG = Template("""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>{{ title }}</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    body { font-family: -apple-system, system-ui, Segoe UI, Roboto, Arial, sans-serif; margin: 0; background: #0b0f19; color: #e8ebf5; }
    .wrap { max-width: 980px; margin: 0 auto; padding: 32px 16px; }
    .card { background: #12182b; border: 1px solid #1e2742; border-radius: 16px; padding: 16px 18px; margin: 12px 0; box-shadow: 0 10px 30px rgba(0,0,0,.25); }
    .muted { color: #a9b2d6; font-size: 13px; }
    .title { font-size: 24px; margin: 0 0 8px 0; }
    .pill { display: inline-block; background: #1c2440; border: 1px solid #2b355a; color: #b5c3ff; padding: 2px 10px; border-radius: 999px; margin-right: 6px; font-size: 12px;}
    .post-title { font-size: 16px; margin: 0; line-height: 1.45 }
    a { color: #8fb3ff; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .header { margin-bottom: 16px; }
    .content p { margin: 0.6em 0; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <h1 class="title">{{ channel_name }}</h1>
      <div class="muted">Период: {{ period_str }} • Найдено: {{ total }}{% if chips %} • Слова: {% for k in chips %}<span class="pill">{{ k }}</span>{% endfor %}{% endif %}</div>
    </div>
    {% for p in posts %}
    <div class="card">
      <div class="muted">{{ p['date'] }}</div>
      {% if p['link'] %}
        <h3 class="post-title"><a href="{{ p['link'] }}" target="_blank" rel="noopener noreferrer">Открыть пост →</a></h3>
      {% else %}
        <h3 class="post-title">Пост #{{ p['id'] }}</h3>
      {% endif %}
      <div class="content">{{ p['html'] | safe }}</div>
    </div>
    {% endfor %}
  </div>
</body>
</html>
""".strip())

HTML_TEMPLATE_SITES = Template("""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>{{ title }}</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    body { font-family: -apple-system, system-ui, Segoe UI, Roboto, Arial, sans-serif; margin: 0; background: #0b0f19; color: #e8ebf5; }
    .wrap { max-width: 980px; margin: 0 auto; padding: 32px 16px; }
    .card { background: #12182b; border: 1px solid #1e2742; border-radius: 16px; padding: 16px 18px; margin: 12px 0; box-shadow: 0 10px 30px rgba(0,0,0,.25); }
    .muted { color: #a9b2d6; font-size: 13px; }
    .title { font-size: 24px; margin: 0 0 8px 0; }
    .pill { display: inline-block; background: #1c2440; border: 1px solid #2b355a; color: #b5c3ff; padding: 2px 10px; border-radius: 999px; margin-right: 6px; font-size: 12px;}
    .post-title { font-size: 18px; margin: 0; line-height: 1.45 }
    a { color: #8fb3ff; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .header { margin-bottom: 16px; }
    .content p { margin: 0.6em 0; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <h1 class="title">Сайты — подборка</h1>
      <div class="muted">Период: {{ period_str }} • Найдено: {{ total }}{% if chips %} • Источники: {% for k in chips %}<span class="pill">{{ k }}</span>{% endfor %}{% endif %}</div>
    </div>
    {% for p in posts %}
    <div class="card">
      <div class="muted">{{ p['date'] }} • {{ p['source'] }}</div>
      <h3 class="post-title">{% if p['link'] %}<a href="{{ p['link'] }}" target="_blank" rel="noopener noreferrer">{{ p['title'] or "Открыть статью →" }}</a>{% else %}{{ p['title'] or "Статья" }}{% endif %}</h3>
      {% if p['html'] %}<div class="content">{{ p['html'] | safe }}</div>{% endif %}
    </div>
    {% endfor %}
  </div>
</body>
</html>
""".strip())

# ---------- ВСТРОЕННЫЙ ПАРСЕР САЙТОВ (исправлен) ----------
EMBEDDED_SITE_PARSER_NAME = "embedded_site_parser.py"
EMBEDDED_SITE_PARSER_CODE = r'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, ssl, csv, time
from datetime import datetime, timezone, timedelta
import urllib.parse as urlparse
import urllib.request as urlrequest
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple

DEFAULT_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
SSL_CONTEXT = None

def root_url(url: str) -> str:
    p = urlparse.urlparse(url)
    scheme = p.scheme or "https"
    return f"{scheme}://{p.netloc}/" if p.netloc else url

def http_get(url: str, timeout: int = 25, headers: Optional[Dict[str, str]] = None) -> bytes:
    req = urlrequest.Request(url, headers=headers or {"User-Agent": DEFAULT_UA})
    with urlrequest.urlopen(req, timeout=timeout, context=SSL_CONTEXT) as resp:
        return resp.read()

def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def to_aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def parse_date_guess(s: str):
    s = (s or "").strip()
    if not s: return None
    # RFC-2822
    try:
        return to_aware(parsedate_to_datetime(s))
    except Exception:
        pass
    # ISO-like
    for pat in [r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?Z?$",
                r"^\d{4}/\d{2}/\d{2}$",
                r"^\d{1,2}\s+\w+\s+\d{4}"]:
        if re.match(pat, s):
            try:
                s2 = s.replace("/", "-").replace("T", " ").replace("Z", "")
                if len(s2) == 10: s2 += " 00:00"
                return to_aware(datetime.fromisoformat(s2))
            except Exception:
                pass
    return None

def parse_date_or_default(s: Optional[str], default_dt: datetime) -> datetime:
    if not s: return default_dt
    s = s.strip()
    try:
        return to_aware(parsedate_to_datetime(s))
    except Exception:
        pass
    try:
        s2 = s.replace("/", "-").replace("T", " ").replace("Z", "")
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}$", s2):
            return datetime.fromisoformat(s2).replace(tzinfo=timezone.utc)
        return to_aware(datetime.fromisoformat(s2))
    except Exception:
        return default_dt

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def parse_feed_xml(xml_bytes: bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []
    items = []
    # RSS 2.0
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        # description ИЛИ content:encoded
        desc = (it.findtext("description") or "").strip()
        cenc = it.findtext("{http://purl.org/rss/1.0/modules/content/}encoded")
        if cenc and (not desc or len(cenc) > len(desc)):
            desc = cenc
        pub_date = (it.findtext("pubDate") or it.findtext("date")
                    or it.findtext("{http://purl.org/dc/elements/1.1/}date"))
        items.append((title, link, pub_date, desc))
    # Atom
    ns_atom = "{http://www.w3.org/2005/Atom}"
    for it in root.findall(f".//{ns_atom}entry"):
        title = (it.findtext(f"{ns_atom}title") or "").strip()
        link_el = it.find(f"{ns_atom}link")
        link = link_el.get("href") if link_el is not None else ""
        pub_date = (it.findtext(f"{ns_atom}updated") or it.findtext(f"{ns_atom}published"))
        desc = it.findtext(f"{ns_atom}summary") or ""
        content_el = it.find(f"{ns_atom}content")
        if content_el is not None and (not desc or len(content_el.text or "") > len(desc)):
            desc = content_el.text or ""
        items.append((title, link, pub_date, desc))
    return items

def discover_feeds(html: str, base_root: str):
    feeds = set()
    # <link rel="alternate" ...>
    for m in re.finditer(r'<link[^>]+rel=["\'](?:alternate|feed)["\'][^>]+>', html, flags=re.I):
        tag = m.group(0)
        href_m = re.search(r'href=["\']([^"\']+)["\']', tag, flags=re.I)
        type_m = re.search(r'type=["\']([^"\']+)["\']', tag, flags=re.I)
        if href_m:
            href = href_m.group(1)
            if not href.startswith("http"): href = urlparse.urljoin(base_root, href)
            if not type_m or re.search(r'(rss|atom|xml)', type_m.group(1), flags=re.I):
                feeds.add(href)
    # типовые пути
    for suffix in ["/feed", "/rss", "/rss.xml", "/atom.xml", "/feed.xml"]:
        feeds.add(urlparse.urljoin(base_root, suffix))
    return list(feeds)

def get_sitemap_links(base_root: str):
    try:
        rb = http_get(urlparse.urljoin(base_root, "robots.txt"))
        links = re.findall(r"(?im)^sitemap:\s*(\S+)$", rb.decode("utf-8", "ignore"))
        return links
    except Exception:
        return []

def parse_sitemap(xml_bytes: bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for it in root.findall(".//sm:url", ns):
        loc = it.findtext("sm:loc", default="", namespaces=ns).strip()
        lastmod = it.findtext("sm:lastmod", default="", namespaces=ns).strip()
        urls.append((loc, lastmod))
    return urls

def collect_site(site_url: str, end_dt, start_dt, throttle=0.6, accept_undated=True, verbose=False, max_items=2000):
    collected, reason = [], ""
    base_root = root_url(site_url)
    try:
        try:
            html = http_get(base_root, timeout=25).decode("utf-8", "ignore")
        except Exception as e:
            return [], f"get_homepage_error:{e}"
        feeds = discover_feeds(html, base_root)
        if verbose: print(f"[feeds] {base_root} -> {len(feeds)} candidates")
        for f in feeds:
            try:
                fb = http_get(f, timeout=25)
                for (title, link, pub_date, desc) in parse_feed_xml(fb):
                    dt = parse_date_guess(pub_date)
                    if not dt and accept_undated:
                        dt = end_dt
                    if not dt or not (start_dt <= dt <= end_dt):
                        continue
                    if not link or not link.startswith("http"):
                        link = urlparse.urljoin(base_root, link or "/")
                    collected.append({
                        "title": (title or "").strip(),
                        "link": (link or "").strip(),
                        "date": to_iso(dt),
                        "summary": (desc or "").strip(),
                        "source": base_root.rstrip("/")
                    })
            except Exception as e:
                if verbose: print(f"[feed_error] {f}: {e}")
        if not collected:
            for sm in get_sitemap_links(base_root):
                try:
                    urls = parse_sitemap(http_get(sm, timeout=25))
                    for u, lastmod in urls:
                        dt = parse_date_guess(lastmod)
                        if not dt and accept_undated:
                            dt = end_dt
                        if not dt or not (start_dt <= dt <= end_dt):
                            continue
                        collected.append({
                            "title": "",
                            "link": (u or "").strip(),
                            "date": to_iso(dt),
                            "summary": "",
                            "source": base_root.rstrip("/")
                        })
                except Exception as e:
                    if verbose: print(f"[sitemap_error] {sm}: {e}")
        seen, deduped = set(), []
        for it in collected:
            lk = it["link"]
            if lk in seen: continue
            seen.add(lk); deduped.append(it)
        if len(deduped) > max_items: deduped = deduped[:max_items]
        return deduped, reason
    finally:
        time.sleep(throttle)

def write_csv(rows: List[Dict[str, str]], path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","title","link","summary","source"])
        w.writeheader()
        for r in rows: w.writerow(r)

def run(sites: List[str], days: Optional[int], start: Optional[str], end: Optional[str],
        throttle: float = 0.6, accept_undated=True, max_items=2000, verbose=False,
        cafile: Optional[str] = None, insecure=False, out_dir: str = "output"):
    global SSL_CONTEXT
    if insecure:
        SSL_CONTEXT = ssl._create_unverified_context()
    else:
        SSL_CONTEXT = ssl.create_default_context(cafile=cafile) if cafile else ssl.create_default_context()

    end_dt = parse_date_or_default(end, datetime.now(timezone.utc))
    if days is not None and (not start and not end):
        start_dt = end_dt - timedelta(days=days)
    else:
        start_dt = parse_date_or_default(start, end_dt - timedelta(days=30))

    sites_norm = []
    for s in (sites or []):
        s = s.strip()
        if not s: continue
        if not s.startswith("http"): s = "https://" + s.lstrip("/")
        sites_norm.append(s)

    all_rows = []
    for s in sites_norm:
        rows, _ = collect_site(s, end_dt=end_dt, start_dt=start_dt,
                               throttle=throttle, accept_undated=accept_undated,
                               verbose=verbose, max_items=max_items)
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", urlparse.urlparse(root_url(s)).netloc or "site")
        write_csv(rows, os.path.join(out_dir, f"{safe}.csv"))
        all_rows.extend(rows)
    write_csv(all_rows, os.path.join(out_dir, "all_sites.csv"))
'''

# ---------- Общие утилиты ----------
def parse_channel_identifier(raw: str) -> str:
    raw = raw.strip()
    m = re.search(r"(?:t\.me/|@)([A-Za-z0-9_]{3,})/?$", raw)
    if m: return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_]{3,}", raw): return raw
    raise ValueError("Не удалось распознать ссылку/юзернейм. Пример: https://t.me/fintechfutures или @fintechfutures")

def parse_period(text: str) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    s = (text or "").strip().lower()
    if re.fullmatch(r"\d{1,4}", s):
        days = int(s); return now - timedelta(days=days), now
    dates = re.findall(r"\b(\d{4}-\d{2}-\d{2})\b", s)
    if len(dates) >= 2:
        d1 = datetime.fromisoformat(dates[0]).replace(tzinfo=timezone.utc)
        d2 = datetime.fromisoformat(dates[1]).replace(tzinfo=timezone.utc)
        start, end = sorted([d1, d2])
        return start, end + timedelta(days=1)
    elif len(dates) == 1:
        d1 = datetime.fromisoformat(dates[0]).replace(tzinfo=timezone.utc)
        return d1, now
    else:
        return now - timedelta(days=DEFAULT_DAYS), now

def normalize_keywords(text: str) -> List[str]:
    parts = [p.strip().lower() for p in re.split(r"[,;\n]", text or "") if p.strip()]
    seen, out = set(), []
    for p in parts:
        if p not in seen: seen.add(p); out.append(p)
    return out

def message_text(msg: Message) -> str:
    return msg.message or ""

def match_keywords(text: str, keywords: List[str]) -> bool:
    if not keywords: return True
    t = text.lower()
    return any(k in t for k in keywords)

def channel_permalink(username: Optional[str], msg_id: int) -> Optional[str]:
    return f"https://t.me/{username}/{msg_id}" if username else None

def first_paragraphs_html(raw_text: str, n: int = 2) -> Optional[str]:
    plain = BeautifulSoup(raw_text or "", "html.parser").get_text()
    t = plain.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t: return None
    paras = [p.strip() for p in re.split(r"\n\s*\n+", t) if p.strip()]
    if len(paras) < n:
        lines = [ln.strip() for ln in t.split("\n") if ln.strip()]
        paras = lines[:n]
    else:
        paras = paras[:n]
    html_parts = []
    for p in paras:
        p_html = html_escape(p).replace("\n", "<br>")
        html_parts.append(f"<p>{p_html}</p>")
    return "".join(html_parts) if html_parts else None

def render_html_tg(channel_name: str, period_str: str, chips: List[str], posts: List[dict]) -> str:
    return HTML_TEMPLATE_TG.render(
        title=f"{channel_name} — подборка",
        channel_name=channel_name,
        period_str=period_str,
        chips=chips,
        posts=posts,
        total=len(posts),
    )

def render_html_sites(period_str: str, sources_chips: List[str], posts: List[dict]) -> str:
    return HTML_TEMPLATE_SITES.render(
        title="Сайты — подборка",
        period_str=period_str,
        chips=sources_chips,
        posts=posts,
        total=len(posts),
    )

def safe_filename(s: str) -> str:
    s = re.sub(r"[^\w\-\.\s]", "_", s, flags=re.UNICODE).strip()
    return re.sub(r"\s+", "_", s)

# ---------- Утилиты «Сайты» ----------
def user_workdir(user_id: int) -> Path:
    d = WORK_ROOT / f"user_{user_id}"
    (d / "output").mkdir(parents=True, exist_ok=True)
    return d

def ensure_embedded_script_on_disk(workdir: Path) -> Path:
    script_path = workdir / EMBEDDED_SITE_PARSER_NAME
    script_path.write_text(EMBEDDED_SITE_PARSER_CODE, encoding="utf-8")
    return script_path

async def run_site_script(args_list: List[str], workdir: Path, timeout_sec: int = 1200) -> Tuple[int, str, str]:
    script_path = ensure_embedded_script_on_disk(workdir)
    cmd = [sys.executable, str(script_path)] + args_list
    log.info("Running embedded site parser: %s", " ".join(shlex.quote(c) for c in cmd))
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=str(workdir),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        proc.kill()
        return 124, "", "Timeout while running embedded site parser"
    return proc.returncode, stdout_b.decode("utf-8", errors="ignore"), stderr_b.decode("utf-8", errors="ignore")

def site_confirm_keyboard() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("▶️ Запустить", callback_data="site:run")],
        [InlineKeyboardButton("❌ Отмена", callback_data="site:cancel")],
    ]
    return InlineKeyboardMarkup(kb)

def norm_urls_from_text(text: str) -> List[str]:
    urls = re.findall(r'https?://[^\s,]+', text or "", flags=re.I)
    bare = [u for u in re.split(r"[,\s]+", text or "") if u and not u.startswith("http")]
    urls += [("https://" + b.lstrip("/")) for b in bare if re.match(r"^[A-Za-z0-9\.\-]+\.[A-Za-z]{2,}$", b)]
    out, seen = [], set()
    for u in urls:
        uu = u.strip()
        if uu and uu not in seen:
            seen.add(uu); out.append(uu)
    return out

def build_site_args_from_context(ctx_ud: Dict) -> List[str]:
    args: List[str] = []
    urls: List[str] = ctx_ud.get("site_urls") or []
    if urls:
        args += ["--sites", ",".join(urls)]

    text_period: str = ctx_ud.get("site_period_text", "") or ""
    if re.fullmatch(r"\d{1,4}", text_period.strip()):
        args += ["--days", text_period.strip()]
    else:
        dates = re.findall(r"\b(\d{4}-\d{2}-\d{2})\b", text_period)
        if len(dates) >= 2:
            args += ["--start", dates[0], "--end", dates[1]]
        elif len(dates) == 1:
            args += ["--start", dates[0]]
        else:
            args += ["--days", str(DEFAULT_DAYS)]

    # берём элементы без даты тоже
    args += ["--accept-undated"]

    # вывод
    args += ["--out", "output"]

    # TLS trust store
    try:
        import certifi
        args += ["--cafile", certifi.where()]
    except Exception:
        pass
    return args

def read_all_sites_csv(out_dir: Path) -> List[Dict[str, str]]:
    target = out_dir / "all_sites.csv"
    rows: List[Dict[str, str]] = []
    if target.exists():
        with open(target, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({k: (r.get(k) or "") for k in ["date","title","link","summary","source"]})
    else:
        for p in out_dir.glob("*.csv"):
            if p.name == "all_sites.csv": continue
            with open(p, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows.append({k: (r.get(k) or "") for k in ["date","title","link","summary","source"]})
    return rows

def site_rows_to_posts(rows: List[Dict[str, str]]) -> List[dict]:
    def parse_dt(s: str) -> datetime:
        s = (s or "").strip()
        try:
            if s.endswith("Z"):
                return datetime.fromisoformat(s.replace("Z","+00:00"))
            return datetime.fromisoformat(s)
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    rows_sorted = sorted(rows, key=lambda r: parse_dt(r.get("date","")), reverse=True)
    posts = []
    for i, r in enumerate(rows_sorted, start=1):
        summary_html = first_paragraphs_html(r.get("summary",""), n=2)
        dt_disp = r.get("date","")
        try:
            if dt_disp:
                dt = parse_dt(dt_disp)
                if dt != datetime.min.replace(tzinfo=timezone.utc):
                    dt_disp = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            pass
        posts.append({
            "id": i,
            "date": dt_disp or "",
            "link": r.get("link",""),
            "title": r.get("title","").strip(),
            "source": re.sub(r"^https?://", "", (r.get("source","") or "")).strip("/"),
            "html": summary_html or ""
        })
    return posts

# ---------- Главное меню ----------
def main_menu_markup() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("📰 Парсинг сайтов", callback_data="menu:site")],
        [InlineKeyboardButton("📣 Парсинг тг каналов", callback_data="menu:tg")],
    ]
    return InlineKeyboardMarkup(kb)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Выбери режим работы:", reply_markup=main_menu_markup())
    return MENU

async def menu_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    choice = query.data
    if choice == "menu:tg":
        await query.edit_message_text(
            "Режим: Парсинг тг каналов.\n"
            "Скинь ссылку на канал (пример: https://t.me/fintechfutures или @fintechfutures)."
        )
        return LINK
    elif choice == "menu:site":
        await query.edit_message_text(
            "Режим: Парсинг сайтов.\n"
            "Пришли ссылки на сайты (через пробел/запятую), например:\n"
            "https://finextra.com https://techcrunch.com\n\n"
            "Можно также прислать .txt-файл (одна ссылка в строке)."
        )
        return SITE_SITES
    else:
        await query.edit_message_text("Неизвестный выбор. Используй /start.")
        return ConversationHandler.END

# ---------- ТГ-ветка ----------
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Окей, отменил. /start — вернуться в меню.")
    return ConversationHandler.END

async def parse_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Выбери режим:", reply_markup=main_menu_markup())
    return MENU

def period_human(start_dt: datetime, end_dt: datetime) -> str:
    return f"{start_dt.date()} — {(end_dt - timedelta(days=1)).date()}"

async def ask_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or "").strip()
    try:
        username = parse_channel_identifier(raw)
        context.user_data["channel_username"] = username
        await update.message.reply_text(
            "Период: напиши либо число дней (например, 30), либо даты:\n"
            "• '2025-08-01 2025-08-27' или 'с 2025-08-01 по 2025-08-27'\n"
            f"По умолчанию — последние {DEFAULT_DAYS} дней."
        )
        return PERIOD
    except ValueError as e:
        await update.message.reply_text(str(e))
        return LINK

async def ask_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    period_text = (update.message.text or "").strip()
    start_dt, end_dt = parse_period(period_text)
    context.user_data["period"] = (start_dt, end_dt)
    await update.message.reply_text(
        f"Ок! Период: {period_human(start_dt, end_dt)}\nТеперь пришли ключевые слова (через запятую). "
        "Если оставить пустым — соберу все посты за период."
    )
    return KEYWORDS

async def run_parse_tg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kw_raw = update.message.text or ""
    keywords = normalize_keywords(kw_raw)
    username = context.user_data["channel_username"]
    start_dt, end_dt = context.user_data["period"]

    await update.message.reply_text("Начинаю сбор… это может занять немного времени при больших каналах.")
    try:
        entity = await tg_client.get_entity(username)
        chan_username = getattr(entity, "username", None)
        chan_title = getattr(entity, "title", username)

        matched: List[dict] = []
        count = 0

        async for msg in tg_client.iter_messages(entity, offset_date=end_dt, reverse=False):
            count += 1
            if count > RESULTS_LIMIT: break
            if not isinstance(msg, Message): continue
            msg_dt = msg.date
            if msg_dt.tzinfo is None: msg_dt = msg_dt.replace(tzinfo=timezone.utc)
            if msg_dt >= end_dt: continue
            if msg_dt < start_dt: break

            text = message_text(msg)
            if not text: continue

            if match_keywords(text, keywords):
                snippet_html = first_paragraphs_html(text, n=2)
                if not snippet_html: continue
                matched.append({
                    "id": msg.id,
                    "date": msg_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                    "link": channel_permalink(chan_username, msg.id),
                    "html": snippet_html
                })

        html = render_html_tg(chan_title, period_human(start_dt, end_dt), chips=keywords, posts=matched)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"{safe_filename(chan_title)}__{start_dt.date()}_{(end_dt - timedelta(days=1)).date()}__{ts}.html"
        fpath = OUTPUT_DIR / fname
        fpath.write_text(html, encoding="utf-8")

        await update.message.reply_document(
            document=fpath.open("rb"),
            filename=fname,
            caption=f"Готово! Найдено постов: {len(matched)}\n/start — вернуться в меню."
        )

    except UsernameNotOccupiedError:
        await update.message.reply_text("Канал с таким username не найден.")
    except UsernameInvalidError:
        await update.message.reply_text("Некорректный username канала.")
    except ChannelPrivateError:
        await update.message.reply_text("Канал приватный. Ваш аккаунт (Telethon) должен быть участником канала.")
    except ChatAdminRequiredError:
        await update.message.reply_text("Нужны права администратора для доступа к истории этого канала.")
    except Exception as e:
        log.exception("Ошибка парсинга")
        await update.message.reply_text(f"Произошла ошибка: {e}")
    return ConversationHandler.END

# ---------- Ветка «Сайты» ----------
async def site_collect_sites(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    workdir = user_workdir(user_id)

    urls: List[str] = []
    if update.message.document:
        doc = update.message.document
        if not (doc.mime_type or "").startswith("text/") and not doc.file_name.lower().endswith(".txt"):
            await update.message.reply_text("Это не текстовый файл. Пришли .txt или напиши ссылки текстом.")
            return SITE_SITES
        tmp = workdir / f"sites_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        file = await doc.get_file()
        await file.download_to_drive(custom_path=str(tmp))
        try:
            with open(tmp, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s and not s.startswith("#"):
                        urls.extend(norm_urls_from_text(s))
        except Exception as e:
            await update.message.reply_text(f"Не смог прочитать файл: {e}")
            return SITE_SITES
    else:
        urls = norm_urls_from_text(update.message.text or "")

    urls = list(dict.fromkeys(urls))
    if not urls:
        await update.message.reply_text("Не нашёл ни одной ссылки. Пришли ещё раз ссылки или .txt-файл.")
        return SITE_SITES

    context.user_data["site_urls"] = urls
    preview = "\n".join(f"• {u}" for u in urls[:10])
    more = "" if len(urls) <= 10 else f"\n…и ещё {len(urls)-10}"
    await update.message.reply_text(
        f"Принял {len(urls)} сайт(ов):\n{preview}{more}\n\n"
        "Теперь укажи период (как для ТГ):\n"
        f"• число дней, например `30`\n"
        f"• или даты: `2025-08-01 2025-08-27`",
        parse_mode="Markdown"
    )
    return SITE_PERIOD

async def site_collect_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    context.user_data["site_period_text"] = text
    start_dt, end_dt = parse_period(text)
    summary = [
        "Проверь параметры 👇",
        f"• Источники: {len(context.user_data.get('site_urls', []))} сайт(ов)",
        f"• Период: {period_human(start_dt, end_dt)}",
        "Запустить парсинг?"
    ]
    await update.message.reply_text("\n".join(summary),
                                    reply_markup=InlineKeyboardMarkup([
                                        [InlineKeyboardButton("▶️ Запустить", callback_data="site:run")],
                                        [InlineKeyboardButton("❌ Отмена", callback_data="site:cancel")],
                                    ]))
    return SITE_CONFIRM

def sources_from_rows(rows: List[Dict[str, str]]) -> List[str]:
    chips = []
    for r in rows:
        src = (r.get("source","") or "")
        if src:
            src = re.sub(r"^https?://", "", src).strip("/")
            if src and src not in chips:
                chips.append(src)
    return chips

async def site_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "site:cancel":
        await query.edit_message_text("Отменил. /start — вернуться в меню.")
        return ConversationHandler.END
    if query.data != "site:run":
        await query.edit_message_text("Неверный выбор. /start — заново.")
        return ConversationHandler.END

    user_id = query.from_user.id
    workdir = user_workdir(user_id)
    args_list = build_site_args_from_context(context.user_data)

    await query.edit_message_text("Запускаю парсер сайтов… Это может занять немного времени.")

    rc, out, err = await run_site_script(args_list, workdir)
    out_dir = workdir / "output"
    rows = read_all_sites_csv(out_dir)

    start_dt, end_dt = parse_period(context.user_data.get("site_period_text",""))
    period_str = period_human(start_dt, end_dt)
    posts = site_rows_to_posts(rows)
    html = render_html_sites(period_str, sources_chips=sources_from_rows(rows), posts=posts)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"Sites__{start_dt.date()}_{(end_dt - timedelta(days=1)).date()}__{ts}.html"
    fpath = OUTPUT_DIR / fname
    fpath.write_text(html, encoding="utf-8")

    await query.message.reply_document(
        document=fpath.open("rb"),
        filename=fname,
        caption=f"Готово! Найдено материалов: {len(posts)} (rc={rc})\n/start — вернуться в меню."
    )
    return ConversationHandler.END

# ---------- LIFECYCLE ----------
async def on_start(app: Application):
    if not tg_client.is_connected():
        await tg_client.connect()
    if not await tg_client.is_user_authorized():
        raise RuntimeError("Telethon не авторизован. Пересоздайте TELETHON_SESSION через generate_session.py")

async def on_stop(app: Application):
    if tg_client.is_connected():
        await tg_client.disconnect()

def build_application() -> Application:
    try:
        import certifi
        from telegram.request import HTTPXRequest
        req = HTTPXRequest(http2=False, verify=certifi.where(), timeout=30.0, trust_env=True)
        log.info("HTTPXRequest с certifi включён.")
        return (
            Application.builder()
            .token(BOT_TOKEN)
            .request(req)
            .post_init(on_start)
            .post_shutdown(on_stop)
            .build()
        )
    except Exception as e:
        log.warning(f"Не удалось настроить HTTPXRequest с certifi: {e}. Использую дефолтные параметры.")
        return (
            Application.builder()
            .token(BOT_TOKEN)
            .post_init(on_start)
            .post_shutdown(on_stop)
            .build()
        )

def main():
    application = build_application()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start), CommandHandler("parse", parse_cmd)],
        states={
            MENU: [CallbackQueryHandler(menu_choice, pattern=r"^menu:(tg|site)$")],
            # ТГ
            LINK:     [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_period)],
            PERIOD:   [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_keywords)],
            KEYWORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, run_parse_tg)],
            # Сайты
            SITE_SITES:  [MessageHandler((filters.Document.ALL | (filters.TEXT & ~filters.COMMAND)), site_collect_sites)],
            SITE_PERIOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, site_collect_period)],
            SITE_CONFIRM:[CallbackQueryHandler(site_confirm, pattern=r"^site:(run|cancel)$")],
        },
        fallbacks=[CommandHandler("cancel", cancel), CommandHandler("start", start)],
        conversation_timeout=900,
    )

    application.add_handler(conv)
    application.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

