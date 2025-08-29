# tg_channel_parser_bot.py
# Меню из двух режимов:
# 1) «Парсинг тг каналов» — читает канал через Telethon, фильтрует по датам и ключам и делает HTML (первые 2 абзаца поста).
# 2) «Парсинг сайтов» — ВСТРОЕННЫЙ парсер, теперь с пошаговым, дружелюбным диалогом:
#     - спрашиваем, использовать пресеты или свой список сайтов;
#     - (если свой) просим ссылки (текстом или .txt-файлом);
#     - спрашиваем период (как у ТГ: число дней или даты);
#     - показываем резюме и кнопки «Запустить» / «Отмена».
#
# Совместим с Python 3.13. Для Bot API используем certifi, чтобы избежать TLS-ошибок.
#
# Требуется .env с переменными:
#   BOT_TOKEN=...
#   API_ID=...
#   API_HASH=...
#   TELETHON_SESSION=...
#
# Зависимости (requirements):
#   python-telegram-bot==21.6
#   telethon==1.36.0
#   python-dotenv==1.0.1
#   jinja2==3.1.4
#   beautifulsoup4==4.12.3
#   certifi>=2024.0.0

import os
import re
import sys
import shlex
import asyncio
import logging
import zipfile
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
(
    MENU,                 # главное меню
    LINK, PERIOD, KEYWORDS,          # ветка ТГ
    SITE_MODE, SITE_SITES, SITE_PERIOD, SITE_CONFIRM  # ветка сайты
) = range(8)

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

# ---------- Telethon клиент (инициализируем в post_init PTB) ----------
tg_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ---------- ВСТРОЕННЫЙ ПАРСЕР САЙТОВ (код) ----------
EMBEDDED_SITE_PARSER_NAME = "embedded_site_parser.py"
EMBEDDED_SITE_PARSER_CODE = r'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NewsScraper v4 — stdlib-only (RSS/Atom/sitemaps) с фильтром по датам.
"""

import os
import re
import csv
import ssl
import time
from datetime import datetime, timezone, timedelta
import urllib.parse as urlparse
import urllib.request as urlrequest
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple

DEFAULT_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

SSL_CONTEXT = None  # set in run()

def http_get(url: str, timeout: int = 20, headers: Optional[Dict[str, str]] = None) -> bytes:
    req = urlrequest.Request(url, headers=headers or {"User-Agent": DEFAULT_UA})
    with urlrequest.urlopen(req, timeout=timeout, context=SSL_CONTEXT) as resp:
        return resp.read()

def to_iso(dt):
    try:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""

def parse_date_guess(s: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return parsedate_to_datetime(s)
    except Exception:
        pass
    for pat in [
        r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?Z?$",
        r"^\d{4}/\d{2}/\d{2}$",
        r"^\d{1,2}\s+\w+\s+\d{4}",
    ]:
        if re.match(pat, s):
            try:
                s2 = s.replace("/", "-").replace("T", " ").replace("Z", "")
                if len(s2) == 10:
                    s2 += " 00:00"
                return datetime.fromisoformat(s2)
            except Exception:
                pass
    return None

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def parse_feed_xml(xml_bytes: bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []
    items = []
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        pub_date = it.findtext("pubDate") or it.findtext("date") or it.findtext("{http://purl.org/dc/elements/1.1/}date")
        desc = it.findtext("description") or ""
        items.append((title, link, pub_date, desc))
    for it in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        title = (it.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
        link_el = it.find("{http://www.w3.org/2005/Atom}link")
        link = link_el.get("href") if link_el is not None else ""
        pub_date = (it.findtext("{http://www.w3.org/2005/Atom}updated")
                    or it.findtext("{http://www.w3.org/2005/Atom}published"))
        desc = it.findtext("{http://www.w3.org/2005/Atom}summary") or ""
        items.append((title, link, pub_date, desc))
    return items

def discover_feeds(html: str, base_url: str):
    feeds = set()
    for m in re.finditer(r'<link[^>]+rel=["\'](?:alternate|feed)["\'][^>]+>', html, flags=re.I):
        tag = m.group(0)
        href_m = re.search(r'href=["\']([^"\']+)["\']', tag, flags=re.I)
        type_m = re.search(r'type=["\']([^"\']+)["\']', tag, flags=re.I)
        if href_m:
            href = href_m.group(1)
            if not href.startswith("http"):
                href = urlparse.urljoin(base_url, href)
            if not type_m or re.search(r'(rss|atom|xml)', type_m.group(1), flags=re.I):
                feeds.add(href)
    for suffix in ["/feed", "/rss", "/rss.xml", "/atom.xml", "/feed.xml"]:
        feeds.add(urlparse.urljoin(base_url, suffix))
    return list(feeds)

def get_sitemap_links(html: str, base_url: str):
    try:
        robots_url = urlparse.urljoin(base_url, "/robots.txt")
        rb = http_get(robots_url)
        import re as _re
        links = _re.findall(r"(?im)^sitemap:\s*(\S+)$", rb.decode("utf-8", "ignore"))
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

def collect_site(site_url: str, end_dt, start_dt, throttle=0.6, accept_undated=False, verbose=False, max_items=1000):
    import urllib.parse as urlparse
    import time
    collected = []
    reason = ""
    try:
        try:
            hb = http_get(site_url, timeout=20)
            html = hb.decode("utf-8", "ignore")
        except Exception as e:
            return [], f"get_homepage_error:{e}"
        feeds = discover_feeds(html, site_url)
        if verbose:
            print(f"[feeds] {site_url} -> {len(feeds)} candidates")
        for f in feeds:
            try:
                fb = http_get(f, timeout=20)
                items = parse_feed_xml(fb)
                for (title, link, pub_date, desc) in items:
                    dt = parse_date_guess(pub_date)
                    if not dt and accept_undated:
                        dt = end_dt
                    if not dt or not (start_dt <= dt <= end_dt):
                        continue
                    if not link.startswith("http"):
                        link = urlparse.urljoin(site_url, link)
                    collected.append({
                        "title": title.strip(),
                        "link": link.strip(),
                        "date": to_iso(dt),
                        "summary": (desc or "").strip(),
                        "source": site_url
                    })
            except Exception as e:
                if verbose:
                    print(f"[feed_error] {f}: {e}")
        if not collected:
            for sm in get_sitemap_links(html, site_url):
                try:
                    sb = http_get(sm, timeout=20)
                    urls = parse_sitemap(sb)
                    for u, lastmod in urls:
                        dt = parse_date_guess(lastmod)
                        if not dt and accept_undated:
                            dt = end_dt
                        if not dt or not (start_dt <= dt <= end_dt):
                            continue
                        collected.append({
                            "title": "",
                            "link": u.strip(),
                            "date": to_iso(dt),
                            "summary": "",
                            "source": site_url
                        })
                except Exception as e:
                    if verbose:
                        print(f"[sitemap_error] {sm}: {e}")
        seen = set()
        deduped = []
        for it in collected:
            if it["link"] in seen:
                continue
            seen.add(it["link"])
            deduped.append(it)
        if len(deduped) > max_items:
            deduped = deduped[:max_items]
        return deduped, reason
    finally:
        time.sleep(throttle)

def write_csv(rows: List[Dict[str, str]], path: str):
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","title","link","summary","source"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

def write_txt(rows: List[Dict[str, str]], path: str):
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(f"{r['date']} | {r['title']} | {r['link']} | {r['source']}\n")

def parse_date_or_default(s: Optional[str], default):
    if not s:
        return default
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        return parsedate_to_datetime(s)
    except Exception:
        return default

def load_sites_from_file(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                out.append(s)
    return out

def presets_feeds() -> List[str]:
    return [
        "https://www.finextra.com/rss/latestnews.aspx",
        "https://techcrunch.com/category/fintech/feed/",
        "https://www.pymnts.com/feed/",
        "https://thepaypers.com/rss",
    ]

def run(sites: List[str], days: Optional[int], start: Optional[str], end: Optional[str],
        throttle: float = 0.6, accept_undated=False, max_items=1000, verbose=False,
        add_presets=False, cafile: Optional[str] = None, insecure=False,
        out_dir: str = "output"):
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

    if add_presets:
        sites = (sites or []) + presets_feeds()
    sites_norm = []
    for s in (sites or []):
        s = s.strip()
        if not s:
            continue
        if not s.startswith("http"):
            s = "https://" + s.lstrip("/")
        sites_norm.append(s)

    all_rows = []
    per_site_stats = []
    for s in sites_norm:
        rows, reason = collect_site(s, end_dt=end_dt, start_dt=start_dt,
                                    throttle=throttle, accept_undated=accept_undated,
                                    verbose=verbose, max_items=max_items)
        per_site_stats.append((s, len(rows), reason))
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", urlparse.urlparse(s).netloc or "site")
        write_csv(rows, os.path.join(out_dir, f"{safe}.csv"))
        all_rows.extend(rows)

    write_csv(all_rows, os.path.join(out_dir, "all_sites.csv"))
    write_txt(all_rows, os.path.join(out_dir, "all_sites.txt"))

# CLI (не используется ботом, но оставлено для совместимости)
def parse_args(argv=None):
    import argparse
    p = argparse.ArgumentParser(description="Fetch news via RSS/Atom/sitemaps.")
    p.add_argument("--sites")
    p.add_argument("--days", type=int)
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--throttle", type=float, default=0.6)
    p.add_argument("--accept-undated", action="store_true")
    p.add_argument("--max-items", type=int, default=1000)
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--presets", action="store_true")
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--cafile")
    p.add_argument("--out", default="output")
    return p.parse_args(argv)

def main(argv=None):
    args = parse_args(argv)
    sites = []
    if args.sites:
        sites = [s.strip() for s in args.sites.split(",") if s.strip()]
    run(
        sites=sites,
        days=args.days,
        start=args.start,
        end=args.end,
        throttle=args.throttle,
        accept_undated=args.accept_undated,
        max_items=args.max_items,
        verbose=args.verbose,
        add_presets=args.presets,
        cafile=args.cafile,
        insecure=args.insecure,
        out_dir=args.out
    )

if __name__ == "__main__":
    main()
'''

# ---------- УТИЛИТЫ (общие) ----------
def parse_channel_identifier(raw: str) -> str:
    raw = raw.strip()
    m = re.search(r"(?:t\.me/|@)([A-Za-z0-9_]{3,})/?$", raw)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_]{3,}", raw):
        return raw
    raise ValueError("Не удалось распознать ссылку/юзернейм. Пример: https://t.me/fintechfutures или @fintechfutures")

def parse_period(text: str) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    s = (text or "").strip().lower()
    if re.fullmatch(r"\d{1,4}", s):
        days = int(s)
        start = now - timedelta(days=days)
        return start, now
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
        start = now - timedelta(days=DEFAULT_DAYS)
        return start, now

def normalize_keywords(text: str) -> List[str]:
    parts = [p.strip().lower() for p in re.split(r"[,;\n]", text or "") if p.strip()]
    seen, out = set(), []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

def message_text(msg: Message) -> str:
    return msg.message or ""

def match_keywords(text: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    t = text.lower()
    return any(k in t for k in keywords)

def channel_permalink(username: Optional[str], msg_id: int) -> Optional[str]:
    if username:
        return f"https://t.me/{username}/{msg_id}"
    return None

def first_paragraphs_html(raw_text: str, n: int = 2) -> Optional[str]:
    plain = BeautifulSoup(raw_text or "", "html.parser").get_text()
    t = plain.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t:
        return None
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

def render_html(channel_name: str, period_str: str, keywords: List[str], posts: List[dict]) -> str:
    HTML_TEMPLATE = Template("""
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
      <div class="muted">Период: {{ period_str }} • Найдено: {{ total }} • Слова:
        {% for k in keywords %}<span class="pill">{{ k }}</span>{% endfor %}
      </div>
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
    return HTML_TEMPLATE.render(
        title=f"{channel_name} — подборка",
        channel_name=channel_name,
        period_str=period_str,
        keywords=keywords,
        total=len(posts),
        posts=posts
    )

def safe_filename(s: str) -> str:
    s = re.sub(r"[^\w\-\.\s]", "_", s, flags=re.UNICODE).strip()
    return re.sub(r"\s+", "_", s)

# ---------- УТИЛИТЫ (режим «Парсинг сайтов») ----------
def user_workdir(user_id: int) -> Path:
    d = WORK_ROOT / f"user_{user_id}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "output").mkdir(parents=True, exist_ok=True)
    return d

def list_files_recursive(root: Path) -> Set[Path]:
    files = set()
    for p in root.rglob("*"):
        if p.is_file():
            files.add(p.resolve())
    return files

def ensure_embedded_script_on_disk(workdir: Path) -> Path:
    script_path = workdir / EMBEDDED_SITE_PARSER_NAME
    script_path.write_text(EMBEDDED_SITE_PARSER_CODE, encoding="utf-8")
    return script_path

async def run_site_script(args_list: List[str], workdir: Path, timeout_sec: int = 1200) -> Tuple[int, str, str]:
    script_path = ensure_embedded_script_on_disk(workdir)
    cmd = [sys.executable, str(script_path)] + args_list
    log.info("Running embedded site parser: %s", " ".join(shlex.quote(c) for c in cmd))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(workdir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        proc.kill()
        return 124, "", "Timeout while running embedded site parser"
    return proc.returncode, stdout_b.decode("utf-8", errors="ignore"), stderr_b.decode("utf-8", errors="ignore")

def zip_many(files: List[Path], zip_path: Path) -> Path:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        base = zip_path.parent
        for f in files:
            try:
                arc = f.relative_to(base)
            except ValueError:
                arc = f.name
            zf.write(f, arcname=str(arc))
    return zip_path

def norm_urls_from_text(text: str) -> List[str]:
    urls = re.findall(r'https?://[^\s,]+', text or "", flags=re.I)
    # если пользователь ввёл домены без схемы — добавим https://
    bare = [u for u in re.split(r"[,\s]+", text or "") if u and not u.startswith("http")]
    urls += [("https://" + b.lstrip("/")) for b in bare if re.match(r"^[A-Za-z0-9\.\-]+\.[A-Za-z]{2,}$", b)]
    # нормализуем/уникализируем
    out, seen = [], set()
    for u in urls:
        uu = u.strip()
        if uu and uu not in seen:
            seen.add(uu)
            out.append(uu)
    return out

def site_mode_keyboard() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("🧰 Использовать пресеты", callback_data="site:presets")],
        [InlineKeyboardButton("✍️ Указать свои сайты", callback_data="site:custom")],
    ]
    return InlineKeyboardMarkup(kb)

def site_confirm_keyboard() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("▶️ Запустить", callback_data="site:run")],
        [InlineKeyboardButton("❌ Отмена", callback_data="site:cancel")],
    ]
    return InlineKeyboardMarkup(kb)

def build_site_args_from_context(ctx_ud: Dict) -> List[str]:
    args: List[str] = []
    # presets?
    if ctx_ud.get("site_use_presets"):
        args.append("--presets")
    # sites?
    urls: List[str] = ctx_ud.get("site_urls") or []
    if urls:
        args += ["--sites", ",".join(urls)]
    # period
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
    # misc defaults
    args += ["--out", "output", "--verbose", "--max-items", "1000"]
    # TLS
    try:
        import certifi
        args += ["--cafile", certifi.where()]
    except Exception:
        pass
    return args

# ---------- HANDLERS: главное меню ----------
def main_menu_markup() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("📰 Парсинг сайтов", callback_data="menu:site")],
        [InlineKeyboardButton("📣 Парсинг тг каналов", callback_data="menu:tg")],
    ]
    return InlineKeyboardMarkup(kb)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Выбери режим работы:",
        reply_markup=main_menu_markup()
    )
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
        context.user_data.pop("site_use_presets", None)
        context.user_data.pop("site_urls", None)
        context.user_data.pop("site_period_text", None)
        await query.edit_message_text(
            "Режим: Парсинг сайтов.\nВыбери источник сайтов:",
            reply_markup=site_mode_keyboard()
        )
        return SITE_MODE
    else:
        await query.edit_message_text("Неизвестный выбор. Используй /start.")
        return ConversationHandler.END

# ---------- HANDLERS: ветка ТГ ----------
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Окей, отменил. /start — вернуться в меню.")
    return ConversationHandler.END

async def parse_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Выбери режим:", reply_markup=main_menu_markup())
    return MENU

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
    human = f"{start_dt.date()} — { (end_dt - timedelta(days=1)).date() }"
    await update.message.reply_text(
        f"Ок! Период: {human}\nТеперь пришли ключевые слова (через запятую). "
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
            if count > RESULTS_LIMIT:
                break
            if not isinstance(msg, Message):
                continue
            msg_dt = msg.date
            if msg_dt.tzinfo is None:
                msg_dt = msg_dt.replace(tzinfo=timezone.utc)
            if msg_dt >= end_dt:
                continue
            if msg_dt < start_dt:
                break

            text = message_text(msg)
            if not text:
                continue

            if match_keywords(text, keywords):
                snippet_html = first_paragraphs_html(text, n=2)
                if not snippet_html:
                    continue
                matched.append({
                    "id": msg.id,
                    "date": msg_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                    "link": channel_permalink(chan_username, msg.id),
                    "html": snippet_html
                })

        period_str = f"{start_dt.date()} — {(end_dt - timedelta(days=1)).date()}"
        html = render_html(chan_title, period_str, keywords, matched)

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

# ---------- HANDLERS: ветка «Сайты» ----------
async def site_mode_choose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора: пресеты или свой список сайтов."""
    query = update.callback_query
    await query.answer()
    if query.data == "site:presets":
        context.user_data["site_use_presets"] = True
        context.user_data["site_urls"] = []
        await query.edit_message_text(
            "Ок! Используем пресеты 🔧\n"
            "Теперь укажи период (как для ТГ):\n"
            f"• число дней, например `30`\n"
            f"• или даты: `2025-08-01 2025-08-27`\n"
            f"Если оставить пусто — возьму по умолчанию {DEFAULT_DAYS} дней.",
            parse_mode="Markdown"
        )
        return SITE_PERIOD
    elif query.data == "site:custom":
        context.user_data["site_use_presets"] = False
        await query.edit_message_text(
            "Пришли сайты:\n"
            "• просто ссылками через пробел/запятую (например: https://finextra.com https://techcrunch.com)\n"
            "• или загрузи `.txt`-файл (одна ссылка в строке).\n\n"
            "После этого попрошу период.",
        )
        return SITE_SITES
    else:
        await query.edit_message_text("Неверный выбор. /start — чтобы начать заново.")
        return ConversationHandler.END

async def site_collect_sites(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принимаем список сайтов текстом или .txt документом."""
    user_id = update.message.from_user.id
    workdir = user_workdir(user_id)

    urls: List[str] = []

    # Если документ
    if update.message.document:
        doc = update.message.document
        if not (doc.mime_type or "").startswith("text/") and not doc.file_name.lower().endswith(".txt"):
            await update.message.reply_text("Это не текстовый файл. Пришли .txt или напиши ссылки текстом.")
            return SITE_SITES
        tmp_path = workdir / f"sites_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        file = await doc.get_file()
        await file.download_to_drive(custom_path=str(tmp_path))
        try:
            with open(tmp_path, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s and not s.startswith("#"):
                        urls.extend(norm_urls_from_text(s))
        except Exception as e:
            await update.message.reply_text(f"Не смог прочитать файл: {e}")
            return SITE_SITES
    else:
        # Текстом
        urls = norm_urls_from_text(update.message.text or "")

    # Уникализируем
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
    """Принимаем период и показываем подтверждение."""
    text = (update.message.text or "").strip()
    context.user_data["site_period_text"] = text
    start_dt, end_dt = parse_period(text)
    human = f"{start_dt.date()} — {(end_dt - timedelta(days=1)).date()}"
    urls = context.user_data.get("site_urls") or []
    use_presets = bool(context.user_data.get("site_use_presets"))

    summary = [
        "Проверь параметры 👇",
        f"• Источники: {'пресеты' if use_presets else (str(len(urls)) + ' сайт(ов)')}",
        f"• Период: {human}",
        "Запустить парсинг?"
    ]
    await update.message.reply_text("\n".join(summary), reply_markup=site_confirm_keyboard())
    return SITE_CONFIRM

async def site_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Стартуем парсер или отменяем."""
    query = update.callback_query
    await query.answer()
    if query.data == "site:cancel":
        await query.edit_message_text("Отменил. /start — вернуться в меню.")
        return ConversationHandler.END
    if query.data != "site:run":
        await query.edit_message_text("Неверный выбор. /start — заново.")
        return ConversationHandler.END

    # RUN
    user_id = query.from_user.id
    workdir = user_workdir(user_id)
    args_list = build_site_args_from_context(context.user_data)

    await query.edit_message_text("Запускаю парсер сайтов… Это может занять немного времени.")
    before = list_files_recursive(workdir)
    rc, out, err = await run_site_script(args_list, workdir)
    after = list_files_recursive(workdir)

    new_files = sorted([p for p in after - before if p.is_file()],
                       key=lambda p: p.stat().st_mtime)

    if new_files:
        useful = [p for p in new_files if p.name.endswith((".csv", ".txt", ".html", ".json"))]
        if not useful:
            useful = new_files
        total_size = sum(p.stat().st_size for p in useful)
        if len(useful) > 8 or total_size > 45 * 1024 * 1024:
            zip_path = workdir / f"site_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            zip_many(useful, zip_path)
            await query.message.reply_document(document=zip_path.open("rb"), filename=zip_path.name,
                                               caption=f"Готово! (rc={rc})\n/start — вернуться в меню.")
        else:
            for f in useful:
                try:
                    await query.message.reply_document(document=f.open("rb"), filename=f.name)
                except Exception as e:
                    await query.message.reply_text(f"Не удалось отправить {f.name}: {e}")
            await query.message.reply_text(f"Готово! (rc={rc})\n/start — вернуться в меню.")
    else:
        text = ""
        if out:
            text += f"stdout:\n{out[:3500]}"
            if len(out) > 3500:
                text += "\n…(обрезано)…\n"
        if err:
            text += f"\nstderr:\n{err[:3500]}"
            if len(err) > 3500:
                text += "\n…(обрезано)…\n"
        if not text:
            text = f"Скрипт завершился без вывода и без файлов. rc={rc}"
        await query.message.reply_text(text + "\n/start — вернуться в меню.")

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
    """Создаём PTB Application с HTTPXRequest(verify=certifi.where()) для стабильного TLS."""
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
            MENU: [
                CallbackQueryHandler(menu_choice, pattern=r"^menu:(tg|site)$"),
            ],
            # ТГ-ветка
            LINK:     [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_period)],
            PERIOD:   [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_keywords)],
            KEYWORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, run_parse_tg)],
            # Сайты-ветка
            SITE_MODE:   [CallbackQueryHandler(site_mode_choose, pattern=r"^site:(presets|custom)$")],
            SITE_SITES:  [
                MessageHandler((filters.Document.ALL | (filters.TEXT & ~filters.COMMAND)), site_collect_sites)
            ],
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
