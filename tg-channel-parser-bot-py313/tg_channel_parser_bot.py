# tg_channel_parser_bot.py
# Бот принимает ссылку на канал, период и ключевые слова,
# парсит посты через Telethon и присылает HTML-файл пользователю.
# Теперь в HTML кладёт ТОЛЬКО ПЕРВЫЕ ДВА АБЗАЦА каждого поста.
# Совместим с Python 3.13 (без htmlmin).

import os
import re
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple, Optional
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

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ConversationHandler,
    ContextTypes, filters
)

# ----- ЛОГИ -----
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("parser-bot")

# ----- СОСТОЯНИЯ ДИАЛОГА -----
LINK, PERIOD, KEYWORDS = range(3)

# ----- КОНФИГ -----
load_dotenv()
BOT_TOKEN       = os.getenv("BOT_TOKEN")
API_ID          = int(os.getenv("API_ID", "0"))
API_HASH        = os.getenv("API_HASH")
SESSION_STRING  = os.getenv("TELETHON_SESSION")
DEFAULT_DAYS    = int(os.getenv("DEFAULT_DAYS", "30"))
RESULTS_LIMIT   = int(os.getenv("RESULTS_LIMIT", "5000"))
OUTPUT_DIR      = Path(os.getenv("OUTPUT_DIR", "output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

if not (BOT_TOKEN and API_ID and API_HASH and SESSION_STRING):
    raise SystemExit("ERROR: BOT_TOKEN, API_ID, API_HASH, TELETHON_SESSION обязательны в .env")

# Инициализируем Telethon-клиент (подключим в post_init)
tg_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ----- ШАБЛОН HTML -----
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

# ----- УТИЛИТЫ -----
def parse_channel_identifier(raw: str) -> str:
    raw = raw.strip()
    # Поддержка @username, t.me/username, https://t.me/username
    m = re.search(r"(?:t\.me/|@)([A-Za-z0-9_]{3,})/?$", raw)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_]{3,}", raw):
        return raw
    raise ValueError("Не удалось распознать ссылку/юзернейм. Пример: https://t.me/fintechfutures или @fintechfutures")

def parse_period(text: str) -> Tuple[datetime, datetime]:
    """
    Возвращает (start_utc, end_utc) для фильтра по датам.
    Поддержка:
      - "30" => последние 30 дней
      - "2025-07-01 2025-08-27"
      - "с 2025-07-01 по 2025-08-27" (вытащит 2 даты)
      - "2025-08-01" => с этой даты по сейчас
    """
    now = datetime.now(timezone.utc)
    s = (text or "").strip().lower()

    # только число => дни
    if re.fullmatch(r"\d{1,4}", s):
        days = int(s)
        start = now - timedelta(days=days)
        return start, now

    dates = re.findall(r"\b(\d{4}-\d{2}-\d{2})\b", s)
    if len(dates) >= 2:
        d1 = datetime.fromisoformat(dates[0]).replace(tzinfo=timezone.utc)
        d2 = datetime.fromisoformat(dates[1]).replace(tzinfo=timezone.utc)
        start, end = sorted([d1, d2])
        # делаем end инклюзивным: добавим день, чтобы легче сравнивать
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
    # Берём только текст сообщения
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
    """
    Берём только первые n абзацев. Абзац — блок текста, разделённый пустой строкой.
    Если пустых строк нет, берём первые n непустых строк как абзацы.
    Возвращаем HTML с <p>…</p>. Если текста нет — None.
    """
    # 1) Превратим возможный HTML в обычный текст
    plain = BeautifulSoup(raw_text or "", "html.parser").get_text()

    # 2) Нормализуем переводы строк
    t = plain.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t:
        return None

    # 3) Пытаемся сначала делить по "пустым строкам"
    paras = [p.strip() for p in re.split(r"\n\s*\n+", t) if p.strip()]

    # 4) Если абзацев мало — делим по строкам
    if len(paras) < n:
        lines = [ln.strip() for ln in t.split("\n") if ln.strip()]
        paras = lines[:n]
    else:
        paras = paras[:n]

    # 5) Экранируем и собираем HTML
    html_parts = []
    for p in paras:
        p_html = html_escape(p).replace("\n", "<br>")
        html_parts.append(f"<p>{p_html}</p>")
    return "".join(html_parts) if html_parts else None

def render_html(channel_name: str, period_str: str, keywords: List[str], posts: List[dict]) -> str:
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

# ----- HANDLERS -----
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я соберу посты из Telegram-канала по ключевым словам и сделаю HTML.\n\n"
        "Команда: /parse — запустить сбор.\n"
        "Отправь канал (ссылка/юзернейм), затем период, затем ключевые слова."
    )

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Окей, отменил.")
    return ConversationHandler.END

async def parse_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Скинь ссылку на канал (пример: https://t.me/fintechfutures или @fintechfutures).")
    return LINK

async def ask_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or "").strip()
    try:
        username = parse_channel_identifier(raw)
        context.user_data["channel_username"] = username
        await update.message.reply_text(
            "Период: напиши либо число дней (например, 30), либо даты:\n"
            "• '2025-08-01 2025-08-27' или 'с 2025-08-01 по 2025-08-27'\n"
        )
        return PERIOD
    except ValueError as e:
        await update.message.reply_text(str(e))
        return LINK

async def ask_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    period_text = (update.message.text or "").strip()
    start, end = parse_period(period_text)
    context.user_data["period"] = (start, end)
    human = f"{start.date()} — { (end - timedelta(days=1)).date() }"
    await update.message.reply_text(
        f"Ок! Период: {human}\n Чтобы получить все новости за заданный период - отправь мне запятую "," в ответ на это сообщение. 
Если хочешь сделать поиск по кодовым словам - направь мне кодовые слова через запятую."
    )
    return KEYWORDS

async def run_parse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kw_raw = update.message.text or ""
    keywords = normalize_keywords(kw_raw)
    username = context.user_data["channel_username"]
    start, end = context.user_data["period"]

    await update.message.reply_text("Начинаю сбор… это может занять немного времени при больших каналах.")

    try:
        entity = await tg_client.get_entity(username)
        chan_username = getattr(entity, "username", None)
        chan_title = getattr(entity, "title", username)

        matched: List[dict] = []
        count = 0

        # Идём от новых к старым, начиная от end (верхняя граница)
        async for msg in tg_client.iter_messages(entity, offset_date=end, reverse=False):
            count += 1
            if count > RESULTS_LIMIT:
                break
            if not isinstance(msg, Message):
                continue

            # Нормализуем дату к UTC
            msg_dt = msg.date
            if msg_dt.tzinfo is None:
                msg_dt = msg_dt.replace(tzinfo=timezone.utc)

            # Отсечения по периоду
            if msg_dt >= end:
                continue
            if msg_dt < start:
                break

            text = message_text(msg)
            if not text:
                continue

            if match_keywords(text, keywords):
                # ВАЖНО: берём только первые два абзаца
                snippet_html = first_paragraphs_html(text, n=2)
                if not snippet_html:
                    continue
                matched.append({
                    "id": msg.id,
                    "date": msg_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                    "link": channel_permalink(chan_username, msg.id),
                    "html": snippet_html
                })

        period_str = f"{start.date()} — {(end - timedelta(days=1)).date()}"
        html = render_html(chan_title, period_str, keywords, matched)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"{safe_filename(chan_title)}__{start.date()}_{(end - timedelta(days=1)).date()}__{ts}.html"
        fpath = OUTPUT_DIR / fname
        fpath.write_text(html, encoding="utf-8")

        await update.message.reply_document(
            document=fpath.open("rb"),
            filename=fname,
            caption=f"Готово! Найдено постов: {len(matched)}"
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

# ----- LIFECYCLE -----
async def on_start(app: Application):
    # Подключаем Telethon в общем event loop
    if not tg_client.is_connected():
        await tg_client.connect()
    if not await tg_client.is_user_authorized():
        raise RuntimeError("Telethon не авторизован. Пересоздайте TELETHON_SESSION через generate_session.py")

async def on_stop(app: Application):
    if tg_client.is_connected():
        await tg_client.disconnect()

def build_application() -> Application:
    """
    Строим Application для python-telegram-bot.
    Пытаемся использовать certifi для корректной TLS-валидации,
    но если модуль отсутствует — падаем обратно на дефолтные настройки.
    """
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
        entry_points=[CommandHandler("parse", parse_cmd)],
        states={
            LINK:     [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_period)],
            PERIOD:   [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_keywords)],
            KEYWORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, run_parse)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        conversation_timeout=600,
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv)

    application.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
