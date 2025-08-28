# Telegram Channel Parser Bot (совместим с Python 3.13)

Бот принимает ссылку на канал, период и ключевые слова, собирает подходящие посты через **Telethon** и присылает **HTML**.

## Установка
```bash
cd tg-channel-parser-bot-py313
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```
Далее:
```bash
cp .env.example .env  # заполните BOT_TOKEN, API_ID, API_HASH
python generate_session.py  # получите TELETHON_SESSION и вставьте в .env
python tg_channel_parser_bot.py
```

> Примечание: Минификация HTML отключена (была несовместимая библиотека `htmlmin`).
