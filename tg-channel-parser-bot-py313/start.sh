#!/usr/bin/env bash
set -euo pipefail
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo "Далее: cp .env.example .env -> заполните -> python generate_session.py -> python tg_channel_parser_bot.py"
