#!/usr/bin/env bash
set -euo pipefail
python3 "$(dirname "$0")/../src/news_harvester.py" --days 30 --sites-file "$(dirname "$0")/../examples/sites.txt" --presets --verbose
