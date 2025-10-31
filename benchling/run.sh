#!/bin/zsh

echo "vars: $@"
source "./__benchling__/bin/activate"
set -a; source .env; set +a && python3 -B crawl.py $@
