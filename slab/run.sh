#!/bin/zsh

echo "vars: $@"
source "./__slab__/bin/activate"
set -a; source .env; set +a && python3 -B crawl.py $@
