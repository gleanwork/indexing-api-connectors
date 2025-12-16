#!/bin/bash

LOCAL_VENV="klue"

echo "vars: $@"
source "./__${LOCAL_VENV}__/bin/activate"
set -a; source .env; set +a && python3 -B crawl.py $@