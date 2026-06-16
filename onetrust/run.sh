#!/bin/zsh

echo "vars: $@"
source "./__onetrust__/bin/activate"
set -a; source .env; set +a
python -u onetrust.py