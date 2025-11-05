#!/bin/zsh

source "./__coupa__/bin/activate"
set -a; source .env; set +a
python coupa.py $@
