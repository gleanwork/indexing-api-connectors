#!/usr/bin/env bash
set -e

ENV_DIR="__uploadshortcuts__"
PYTHON="/usr/bin/python3"

if [ ! -d "$ENV_DIR" ]; then
    $PYTHON -m venv $ENV_DIR
    echo "Virtual environment created at ./$ENV_DIR"
fi;

source ./$ENV_DIR/bin/activate
echo "Virtual environment activated"

pip install -U pip
pip install https://app.glean.com/meta/indexing_api_client.zip
pip install -r requirements.txt
echo "Requirements sucessfully installed"
