#!/usr/bin/env bash
set -e

ENV_DIR="__db_hackathon_venv__"

if [ ! -d "$ENV_DIR" ]; then
    python -m venv $ENV_DIR
    echo "Virtual environment created at ./$ENV_DIR"
fi;

source ./$ENV_DIR/bin/activate
echo "Virtual environment activated"

pip install -U pip
pip install https://app.glean.com/meta/indexing_api_client.zip
pip install -r requirements.txt
echo "Requirements successfully installed"
