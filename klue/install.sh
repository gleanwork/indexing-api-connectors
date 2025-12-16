#!/bin/zsh

LOCAL_VENV="klue"

if [[ "$PROMPT" == *"__${LOCAL_VENV}__"* ]]; then
    # Upgrade pip and install Glean Indexing API Client
    pip install -U pip
    pip install -r requirements.txt
else
    echo
    echo "Your python environment is not set."
    echo "See init.sh"
    echo
    exit
fi
