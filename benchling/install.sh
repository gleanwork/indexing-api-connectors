#!/bin/zsh

if [[ "$PROMPT" == *"__benchling__"* ]]; then
    # Upgrade pip and install Glean Indexing API Client
    pip install -U pip
    pip install https://app.glean.com/meta/indexing_api_client.zip
    pip install -r requirements.txt
else
    echo
    echo "Your python environment is not set."
    echo "See init.sh"
    echo
    exit
fi
