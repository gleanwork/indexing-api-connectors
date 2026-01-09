#!/bin/zsh

if [[ "$PROMPT" == *"__slab__"* ]]; then
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
