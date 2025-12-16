#!/bin/zsh

LOCAL_VENV="klue"

# Build and activate localized python environment
mkdir -p __${LOCAL_VENV}__
/usr/bin/python3 -m venv __${LOCAL_VENV}__

source "./__${LOCAL_VENV}__/bin/activate"

echo
echo "RUN THIS COMMAND:"
echo "source ./__${LOCAL_VENV}__/bin/activate"
echo
echo "Then run ./install.sh"
echo