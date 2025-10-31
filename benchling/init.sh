#!/bin/zsh

# Build and activate localized python environment
mkdir -p __benchling__ 
/usr/bin/python3 -m venv __benchling__

source ./__benchling__/bin/activate

echo
echo "RUN THIS COMMAND:"
echo "source ./__benchling__/bin/activate"
echo
echo "Then run ./install.sh"
echo