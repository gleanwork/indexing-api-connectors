#!/bin/zsh

# Build and activate localized python environment
mkdir -p __onetrust__ 
/usr/bin/python3 -m venv __onetrust__

source "./__onetrust__/bin/activate"

echo
echo "RUN THIS COMMAND:"
echo "source ./__onetrust__/bin/activate"
echo
echo "Then run ./install.sh"
echo