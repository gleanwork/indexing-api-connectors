#!/bin/zsh

# Build and activate localized python environment
mkdir -p __coupa__ 
/usr/bin/python3 -m venv __coupa__

source "./__coupa__/bin/activate"

echo
echo "RUN THIS COMMAND:"
echo "source ./__coupa__/bin/activate"
echo
echo "Then run ./install.sh"
echo