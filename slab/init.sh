#!/bin/zsh

# Build and activate localized python environment
mkdir -p __slab__ 
/usr/bin/python3 -m venv __slab__

source ./__slab__/bin/activate

echo
echo "RUN THIS COMMAND:"
echo "source ./__slab__/bin/activate"
echo
echo "Then run ./install.sh"
echo