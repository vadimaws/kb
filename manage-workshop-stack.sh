#!/bin/bash

pip install -r requirements.txt

STACK_OPERATION=$1

if [[ "$STACK_OPERATION" == "create" || "$STACK_OPERATION" == "update" ]]; then
    # deploy / update workshop resources
    python3 create_kb.py
elif [ "$STACK_OPERATION" == "delete" ]; then
    # delete workshop resources
else
    echo "Invalid stack operation!"
    exit 1
fi
