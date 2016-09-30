#!/bin/bash

source ~/walker/config.sample.sh

PORT=$1

if [ -n $PORT ]; then
    sudo -E bash -c 'python app.py 80'
else
    python app.py 9009
fi
