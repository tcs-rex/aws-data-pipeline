#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 <program-name>"
    exit 1
fi

program_name="$1"

pids=$(ps aux | grep "$program_name" | grep -v grep | awk '{print $2}')

if [ -z "$pids" ]; then
    echo "No processes found for $program_name"
else
    for pid in $pids; do
        echo "Killing PID $pid for $program_name"
        kill -TERM $pid 
    done
fi
