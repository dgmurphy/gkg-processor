#!/usr/bin/env bash

if pgrep --list-full python | grep 'process_gkgs.py'
then
  echo "GKG processor is already running."
else
  echo GKG processor is not running
  echo Starting GKG processor at: `date`
  source /home/parallels/dev/gkg-processor/venv/bin/activate
  python /home/parallels/dev/gkg-processor/src/process_gkgs.py
fi
