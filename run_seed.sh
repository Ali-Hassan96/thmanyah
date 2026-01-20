#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate
python seed_loop.py
