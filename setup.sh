#!/bin/bash

virtualenv -p python3 ../venv
source ../venv/bin/activate
pip install --upgrade pip
pip install --upgrade wheel
pip install --no-cache-dir -r requirements.txt

