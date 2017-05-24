#!/bin/bash
SOURCE_FILE="${BASH_SOURCE[0]}"
SOURCE_DIR=$(dirname $SOURCE_FILE)

cd $SOURCE_DIR
python oracle_log.py
