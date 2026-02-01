#!/bin/bash
echo 'run mypy'
cd "$(dirname "$0")/../api" || exit 1
poetry run mypy .
