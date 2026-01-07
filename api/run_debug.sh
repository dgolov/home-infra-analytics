#!/bin/bash
echo 'Starting infra analytics api'
sleep 0.1
uvicorn main:app --reload --port 8000