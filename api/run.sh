#!/bin/bash
echo 'Starting infra analytics api'
sleep 0.1
gunicorn main:app \
  -k uvicorn.workers.UvicornWorker \
  -w $(nproc) \
  --bind 0.0.0.0:8000