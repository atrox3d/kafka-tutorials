#!/usr/bin/env bash

deactivate
source .venv/bin/activate
fastapi dev src/fastapi_producer_consumer/main.py
