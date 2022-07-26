#!/bin/bash

docker compose up -d
# TODO(pricilla): look for a more efficient way of waiting until the container is healthy
sleep 15
poetry run python -m import_data && docker compose down
