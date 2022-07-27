#!/bin/bash

docker compose up -d
# TODO(pricilla): look for a more efficient way of waiting until the container is healthy
sleep 20
poetry run python -m example_writing_to_csv
docker compose down
