#!/bin/bash

docker compose build && docker compose push && docker stack deploy --prune --compose-file ./docker-compose.yml transcoder

