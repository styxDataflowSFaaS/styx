#!/bin/bash

set -m

python worker/worker_service.py # &

#exec redis-server /usr/local/etc/redis/redis.conf