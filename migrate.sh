#!/bin/bash
# change-job-status.sh
# Usage: ./change-job-status.sh [redis-host] [redis-port] [redis-db]

REDIS_HOST="${1:-127.0.0.1}"
REDIS_PORT="${2:-6379}"
REDIS_DB="${3:-1}"

echo "Connecting to Redis at $REDIS_HOST:$REDIS_PORT DB $REDIS_DB"
echo "Updating job statuses: COMPLETED -> DONE, ABORTED -> FAILED"

# Process COMPLETED -> DONE
for key in $(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" --raw SCAN 0 MATCH "job:*" COUNT 1000 | grep '^job:'); do
    status=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" --raw HGET "$key" status)
    if [[ "$status" == "COMPLETED" ]]; then
        echo "Updating $key: COMPLETED -> DONE"
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" HSET "$key" status "DONE" > /dev/null
    elif [[ "$status" == "ABORTED" ]]; then
        echo "Updating $key: ABORTED -> FAILED"
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" HSET "$key" status "FAILED" > /dev/null
    fi
done

echo "Done."
