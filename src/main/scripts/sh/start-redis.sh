#!/bin/sh

REDIS_PASSWORD="password"
REDIS_ARGS="--requirepass $REDIS_PASSWORD --save 60 1 --loglevel verbose"

echo "=== Waiting for server startup ==="
redis-server $REDIS_ARGS &
redis_pid=$!

status=1
msg="Waiting for redis-server."
while [ $status -ne 0 ]
do
  echo $msg
  sleep 0.05
  redis-cli -a $REDIS_PASSWORD ping >/dev/null 2>/dev/null
  status=$?
  msg="$msg."
done

echo "=== Loading functions ==="
redis-cli -a $REDIS_PASSWORD -x FUNCTION LOAD REPLACE < /opt/redis-functions/redis_functions.lua
echo "=== Ready ==="
wait "$redis_pid"

