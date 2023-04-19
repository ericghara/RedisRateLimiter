#!/bin/sh
REDIS_ARGS="--requirepass password --save 60 1 --loglevel warning"
echo "=== Loading functions ==="
#redis-server --requirepass password --save 60 1 --loglevel warning  --daemonize yes
/entrypoint.sh &
sleep 0.5
cat /opt/redis-functions/redis_functions.lua | redis-cli -a password -x FUNCTION LOAD REPLACE
redis-cli -a password shutdown
echo "=== Rebooting redis ==="
REDIS_ARGS="--requirepass password --save 60 1 --loglevel warning"
/entrypoint.sh