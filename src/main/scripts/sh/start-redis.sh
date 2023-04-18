#!/bin/sh
# run these commands
echo "=== Loading functions ==="
redis-server --requirepass password --save 60 1 --loglevel warning --daemonize yes
cat /opt/redis-functions/redis_functions.lua | redis-cli -a password -x FUNCTION LOAD REPLACE
redis-cli -a password shutdown
echo "=== Rebooting redis ==="
redis-server --requirepass password --save 60 1 --loglevel warning