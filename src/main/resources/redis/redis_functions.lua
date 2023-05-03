#!lua name=event_functions

-- keys [eventKey, clockKey]
-- args [time, durationMillis]
-- returns: Table[Table[prevTime, prevIsValid], Table[curTime,curIsValid]]
-- note: for new keys [prevTime, prevIsValid] will be null and an empty table will be returned b/c of how lua handles null keys
local function put_event(keys, args)
    if (#keys ~= 2) or (#args ~= 2) then
        return redis.error_reply("Incorrect # of keys keys [event, clockKey] or args [time, durationMillis].")
    end
    local is_valid_hkey = "is_valid"
    local time_hkey = "time"
    local retired_hkey = "retired"

    local eventKey = keys[1]
    local clockKey = keys[2]

    local newTime = tonumber(args[1])
    local durationMillis = tonumber(args[2])

    if not (newTime and durationMillis) then
        -- tonumber returns nil on number conversion error
        return redis.error_reply("Unable to convert time or durationMillis to a number")
    end

    local curState = redis.call('HMGET', eventKey, time_hkey, is_valid_hkey)
    -- older lua versions cannot unpack nil
    local curTime = tonumber(curState[1])
    local isValid = tonumber(curState[2])
    local updatedRetired = tonumber(curState[3]) -- initialized as current state of updated retired, may mutate
    local updatedTime = nil
    local updatedIsValid = nil
    local updatedClock = nil

    local function state_changed()
        if isValid == 1 and updatedIsValid == 1 and curTime < updatedTime then
            updatedRetired = curTime;
        end
        if updatedRetired then
            redis.call("HMSET", eventKey, time_hkey, updatedTime, is_valid_hkey, updatedIsValid, retired_hkey, updatedRetired)
        else
            -- cannot call hmset with null hash value, and prefer not to use a sentinel value
            redis.call("HMSET", eventKey, time_hkey, updatedTime, is_valid_hkey, updatedIsValid)
        end
        redis.call("PEXPIRE", eventKey, 2*durationMillis)
        updatedClock = redis.call("INCR", clockKey)
        return
    end

    if (curTime == nil) or (curTime + durationMillis <= newTime) then
        updatedTime, updatedIsValid = newTime, 1
        --    nextTime in previous but no conflict
    elseif (newTime < curTime) and (newTime + durationMillis <= curTime) then
        updatedTime, updatedIsValid = curTime, isValid
        --    there was a conflict and we need to change db state
    elseif (newTime > curTime) or (isValid == 1) then
        updatedTime, updatedIsValid = math.max(curTime, newTime), 0
    else
        -- no state change (new time is in previous and no conflict)
        updatedTime, updatedIsValid = curTime, isValid
    end

    if (curTime ~= updatedTime or isValid ~= updatedIsValid) then
        -- lua doesn't natively support table equality
        state_changed(updatedTime, updatedIsValid)
    end

    local nextState = { updatedTime, updatedIsValid }
    curState = { curTime, isValid } -- converting to numeric type
    return { curState, nextState, updatedClock }  -- updatedClock nil unless state changed
end

redis.register_function("PUT_EVENT", put_event)

-- keys: {queueKey, clockKey} args: {thresholdTime}
-- return: {length queue (number)} or { EventTime (JSON dump), clock (number), length queue (number)}
local function poll_queue(keys, args)
    if (not #keys == 2 or not #args == 1) then
        redis.error_reply("Invalid keys or arguments.")
    end

    local queueKey = keys[1]
    local clockKey = keys[2]
    local thresholdTime = tonumber(args[1])
    local jsonTimeKey = 'time'

    local polled = redis.call("LINDEX", queueKey, 0)
    if not polled then
        return {0}
    end
    local polledTime = cjson.decode(polled)[jsonTimeKey]
    if polledTime == nil then
        redis.error_reply("Improperly formatted list element.")
    end
    if polledTime > thresholdTime then
        return {redis.call("LLEN", queueKey)}
    end
    local clock = redis.call("INCR", clockKey)
    local length = redis.call("LLEN", queueKey)
    redis.call("LPOP", queueKey)
    return { polled, clock, length }
end

redis.register_function("POLL_QUEUE", poll_queue)

-- keys: {queueKey, clockKey} args: {eventJson}
-- return: {clock (number) after offer, length queue (number) after offer}
local function offer_queue(keys, args)
    if (not #keys == 2 or #args == 1) then
        redis.error_reply("Invalid keys or arguments.")
    end

    local queueKey = keys[1]
    local clockKey = keys[2]
    local eventJson = args[1]

    local queueLength = redis.call("RPUSH", queueKey, eventJson)
    return {redis.call("INCR", clockKey), queueLength}
end

redis.register_function("OFFER_QUEUE", offer_queue)

-- keys: {queueKey, clockKey}, args: {startIndex, endIndex}
-- return: {elements (table of JSON serialized EventTime), clock (number) }
local function range_queue(keys, args)
    if (not #keys == 2 or #args == 2) then
        redis.error_reply("Invalid keys or arguments.")
    end

    local queueKey = keys[1]
    local clockKey = keys[2]
    local startIndex = tonumber(args[1])
    local endIndex = tonumber(args[2])

    if (startIndex == nil or endIndex == nil) then
        redis.error_reply("Could not convert arguments to numbers.")
    end

    local elements = redis.call("LRANGE", queueKey, startIndex, endIndex)
    local clock = redis.call("INCR", clockKey)

    return {elements, clock}
end

redis.register_function("RANGE_QUEUE", range_queue)