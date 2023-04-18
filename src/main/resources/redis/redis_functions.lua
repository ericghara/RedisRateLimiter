#!lua name=event_functions

-- keys [event, keyPrefix]
-- args [time, durationMillis]
-- returns: Table[Table[prevTime, prevIsValid], Table[curTime,curIsValid]]
-- note: for new keys [prevTime, prevIsValid] will be null and an empty table will be returned b/c of how lua handles null keys
local function put_event(keys, args)
    if (#keys ~= 2) or (#args ~= 2) then
        return redis.error_reply("Incorrect # of keys keys [event, clockKey] or args [time, durationMillis].")
    end
    local CLOCK_ELEMENT = "CLOCK"
    local EVENT_ELEMENT = "EVENT"
    local is_valid_hkey = "is_valid"
    local time_hkey = "time"

    local event = keys[1]
    local prefix = keys[2]

    local newTime = tonumber(args[1])
    local durationMillis = tonumber(args[2])

    if not (newTime and durationMillis) then
        -- tonumber returns nil on number conversion error
        return redis.error_reply("Unable to convert time or durationMillis to a number")
    end

    local function encodeKey(elements)
        local DELIMITER = ":"
        return table.concat(elements, DELIMITER);
    end

    local eventKey = encodeKey({ prefix, EVENT_ELEMENT, event })

    local function state_changed(updatedTime, updatedIsValid)
        redis.call("HMSET", eventKey, time_hkey, updatedTime, is_valid_hkey, updatedIsValid)
        redis.call("expire", eventKey, math.ceil(2 * durationMillis / 1000))
        redis.call("INCR", encodeKey({ prefix, CLOCK_ELEMENT }))
        return
    end

    local curState = redis.call('HMGET', eventKey, time_hkey, is_valid_hkey)
    -- older lua versions cannot unpack nil
    local curTime = tonumber(curState[1])
    local isValid = tonumber(curState[2])
    local updatedTime = nil
    local updatedIsValid = nil
    if (curTime == nil) or (curTime + durationMillis <= newTime) then
        updatedTime, updatedIsValid = newTime, 1
        --    nextTime in past but no conflict
    elseif (newTime < curTime) and (newTime + durationMillis <= curTime) then
        updatedTime, updatedIsValid = curTime, isValid
        --    there was a conflict and we need to change db state
    elseif (newTime > curTime) or (isValid == 1) then
        updatedTime, updatedIsValid = math.max(curTime, newTime), 0
    else
        -- no state change (new time is in past and no conflict)
        updatedTime, updatedIsValid = curTime, isValid
    end

    if (curTime ~= updatedTime or isValid ~= updatedIsValid) then -- lua doesn't natively support table equality
        state_changed(updatedTime, updatedIsValid)
    end

    local nextState = {updatedTime, updatedIsValid}
    curState = {curTime, isValid} -- converting to numeric type
    return { curState, nextState }
end

redis.register_function("put_event", put_event)