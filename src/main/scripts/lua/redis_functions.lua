#!lua name=event_functions

-- keys [event, clock]
-- args [time, durationMillis]
local function put_event(keys, args)
    if (#keys ~= 2) or (#args ~= 2) then
        return redis.error_reply("Incorrect # of keys keys [event, clockKey] or args [time, durationMillis].")
    end
    local event = keys[1]
    local clockKey = keys[2]
    local newTime = tonumber(args[1])
    local durationMillis = tonumber(args[2])

    if not (newTime and durationMillis) then
        -- tonumber returns nil on number conversion error
        return redis.error_reply("Unable to convert time or durationMillis to a number")
    end

    local function state_changed()
        redis.call("INCR", clockKey)
        redis.call("expire", event, math.ceil(2*durationMillis/1000))
        return
    end

    local eventStats = redis.call('HMGET', event, "time", "is_valid")
    -- older lua versions cannot unpack nil
    local curTime = tonumber(eventStats[1])
    local isValid = tonumber(eventStats[2])
    if (curTime == nil) or (curTime + durationMillis <= newTime) then
        redis.call("HMSET", event, "time", newTime, "is_valid", 1)
        state_changed()
        return {newTime, 1}
        --    nextTime in past but no conflict
    elseif (newTime < curTime) and (newTime + durationMillis <= curTime) then
        return {curTime, isValid}
        --    there was a conflict and we need to change db state
    elseif (newTime > curTime) or (isValid == 1) then
        local persistTime = math.max(curTime, newTime)
        redis.call("HMSET", event, "time", persistTime, "is_valid", 0 )
        state_changed()
        return {persistTime, 0}
    end
    -- no state change (new time is in past and no conflict)
    return {curTime, isValid}
end

redis.register_function("put_event", put_event)