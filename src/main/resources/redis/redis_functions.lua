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

    local function state_changed()
        if isValid == 1 and updatedIsValid == 1 and curTime < updatedTime then
            updatedRetired = curTime;
        end
        if updatedRetired then
            redis.call("HMSET", eventKey, time_hkey, updatedTime, is_valid_hkey, updatedIsValid, retired_hkey, updatedRetired)
        else
            -- cannot call hmset with null hash value, prefer not to use sentinel
            redis.call("HMSET", eventKey, time_hkey, updatedTime, is_valid_hkey, updatedIsValid)
        end
        redis.call("EXPIRE", eventKey, math.ceil(2 * durationMillis / 1000))
        redis.call("INCR", clockKey)
        return
    end

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

    if (curTime ~= updatedTime or isValid ~= updatedIsValid) then
        -- lua doesn't natively support table equality
        state_changed(updatedTime, updatedIsValid)
    end

    local nextState = { updatedTime, updatedIsValid }
    curState = { curTime, isValid } -- converting to numeric type
    return { curState, nextState }
end

redis.register_function("PUT_EVENT", put_event)