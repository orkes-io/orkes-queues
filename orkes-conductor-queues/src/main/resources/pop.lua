local message_queue = KEYS[1]
local timeout = ARGV[1]
local new_timeout = ARGV[2]

local msg_array = redis.call("ZRANGEBYSCORE", message_queue, 0, timeout, "LIMIT", 0, 1)
local msg = msg_array[1]
if msg == nil  or  #msg_array == 0 then
    return nil
end

local added = redis.call("ZADD", message_queue, "XX", "CH", new_timeout, msg)
if added == 0 then
    return nil
end

return msg