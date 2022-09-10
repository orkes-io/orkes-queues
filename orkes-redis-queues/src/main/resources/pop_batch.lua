local message_queue = KEYS[1]
local timeout = ARGV[1]
local new_timeout = ARGV[2]
local batch_size = ARGV[3]

local msg_array = redis.call("ZRANGEBYSCORE", message_queue, 0, timeout, "WITHSCORES", "LIMIT", 0, batch_size)
if #msg_array == 0 then
    return nil
end

local j
j = 1
for i = 1, #msg_array/2 do
    local added = redis.call("ZADD", message_queue, "XX", "CH", new_timeout, msg_array[j])
    j = j + 2
end

return msg_array