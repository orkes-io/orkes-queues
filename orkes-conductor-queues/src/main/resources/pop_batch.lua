local message_queue = KEYS[1]
local timeout = ARGV[1]
local new_timeout = ARGV[2]
local batch_size = ARGV[3]

local msg_array = redis.call("ZRANGEBYSCORE", message_queue, 0, timeout, "WITHSCORES", "LIMIT", 0, batch_size)
if #msg_array == 0 then
    return nil
end

-- Rescore every claimed member invisible (now + unack) in a SINGLE multi-member ZADD rather than
-- one ZADD per message. ZRANGEBYSCORE ... WITHSCORES returns [member, score, member, score, ...],
-- so members are at the odd indices. batch_size is bounded (<= MAX_POLL_COUNT), keeping the unpack
-- well within Lua's stack limit.
local zadd_args = {message_queue, "XX", "CH"}
for i = 1, #msg_array, 2 do
    zadd_args[#zadd_args + 1] = new_timeout
    zadd_args[#zadd_args + 1] = msg_array[i]
end
redis.call("ZADD", unpack(zadd_args))

return msg_array
