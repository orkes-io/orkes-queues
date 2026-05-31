local message_queue = KEYS[1]
local timeout = ARGV[1]
local new_timeout = ARGV[2]
local batch_size = ARGV[3]

local msg_array = redis.call("ZRANGEBYSCORE", message_queue, 0, timeout, "WITHSCORES", "LIMIT", 0, batch_size)
if #msg_array == 0 then
    return nil
end

-- Rescore every claimed member invisible (now + unack). ZRANGEBYSCORE ... WITHSCORES returns
-- [member, score, member, score, ...], so members are at the odd indices. We batch the rescore into
-- multi-member ZADD calls (far fewer command dispatches than one ZADD per message) but CHUNK them:
-- unpack() of a single huge argument list overflows Lua's stack (LUAI_MAXCSTACK) once a claim batch
-- gets large (a few thousand), which the old one-ZADD-per-message loop never hit. CHUNK keeps each
-- ZADD's argument count bounded regardless of batch_size.
local CHUNK = 500
local i = 1
local n = #msg_array
while i <= n do
    local zadd_args = {message_queue, "XX", "CH"}
    -- add up to CHUNK members (each contributes a score+member pair to the ZADD)
    local added = 0
    while i <= n and added < CHUNK do
        zadd_args[#zadd_args + 1] = new_timeout
        zadd_args[#zadd_args + 1] = msg_array[i]
        i = i + 2
        added = added + 1
    end
    redis.call("ZADD", unpack(zadd_args))
end

return msg_array
