-- Atomic enqueue + doorbell ring for the standalone doorbell path, so a push of due messages is a
-- single round-trip instead of ZADD + LPUSH + LTRIM.
-- KEYS[1] = queue (sorted set)   KEYS[2] = doorbell (list)
-- ARGV[1] = ring flag ("1" to ring the doorbell, else skip)
-- ARGV[2..] = score, member, score, member, ...  (messages to add; may be empty)
--
-- The score/member pairs are added in CHUNKed multi-member ZADD calls: a single unpack() of a huge
-- argument list overflows Lua's stack (LUAI_MAXCSTACK) once a push batch gets large (a few
-- thousand), so we cap each ZADD's argument count regardless of how many messages are pushed.
local CHUNK = 500
local i = 2
local n = #ARGV
while i <= n do
    local zadd_args = {KEYS[1]}
    local added = 0
    while i + 1 <= n and added < CHUNK do
        zadd_args[#zadd_args + 1] = ARGV[i] -- score
        zadd_args[#zadd_args + 1] = ARGV[i + 1] -- member
        i = i + 2
        added = added + 1
    end
    -- Only call ZADD if we actually collected at least one member (guards an empty push).
    if #zadd_args > 1 then
        redis.call("ZADD", unpack(zadd_args))
    end
end

if ARGV[1] == "1" then
    -- Keep the doorbell a single-token "there is work" flag (see RedisDoorbell).
    redis.call("LPUSH", KEYS[2], "1")
    redis.call("LTRIM", KEYS[2], 0, 0)
end

return 1
