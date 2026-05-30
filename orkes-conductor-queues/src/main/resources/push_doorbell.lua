-- Atomic enqueue + doorbell ring for the standalone doorbell path, so a push of a due message is a
-- single round-trip instead of ZADD + LPUSH + LTRIM.
-- KEYS[1] = queue (sorted set)   KEYS[2] = doorbell (list)
-- ARGV[1] = ring flag ("1" to ring the doorbell, else skip)
-- ARGV[2..] = score, member, score, member, ...  (messages to add)
local zadd_args = {KEYS[1]}
for i = 2, #ARGV do
    zadd_args[#zadd_args + 1] = ARGV[i]
end
redis.call("ZADD", unpack(zadd_args))

if ARGV[1] == "1" then
    -- Keep the doorbell a single-token "there is work" flag (see RedisDoorbell).
    redis.call("LPUSH", KEYS[2], "1")
    redis.call("LTRIM", KEYS[2], 0, 0)
end

return 1
