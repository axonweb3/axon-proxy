local current = redis.call('incrBy', KEYS[1], ARGV[1])
local pttl = redis.call('pttl', KEYS[1])
if pttl < 0 then
    redis.call('pexpire', KEYS[1], ARGV[2])
    pttl = ARGV[2]
end
return {current, pttl}
