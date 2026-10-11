package ledger

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// grantLua mirrors Mem.Grant line for line. Window keys are derived inside
// the script from the ledger's clock (TIME); they share KEYS[1]'s hash tag,
// so they live on the same Cluster slot.
//
// KEYS: base[, parent base]
// ARGV: algo, limit, window_ms, rate, target, edge, active_ms, ttl_ms, parent_limit, parent_window_ms, alloc_base
var grantLua = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local algo = ARGV[1]
local limit = tonumber(ARGV[2])
local win = tonumber(ARGV[3])
local rate = tonumber(ARGV[4])
local target = tonumber(ARGV[5])
local edge = ARGV[6]
local active_ms = tonumber(ARGV[7])
local ttl = tonumber(ARGV[8])

local avail, wid, reset, expires, tat, key = 0, 0, 0, 0, 0, KEYS[1]
local prev, cur, f = 0, 0, 0
if algo == 'token_bucket' then
  local T = 1000 / rate
  local tau = limit * T
  tat = tonumber(redis.call('GET', key) or now)
  if tat < now then tat = now end
  avail = math.floor((now + tau - tat) / T)
  expires = ttl
else
  wid = math.floor(now / win)
  reset = (wid + 1) * win - now
  expires = reset
  key = KEYS[1] .. ':' .. wid
  if tonumber(ARGV[11]) >= 0 then
    limit = tonumber(ARGV[11]) + tonumber(redis.call('GET', KEYS[1] .. ':alloc:' .. wid) or 0)
  end
  cur = tonumber(redis.call('GET', key) or 0)
  local used = cur
  if algo == 'sliding_window' then
    prev = tonumber(redis.call('GET', KEYS[1] .. ':' .. (wid - 1)) or 0)
    f = (now - wid * win) / win
    used = used + math.ceil(prev * (1 - f))
  end
  avail = limit - used
end

local pkey, pwid, preset, pwin = nil, 0, 0, 0
local parent_binds = false
if #KEYS > 1 then
  local plimit = tonumber(ARGV[9])
  pwin = tonumber(ARGV[10])
  pwid = math.floor(now / pwin)
  pkey = KEYS[2] .. ':' .. pwid
  preset = (pwid + 1) * pwin - now
  local pavail = plimit - tonumber(redis.call('GET', pkey) or 0)
  if pavail < avail then
    avail = pavail
    parent_binds = true
  end
  if preset < expires then expires = preset end
end

-- Near the endgame (less than 64 grants left), edges register and are
-- counted so the rest can be split between them. Far from it, a grant
-- skips the bookkeeping: every edge that keeps asking registers long
-- before the split applies.
local n = 1
if edge ~= '' and avail < 64 * target then
  local ekey = KEYS[1] .. ':edges'
  redis.call('ZADD', ekey, now, edge)
  redis.call('PEXPIRE', ekey, active_ms)
  -- Count edges seen within the active window; stale members go with the
  -- key when the quota goes quiet.
  n = redis.call('ZCOUNT', ekey, '(' .. (now - active_ms), '+inf')
end

if avail <= 0 then
  local retry
  if parent_binds then
    retry = math.min(math.max(math.floor(pwin / 10), 500), preset)
  elseif algo == 'token_bucket' then
    local T = 1000 / rate
    retry = math.max(math.ceil(tat + T - limit * T - now), 1)
  elseif algo == 'sliding_window' and prev > 0 and cur < limit then
    local fneed = 1 - (limit - 1 - cur) / prev
    retry = math.min(math.max(math.ceil((fneed - f) * win), 10), reset)
  else
    retry = math.min(math.max(math.floor(win / 10), 500), reset)
  end
  if algo == 'token_bucket' then reset = tat - now end
  return {0, 0, wid, expires, reset, retry, n, pwid}
end

if avail < n * target then
  target = math.floor((avail + n - 1) / n)
end
local grant = math.min(target, avail)

if algo == 'token_bucket' then
  local T = 1000 / rate
  tat = math.ceil(tat + grant * T)
  redis.call('SET', key, string.format('%d', tat), 'PX', tat - now + 1000)
  reset = tat - now
else
  if redis.call('INCRBY', key, grant) == grant then
    redis.call('PEXPIRE', key, 2 * win + 1000)
  end
end
if pkey then
  redis.call('INCRBY', pkey, grant)
  redis.call('PEXPIRE', pkey, 2 * pwin + 1000)
end
return {grant, avail - grant, wid, expires, reset, 0, n, pwid}
`)

// returnLua mirrors Mem.Return for one item.
//
// KEYS: base[, parent base]
// ARGV: algo, window_ms, rate, window_id, tokens, parent_window_ms, parent_window_id, dedupe_key, dedupe_ttl_ms
var returnLua = redis.NewScript(`
if ARGV[8] ~= '' then
  if not redis.call('SET', ARGV[8], '1', 'NX', 'PX', ARGV[9]) then return 0 end
end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local algo = ARGV[1]
local win = tonumber(ARGV[2])
local tokens = tonumber(ARGV[5])

local function credit(key, ttl)
  local used = tonumber(redis.call('GET', key) or 0)
  local c = tokens
  if c > used then c = used end
  if c ~= 0 then
    redis.call('DECRBY', key, c)
    redis.call('PEXPIRE', key, ttl)
  end
end

if algo == 'token_bucket' then
  local T = 1000 / tonumber(ARGV[3])
  local tat = tonumber(redis.call('GET', KEYS[1]) or now)
  if tat < now then tat = now end
  tat = math.ceil(tat - tokens * T)
  if tat < now then tat = now end
  redis.call('SET', KEYS[1], string.format('%d', tat), 'PX', tat - now + 1000)
elseif math.floor(now / win) == tonumber(ARGV[4]) then
  credit(KEYS[1] .. ':' .. ARGV[4], 2 * win + 1000)
end
if #KEYS > 1 then
  local pwin = tonumber(ARGV[6])
  if math.floor(now / pwin) == tonumber(ARGV[7]) then
    credit(KEYS[2] .. ':' .. ARGV[7], 2 * pwin + 1000)
  end
end
return 1
`)

// Redis is the production Ledger. It works with a single Redis or a Redis
// Cluster: every script touches one hash tag.
type Redis struct {
	rdb redis.UniversalClient
}

// NewRedis returns a Ledger over rdb.
func NewRedis(rdb redis.UniversalClient) *Redis { return &Redis{rdb: rdb} }

func (in grantInput) keys() []string {
	if in.parentKey != "" {
		return []string{in.key, in.parentKey}
	}
	return []string{in.key}
}

func (in grantInput) argv() []any {
	return []any{
		string(in.algo), in.limit, in.win, strconv.FormatFloat(in.rate, 'g', -1, 64),
		in.target, in.edge, in.activeMs, in.ttl, in.plimit, in.pwin, in.allocBase,
	}
}

func (r *Redis) Grant(ctx context.Context, ref Ref, spec Spec, args GrantArgs) (Grant, error) {
	in, err := prepare(ref, spec, args)
	if err != nil {
		return Grant{}, err
	}
	res, err := grantLua.Run(ctx, r.rdb, in.keys(), in.argv()...).Int64Slice()
	if err != nil {
		return Grant{}, err
	}
	if len(res) < 8 {
		return Grant{}, fmt.Errorf("ledger: malformed grant reply %v", res)
	}
	return in.result(res), nil
}

// GrantBatch runs many grants in one pipeline (one round trip per shard).
// Errors are reported per item.
func (r *Redis) GrantBatch(ctx context.Context, items []BatchItem) []BatchResult {
	out := make([]BatchResult, len(items))
	ins := make([]grantInput, len(items))
	cmds := make([]*redis.Cmd, len(items))
	pipe := r.rdb.Pipeline()
	for i, it := range items {
		in, err := prepare(it.Ref, it.Spec, it.Args)
		if err != nil {
			out[i].Err = err
			continue
		}
		ins[i] = in
		cmds[i] = grantLua.EvalSha(ctx, pipe, in.keys(), in.argv()...)
	}
	_, _ = pipe.Exec(ctx) // errors are read per command below
	for i, c := range cmds {
		// A shard that has not cached the script yet answers NOSCRIPT;
		// Run loads it there.
		if c != nil && c.Err() != nil && redis.HasErrorPrefix(c.Err(), "NOSCRIPT") {
			cmds[i] = grantLua.Run(ctx, r.rdb, ins[i].keys(), ins[i].argv()...)
		}
	}
	for i, c := range cmds {
		if c == nil {
			continue
		}
		res, err := c.Int64Slice()
		switch {
		case err != nil:
			out[i].Err = err
		case len(res) < 8:
			out[i].Err = fmt.Errorf("ledger: malformed grant reply %v", res)
		default:
			out[i].Grant = ins[i].result(res)
		}
	}
	return out
}

func (r *Redis) Return(ctx context.Context, dedupe string, returns []Return) error {
	for _, ret := range returns {
		in, err := prepareReturn(ret, dedupe)
		if err != nil {
			return err
		}
		keys := []string{in.key}
		if in.parentKey != "" {
			keys = append(keys, in.parentKey)
		}
		err = returnLua.Run(ctx, r.rdb, keys,
			string(in.algo), in.win, strconv.FormatFloat(in.rate, 'g', -1, 64), in.wid, in.tokens,
			in.pwin, in.pwid, in.dedupeKey, in.dedupeTTL).Err()
		if err != nil {
			return err
		}
	}
	return nil
}
