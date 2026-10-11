package ledger

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Regions: a global quota Q is split between regions, each enforcing its
// allocation in its own ledger (GrantArgs.Regional). Allocations move
// between regions by debit-first transfers: the lender lowers its own
// allocation before the borrower raises its own, and a transfer for a window
// that has ended is dropped. Whatever is lost on the way (a lost reply, a
// borrower that has moved on to the next window) is under-use, never
// overshoot: the allocations never sum to more than Q.

// Allocator is a ledger that can lend and credit regional allocation.
type Allocator interface {
	// Lend takes up to want from this region's unused allocation for ref in
	// window wid, keeping back margin or what the region is on pace to use
	// for the rest of the window (whichever is more), and returns how much
	// it gave. A
	// retried call with the same id returns the first answer.
	Lend(ctx context.Context, ref Ref, spec Spec, base, wid, want, margin int64, id string) (int64, error)
	// Credit adds amount to this region's allocation for window wid, once
	// per id. It reports false when the window has ended or id was seen.
	Credit(ctx context.Context, ref Ref, spec Spec, wid, amount int64, id string) (bool, error)
}

var lendLua = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local win = tonumber(ARGV[1])
local wid = tonumber(ARGV[2])
if math.floor(now / win) ~= wid then return 0 end
local idk = KEYS[1] .. ':lend:' .. ARGV[6]
local prev = redis.call('GET', idk)
if prev then return tonumber(prev) end
local ak = KEYS[1] .. ':alloc:' .. ARGV[2]
local alloc = tonumber(ARGV[3]) + tonumber(redis.call('GET', ak) or 0)
local used = tonumber(redis.call('GET', KEYS[1] .. ':' .. ARGV[2]) or 0)
-- Keep back what this region is on pace to use for the rest of the window.
local elapsed = now - wid * win
local margin = math.max(tonumber(ARGV[5]), math.ceil(used * (win - elapsed) / math.max(elapsed, 1)))
local give = math.min(tonumber(ARGV[4]), alloc - used - margin)
if give < 0 then give = 0 end
if give > 0 then
  redis.call('INCRBY', ak, -give)
  redis.call('PEXPIRE', ak, 2 * win + 1000)
end
redis.call('SET', idk, give, 'PX', 2 * win + 1000)
return give
`)

var creditLua = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local win = tonumber(ARGV[1])
if math.floor(now / win) ~= tonumber(ARGV[2]) then return 0 end
if not redis.call('SET', KEYS[1] .. ':credit:' .. ARGV[4], '1', 'NX', 'PX', 2 * win + 1000) then return 0 end
local ak = KEYS[1] .. ':alloc:' .. ARGV[2]
redis.call('INCRBY', ak, tonumber(ARGV[3]))
redis.call('PEXPIRE', ak, 2 * win + 1000)
return 1
`)

func (r *Redis) Lend(ctx context.Context, ref Ref, spec Spec, base, wid, want, margin int64, id string) (int64, error) {
	if spec.Algo.Or() != FixedWindow {
		return 0, ErrBadSpec
	}
	return lendLua.Run(ctx, r.rdb, []string{ref.Key(spec.Algo)}, windowMs(spec), wid, base, want, margin, id).Int64()
}

func (r *Redis) Credit(ctx context.Context, ref Ref, spec Spec, wid, amount int64, id string) (bool, error) {
	if spec.Algo.Or() != FixedWindow {
		return false, ErrBadSpec
	}
	n, err := creditLua.Run(ctx, r.rdb, []string{ref.Key(spec.Algo)}, windowMs(spec), wid, amount, id).Int64()
	return n == 1, err
}

func (m *Mem) Lend(_ context.Context, ref Ref, spec Spec, base, wid, want, margin int64, id string) (int64, error) {
	if spec.Algo.Or() != FixedWindow {
		return 0, ErrBadSpec
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	win := windowMs(spec)
	if m.now()/win != wid {
		return 0, nil
	}
	key := ref.Key(spec.Algo)
	idk := key + ":lend:" + id
	if v, ok := m.vals[idk]; ok {
		return int64(v), nil
	}
	ak := key + ":alloc:" + strconv.FormatInt(wid, 10)
	alloc := base + m.getInt(ak, 0)
	used := m.getInt(winKey(key, wid), 0)
	// Keep back what this region is on pace to use for the rest of the window.
	elapsed := m.now() - wid*win
	margin = max(margin, int64(math.Ceil(float64(used*(win-elapsed))/float64(max(elapsed, 1)))))
	give := max(min(want, alloc-used-margin), 0)
	m.vals[ak] -= float64(give)
	m.vals[idk] = float64(give)
	return give, nil
}

func (m *Mem) Credit(_ context.Context, ref Ref, spec Spec, wid, amount int64, id string) (bool, error) {
	if spec.Algo.Or() != FixedWindow {
		return false, ErrBadSpec
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.now()/windowMs(spec) != wid {
		return false, nil
	}
	key := ref.Key(spec.Algo)
	if m.deduped[key+":credit:"+id] {
		return false, nil
	}
	m.deduped[key+":credit:"+id] = true
	m.vals[key+":alloc:"+strconv.FormatInt(wid, 10)] += float64(amount)
	return true, nil
}

// Allocation reports this region's allocation adjustment for a window
// (what peers lent it minus what it lent them), for tests.
func (m *Mem) Allocation(ref Ref, spec Spec, wid int64) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getInt(ref.Key(spec.Algo)+":alloc:"+strconv.FormatInt(wid, 10), 0)
}

// WindowOf is the window containing t for spec (fixed windows).
func WindowOf(spec Spec, t time.Time) int64 { return t.UnixMilli() / windowMs(spec) }
