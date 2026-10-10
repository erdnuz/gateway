package ledger

import (
	"context"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Mem is an in-process Ledger running the same algorithms as Redis, with
// the same arithmetic, on an injectable clock. The simulation and unit tests
// use it; the conformance test keeps it in step with the Lua.
type Mem struct {
	// Now is the ledger clock; nil means time.Now.
	Now func() time.Time

	mu      sync.Mutex
	vals    map[string]float64 // counters and GCRA TATs (whole ms)
	edges   map[string]map[string]int64
	deduped map[string]bool
}

// NewMem returns an empty in-memory ledger.
func NewMem() *Mem {
	return &Mem{vals: map[string]float64{}, edges: map[string]map[string]int64{}, deduped: map[string]bool{}}
}

func (m *Mem) now() int64 {
	if m.Now != nil {
		return m.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

// Used reports the cost granted so far in the window containing now (tests).
func (m *Mem) Used(ref Ref, spec Spec) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	win := windowMs(spec)
	return int64(m.vals[winKey(ref.Key(spec.Algo), m.now()/win)])
}

func (m *Mem) Grant(_ context.Context, ref Ref, spec Spec, args GrantArgs) (Grant, error) {
	in, err := prepare(ref, spec, args)
	if err != nil {
		return Grant{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()

	var avail, wid, reset, expires, tat, prev, cur int64
	var f float64
	var key string
	switch in.algo {
	case TokenBucket:
		T := 1000 / in.rate
		tau := float64(in.limit) * T
		key = in.key
		tat = m.getInt(key, now)
		if tat < now {
			tat = now
		}
		avail = int64(math.Floor(float64(float64(now)+tau-float64(tat)) / T))
		expires = in.ttl
	default:
		wid = now / in.win
		reset = (wid+1)*in.win - now
		expires = reset
		key = winKey(in.key, wid)
		if in.allocBase >= 0 {
			in.limit = in.allocBase + m.getInt(in.key+":alloc:"+strconv.FormatInt(wid, 10), 0)
		}
		cur = m.getInt(key, 0)
		used := cur
		if in.algo == SlidingWindow {
			prev = m.getInt(winKey(in.key, wid-1), 0)
			f = float64(now-wid*in.win) / float64(in.win)
			used += int64(math.Ceil(float64(float64(prev) * (1 - f))))
		}
		avail = in.limit - used
	}

	var pkey string
	var pwid, preset int64
	parentBinds := false
	if in.parentKey != "" {
		pwid = now / in.pwin
		pkey = winKey(in.parentKey, pwid)
		preset = (pwid+1)*in.pwin - now
		pavail := in.plimit - m.getInt(pkey, 0)
		if pavail < avail {
			avail, parentBinds = pavail, true
		}
		expires = min(expires, preset)
	}

	n := int64(1)
	if in.edge != "" {
		set := m.edges[in.key]
		if set == nil {
			set = map[string]int64{}
			m.edges[in.key] = set
		}
		set[in.edge] = now
		for e, at := range set {
			if at <= now-in.activeMs {
				delete(set, e)
			}
		}
		n = int64(len(set))
	}

	if avail <= 0 {
		var retry int64
		switch {
		case parentBinds:
			retry = min(max(in.pwin/10, 500), preset)
		case in.algo == TokenBucket:
			T := 1000 / in.rate
			retry = max(int64(math.Ceil(float64(tat)+T-float64(float64(in.limit)*T)-float64(now))), 1)
		case in.algo == SlidingWindow && prev > 0 && cur < in.limit:
			// Capacity returns continuously as the previous window's
			// weight decays: retry when one token has (at least 10 ms).
			fneed := 1 - float64(in.limit-1-cur)/float64(prev)
			retry = min(max(int64(math.Ceil(float64((fneed-f)*float64(in.win)))), 10), reset)
		default:
			retry = min(max(in.win/10, 500), reset)
		}
		if in.algo == TokenBucket {
			reset = tat - now
		}
		return in.result([]int64{0, 0, wid, expires, reset, retry, n, pwid}), nil
	}

	target := in.target
	if avail < n*target {
		target = (avail + n - 1) / n
	}
	grant := min(target, avail)

	if in.algo == TokenBucket {
		T := 1000 / in.rate
		tat = int64(math.Ceil(float64(tat) + float64(float64(grant)*T)))
		m.vals[key] = float64(tat)
		reset = tat - now
	} else {
		m.vals[key] += float64(grant)
	}
	if pkey != "" {
		m.vals[pkey] += float64(grant)
	}
	return in.result([]int64{grant, avail - grant, wid, expires, reset, 0, n, pwid}), nil
}

func (m *Mem) Return(_ context.Context, dedupe string, returns []Return) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for _, r := range returns {
		in, err := prepareReturn(r, dedupe)
		if err != nil {
			return err
		}
		if in.dedupeKey != "" {
			if m.deduped[in.dedupeKey] {
				continue
			}
			m.deduped[in.dedupeKey] = true
		}
		if in.algo == TokenBucket {
			T := 1000 / in.rate
			tat := m.getInt(in.key, now)
			if tat < now {
				tat = now
			}
			tat = int64(math.Ceil(float64(tat) - float64(float64(in.tokens)*T)))
			if tat < now {
				tat = now
			}
			m.vals[in.key] = float64(tat)
		} else if now/in.win == in.wid {
			m.credit(winKey(in.key, in.wid), in.tokens)
		}
		if in.parentKey != "" && now/in.pwin == in.pwid {
			m.credit(winKey(in.parentKey, in.pwid), in.tokens)
		}
	}
	return nil
}

// credit gives tokens back to a window counter, never below zero; negative
// tokens (debt) are added.
func (m *Mem) credit(key string, tokens int64) {
	used := m.getInt(key, 0)
	m.vals[key] = float64(used - min(tokens, used))
}

func (m *Mem) getInt(key string, def int64) int64 {
	if v, ok := m.vals[key]; ok {
		return int64(v)
	}
	return def
}

// ActiveEdges lists the edges counted for ref's endgame split (tests).
func (m *Mem) ActiveEdges(ref Ref, spec Spec) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for e := range m.edges[ref.Key(spec.Algo)] {
		out = append(out, e)
	}
	sort.Strings(out)
	return out
}
