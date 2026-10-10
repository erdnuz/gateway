package region

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"

	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"
	"gateway/packages/ledger"

	"google.golang.org/protobuf/proto"
)

// Peer is another region's hub.
type Peer interface {
	Borrow(ctx context.Context, in *types.BorrowRequest) (*types.BorrowResponse, error)
}

// Need is a quota this region ran out of in one window.
type Need struct {
	Ref     ledger.Ref
	Spec    ledger.Spec // Limit is the global quota
	Window  int64
	Request *types.BorrowRequest // how peers name the quota
}

// Node borrows allocation from peer regions for quotas this region ran out
// of. Each hub replica rebalances the quotas it saw run out; replicas may
// borrow for the same quota at once, which costs nothing in safety (the
// lenders debit first) and at most some extra borrowing.
type Node struct {
	Name     string
	Ledger   ledger.Allocator
	Peers    map[string]Peer
	Interval time.Duration // how often to rebalance (default 250ms)

	mu    sync.Mutex
	needs map[string]Need
	turn  int
}

const borrowTimeout = time.Second

// Starved records that a quota ran out here; the next rebalance borrows for
// it.
func (n *Node) Starved(need Need) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.needs == nil {
		n.needs = map[string]Need{}
	}
	n.needs[need.Ref.Key(need.Spec.Algo)] = need
}

// Run rebalances until ctx ends.
func (n *Node) Run(ctx context.Context) {
	every := n.Interval
	if every <= 0 {
		every = 250 * time.Millisecond
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.Rebalance(ctx)
		}
	}
}

// Rebalance makes one round: for each quota that ran out in the current
// window, ask peers (in rotating order) until one lends.
func (n *Node) Rebalance(ctx context.Context) {
	n.mu.Lock()
	needs := n.needs
	n.needs = nil
	peers := Names(peerSet(n.Peers))
	n.turn++
	turn := n.turn
	n.mu.Unlock()
	if len(peers) == 0 {
		return
	}
	for _, need := range needs {
		if ledger.WindowOf(need.Spec, time.Now()) != need.Window {
			continue // the window has ended: nothing left to borrow for
		}
		// A quarter of the quota per round: a region that has taken over the
		// demand catches up within a few rounds; lenders keep what they need.
		want := max(need.Spec.Limit/4, 1)
		for i := range peers {
			name := peers[(turn+i)%len(peers)]
			if n.borrow(ctx, name, need, want) {
				break
			}
		}
	}
}

// borrow asks one peer and credits what it lends. It reports whether the
// peer lent anything.
func (n *Node) borrow(ctx context.Context, peer string, need Need, want int64) bool {
	req := proto.Clone(need.Request).(*types.BorrowRequest)
	req.WindowId, req.Want, req.TransferId, req.FromRegion = need.Window, want, transferID(), n.Name
	bctx, cancel := context.WithTimeout(ctx, borrowTimeout)
	resp, err := n.Peers[peer].Borrow(bctx, req)
	cancel()
	if err != nil {
		telemetry.RegionTransfers.WithLabelValues(peer, "unreachable").Inc()
		return false
	}
	if resp.GetGiven() <= 0 {
		telemetry.RegionTransfers.WithLabelValues(peer, "refused").Inc()
		return false
	}
	ok, err := n.Ledger.Credit(ctx, need.Ref, need.Spec, need.Window, resp.GetGiven(), req.TransferId)
	switch {
	case err != nil:
		// The lender has already debited itself: the tokens are lost for
		// this window (under-use), never double counted.
		slog.Warn("crediting a borrowed allocation failed; it is lost for this window", "peer", peer, "err", err)
		telemetry.RegionTransfers.WithLabelValues(peer, "lost").Inc()
	case !ok:
		telemetry.RegionTransfers.WithLabelValues(peer, "late").Inc()
	default:
		telemetry.RegionTransfers.WithLabelValues(peer, "credited").Inc()
		telemetry.RegionTokensBorrowed.Add(float64(resp.GetGiven()))
	}
	return true
}

func peerSet(p map[string]Peer) map[string]float64 {
	out := make(map[string]float64, len(p))
	for name := range p {
		out[name] = 0
	}
	return out
}

func transferID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
