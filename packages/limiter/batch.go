package limiter

import (
	"context"
	"sync"
	"sync/atomic"

	"gateway/packages/common/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BatchLeaseClient is a hub client that can answer many lease requests in
// one round trip.
type BatchLeaseClient interface {
	LeaseClient
	RequestQuotaLeases(ctx context.Context, in *types.QuotaLeaseBatchRequest) (*types.QuotaLeaseBatchResponse, error)
}

const (
	batchMaxItems    = 256
	batchMaxInflight = 2
)

// Batcher coalesces concurrent lease requests (for different keys) into
// batched RPCs, so the hub's request rate grows with the number of edges
// rather than with the number of active keys.
//
// It batches naturally rather than on a timer: a request goes out at once
// when fewer than batchMaxInflight batches are in flight, and otherwise
// waits for the next one. An idle edge pays no added latency; a busy one
// sends a few large batches.
type Batcher struct {
	client BatchLeaseClient

	mu       sync.Mutex
	pending  []*batchCall
	inflight int
	// unbatched is set when the hub predates batching.
	unbatched atomic.Bool
}

type batchCall struct {
	ctx  context.Context
	req  *types.QuotaLeaseRequest
	resp *types.QuotaLeaseResponse
	err  error
	done chan struct{}
}

// NewBatcher wraps client.
func NewBatcher(client BatchLeaseClient) *Batcher { return &Batcher{client: client} }

// RequestQuotaLease queues one request into the next batch.
func (b *Batcher) RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	if b.unbatched.Load() {
		return b.client.RequestQuotaLease(ctx, in)
	}
	c := &batchCall{ctx: ctx, req: in, done: make(chan struct{})}
	b.mu.Lock()
	b.pending = append(b.pending, c)
	batch := b.takeLocked()
	b.mu.Unlock()
	if batch != nil {
		go b.send(batch)
	}
	select {
	case <-c.done:
		return c.resp, c.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ReturnLeases passes through to the hub.
func (b *Batcher) ReturnLeases(ctx context.Context, in *types.ReturnLeasesRequest) (*types.ReturnLeasesResponse, error) {
	r, ok := b.client.(LeaseReturner)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "hub client cannot return leases")
	}
	return r.ReturnLeases(ctx, in)
}

// takeLocked starts a batch if a slot is free.
func (b *Batcher) takeLocked() []*batchCall {
	if len(b.pending) == 0 || b.inflight >= batchMaxInflight {
		return nil
	}
	n := min(len(b.pending), batchMaxItems)
	batch := b.pending[:n:n]
	b.pending = append([]*batchCall(nil), b.pending[n:]...)
	b.inflight++
	return batch
}

func (b *Batcher) send(batch []*batchCall) {
	for batch != nil {
		b.exchange(batch)
		b.mu.Lock()
		b.inflight--
		batch = b.takeLocked()
		b.mu.Unlock()
	}
}

func (b *Batcher) exchange(batch []*batchCall) {
	defer func() {
		for _, c := range batch {
			close(c.done)
		}
	}()
	// Callers detach lease fetches from their own cancellation; the batch
	// is bounded by the RPC timeout alone.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(batch[0].ctx), rpcTimeout)
	defer cancel()
	in := &types.QuotaLeaseBatchRequest{Requests: make([]*types.QuotaLeaseRequest, len(batch))}
	for i, c := range batch {
		in.Requests[i] = c.req
	}
	out, err := b.client.RequestQuotaLeases(ctx, in)
	if status.Code(err) == codes.Unimplemented {
		b.unbatched.Store(true)
		for _, c := range batch {
			c.resp, c.err = b.client.RequestQuotaLease(ctx, c.req)
		}
		return
	}
	for i, c := range batch {
		switch {
		case err != nil:
			c.err = err
		case i >= len(out.GetResults()):
			c.err = status.Error(codes.Internal, "hub answered fewer leases than requested")
		case out.Results[i].GetCode() != 0:
			c.err = status.Error(codes.Code(out.Results[i].GetCode()), out.Results[i].GetMessage())
		default:
			c.resp = out.Results[i].GetResponse()
		}
	}
}
