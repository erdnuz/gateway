package hub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"
	"gateway/packages/decide"
	"gateway/packages/ledger"
	"gateway/packages/region"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const leaseRedisOpTimeout = 250 * time.Millisecond

// QuotaLeaseServer grants edges blocks of quota from the ledger. It decides
// everything about a grant itself: the quota, the algorithm and the tier
// come from config and the hub's own key records, never from the edge.
type QuotaLeaseServer struct {
	types.UnimplementedQuotaLeaseServiceServer

	ledger    ledger.Ledger
	cfgStore  ConfigRegistry
	tierStore TierStore
	region    *region.Node // nil: a single-region deployment
}

// SetRegion makes this hub one region of a multi-region deployment:
// services with global quotas grant from this region's allocation, and
// quotas that run out are reported to node, which borrows from peers.
func (s *QuotaLeaseServer) SetRegion(node *region.Node) { s.region = node }

func NewQuotaLeaseServer(l ledger.Ledger, cfgStore ConfigRegistry, tierStore TierStore) *QuotaLeaseServer {
	return &QuotaLeaseServer{ledger: l, cfgStore: cfgStore, tierStore: tierStore}
}

// lease is one request resolved against config.
type lease struct {
	item  ledger.BatchItem
	limit int64
}

func (s *QuotaLeaseServer) RequestQuotaLease(ctx context.Context, req *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	l, err := s.resolveLease(ctx, req)
	if err != nil {
		return nil, err
	}
	g, err := s.ledger.Grant(ctx, l.item.Ref, l.item.Spec, l.item.Args)
	if err != nil {
		telemetry.LeaseGrants.WithLabelValues("error").Inc()
		return nil, err
	}
	s.noteStarved(req, l, g)
	return grantResponse(l, g), nil
}

// noteStarved reports a regional quota that ran out to the region node.
func (s *QuotaLeaseServer) noteStarved(req *types.QuotaLeaseRequest, l lease, g ledger.Grant) {
	if s.region == nil || !l.item.Args.Regional || g.Granted > 0 {
		return
	}
	s.region.Starved(region.Need{Ref: l.item.Ref, Spec: l.item.Spec, Window: g.WindowID, Request: &types.BorrowRequest{
		Prefix: req.Prefix, ServiceId: req.ServiceId, ApiKey: req.ApiKey, LimitId: req.LimitId, TierId: req.TierId,
	}})
}

// RequestQuotaLeases answers a batch in one ledger round trip per shard.
func (s *QuotaLeaseServer) RequestQuotaLeases(ctx context.Context, in *types.QuotaLeaseBatchRequest) (*types.QuotaLeaseBatchResponse, error) {
	out := &types.QuotaLeaseBatchResponse{Results: make([]*types.QuotaLeaseResult, len(in.GetRequests()))}
	var items []ledger.BatchItem
	var leases []lease
	var index []int
	for i, req := range in.GetRequests() {
		l, err := s.resolveLease(ctx, req)
		if err != nil {
			out.Results[i] = errorResult(err)
			continue
		}
		items, leases, index = append(items, l.item), append(leases, l), append(index, i)
	}
	for j, r := range ledger.GrantAll(ctx, s.ledger, items) {
		if r.Err != nil {
			telemetry.LeaseGrants.WithLabelValues("error").Inc()
			out.Results[index[j]] = errorResult(r.Err)
			continue
		}
		s.noteStarved(in.GetRequests()[index[j]], leases[j], r.Grant)
		out.Results[index[j]] = &types.QuotaLeaseResult{Response: grantResponse(leases[j], r.Grant)}
	}
	return out, nil
}

// ReturnLeases credits unspent tokens back to the windows they came from, so
// a draining edge does not take its share of the quota with it.
func (s *QuotaLeaseServer) ReturnLeases(ctx context.Context, in *types.ReturnLeasesRequest) (*types.ReturnLeasesResponse, error) {
	var returns []ledger.Return
	for _, r := range in.GetReturns() {
		l, err := s.resolve(ctx, r.GetPrefix(), r.GetServiceId(), r.GetApiKey(), r.GetLimitId(), r.GetTierId(), r.GetOrg())
		if err != nil {
			continue // the limit or key is gone: nothing to credit
		}
		returns = append(returns, ledger.Return{
			Ref: l.item.Ref, Spec: l.item.Spec, Parent: l.item.Args.Parent,
			WindowID: r.GetWindowId(), ParentWindowID: r.GetParentWindowId(), Tokens: r.GetTokens(),
		})
	}
	dedupe := ""
	if in.GetDedupe() != "" {
		dedupe = edgeIdentity(ctx, in.GetEdgeId()) + ":" + in.GetDedupe()
	}
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.ledger.Return(rctx, dedupe, returns); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	for _, r := range returns {
		if r.Tokens > 0 {
			telemetry.TokensReturned.Add(float64(r.Tokens))
		} else {
			telemetry.DebtSettled.Add(float64(-r.Tokens))
		}
	}
	return &types.ReturnLeasesResponse{}, nil
}

func (s *QuotaLeaseServer) resolveLease(ctx context.Context, req *types.QuotaLeaseRequest) (lease, error) {
	if err := req.Validate(); err != nil {
		return lease{}, status.Error(codes.InvalidArgument, err.Error())
	}
	l, err := s.resolve(ctx, req.Prefix, req.ServiceId, req.ApiKey, req.LimitId, req.TierId, req.Org)
	if err != nil {
		return lease{}, err
	}
	l.item.Args.Requested = req.RequestedTokens
	l.item.Args.EdgeID = edgeIdentity(ctx, req.EdgeId)
	l.item.Args.ObservedRate = req.ObservedRate
	return l, nil
}

// resolve finds the quota a request draws on: the caller's tier limit (with
// the owner's org budget around it), or one of the service's named limits.
func (s *QuotaLeaseServer) resolve(ctx context.Context, prefix, service, subject, limitID, tierID, org string) (lease, error) {
	if !validKey(s.cfgStore, subject) {
		return lease{}, status.Error(codes.InvalidArgument, "api_key does not match api_key_pattern")
	}
	if prefix == decide.Scope {
		return s.resolveDescriptor(service, subject, limitID)
	}
	prefixCfg, svcCfg, ok := s.cfgStore.FindService(prefix, service)
	if !ok {
		return lease{}, status.Errorf(codes.NotFound, "service %q not found in prefix %q", service, prefix)
	}
	l := lease{item: ledger.BatchItem{Ref: ledger.Ref{Scope: prefix, Service: service, Limit: limitID, Subject: subject}}}
	if p := svcCfg.Lease; p.Adaptive() {
		l.item.Args.Horizon = p.EffectiveHorizon()
		l.item.Args.MinGrant = max(p.Min, types.DefaultLeaseMin)
		l.item.Args.MaxGrant = p.Max
	}

	if limitID != "" {
		spec, ok := findLimit(svcCfg, limitID)
		if !ok {
			telemetry.LeaseGrants.WithLabelValues("rejected").Inc()
			return lease{}, status.Errorf(codes.NotFound, "no limit %q on service %q", limitID, service)
		}
		l.item.Spec = ledger.Spec{Algo: ledger.Algo(spec.Algorithm), Limit: spec.Quota, Window: spec.Window, Rate: spec.Rate}
		if l.item.Spec.Window <= 0 {
			l.item.Spec.Window = prefixCfg.QuotaPeriod
		}
		l.limit = spec.Quota
		return l, nil
	}

	// API keys get their tier and owner from the hub's own records. Callers
	// of jwt and none services have none: the (mTLS-authenticated) edge
	// resolved their tier and org from the token or the service default.
	if svcCfg.AuthType() == "api_key" {
		tctx, cancel := context.WithTimeout(ctx, leaseRedisOpTimeout)
		defer cancel()
		var err error
		if tierID, err = s.tierStore.GetTier(tctx, prefix, subject); err != nil {
			return lease{}, status.Errorf(codes.Unavailable, "tier lookup failed: %v", err)
		}
		org = ""
		if owners, ok := s.tierStore.(OwnerStore); ok && svcCfg.Org != nil {
			if org, err = owners.GetOwner(tctx, prefix, subject); err != nil {
				return lease{}, status.Errorf(codes.Unavailable, "owner lookup failed: %v", err)
			}
		}
	}
	tierCfg, ok := findTier(svcCfg, tierID)
	if !ok {
		// Unknown key or unknown tier: a policy rejection, not an outage, so
		// the edge must deny rather than fall back to its safety tier.
		telemetry.LeaseGrants.WithLabelValues("rejected").Inc()
		return lease{}, status.Errorf(codes.PermissionDenied, "no tier %q for key in service", tierID)
	}
	l.item.Spec = ledger.Spec{
		Algo:   ledger.Algo(tierCfg.Algorithm),
		Limit:  int64(tierCfg.Quota),
		Window: prefixCfg.QuotaPeriod,
		Rate:   tierCfg.Rate,
	}
	l.limit = int64(tierCfg.Quota)
	if g := svcCfg.Global; g != nil && s.region != nil {
		l.item.Args.Regional = true
		l.item.Args.AllocBase = region.Base(l.limit, g.Floors, s.region.Name, subject)
	}
	if o := svcCfg.Org; o != nil && org != "" {
		l.item.Ref.Parent = orgID(org)
		window := o.Window
		if window <= 0 {
			window = prefixCfg.QuotaPeriod
		}
		l.item.Args.Parent = &ledger.Spec{Algo: ledger.FixedWindow, Limit: o.Quota, Window: window}
	}
	return l, nil
}

// resolveDescriptor finds a decision-service limit: domain and entry path
// name it, the subject is the hash of the caller's descriptor values.
func (s *QuotaLeaseServer) resolveDescriptor(domain, subject, limitID string) (lease, error) {
	spec, ok := decide.Find(s.cfgStore.Snapshot(), domain, limitID)
	if !ok {
		telemetry.LeaseGrants.WithLabelValues("rejected").Inc()
		return lease{}, status.Errorf(codes.NotFound, "no descriptor limit %q in domain %q", limitID, domain)
	}
	l := lease{limit: spec.Quota, item: ledger.BatchItem{
		Ref:  ledger.Ref{Scope: decide.Scope, Service: domain, Limit: limitID, Subject: subject},
		Spec: ledger.Spec{Algo: ledger.Algo(spec.Algorithm), Limit: spec.Quota, Window: spec.Window, Rate: spec.Rate},
		Args: ledger.GrantArgs{Horizon: types.DefaultLeaseHorizon, MinGrant: types.DefaultLeaseMin},
	}}
	return l, nil
}

// RegionServer lends this region's unused allocation to peer regions.
type RegionServer struct {
	types.UnimplementedRegionPeerServer
	Leases *QuotaLeaseServer
}

// Borrow lends up to want of this region's allocation for the window the
// peer is short in, keeping what this region is on pace to use itself. The
// allocation is debited before the answer is sent.
func (r *RegionServer) Borrow(ctx context.Context, in *types.BorrowRequest) (*types.BorrowResponse, error) {
	s := r.Leases
	alloc, ok := s.ledger.(ledger.Allocator)
	if !ok || s.region == nil {
		return nil, status.Error(codes.FailedPrecondition, "this hub is not part of a multi-region deployment")
	}
	if in.GetTransferId() == "" || in.GetWant() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "transfer_id and want are required")
	}
	l, err := s.resolve(ctx, in.GetPrefix(), in.GetServiceId(), in.GetApiKey(), in.GetLimitId(), in.GetTierId(), "")
	if err != nil {
		return nil, err
	}
	if !l.item.Args.Regional {
		return nil, status.Error(codes.FailedPrecondition, "the quota is not global")
	}
	given, err := alloc.Lend(ctx, l.item.Ref, l.item.Spec, l.item.Args.AllocBase, in.GetWindowId(), in.GetWant(), 0, in.GetTransferId())
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	telemetry.RegionTokensLent.Add(float64(given))
	return &types.BorrowResponse{Given: given}, nil
}

func grantResponse(l lease, g ledger.Grant) *types.QuotaLeaseResponse {
	resp := &types.QuotaLeaseResponse{
		GrantedTokens:   g.Granted,
		LeaseTtlSeconds: int64((g.ExpiresIn + time.Second - 1) / time.Second),
		RemainingGlobal: g.Remaining,
		WindowId:        g.WindowID,
		ExpiresInMs:     g.ExpiresIn.Milliseconds(),
		ResetInMs:       g.ResetIn.Milliseconds(),
		ActiveEdges:     g.ActiveEdges,
		ParentWindowId:  g.ParentWindowID,
		Limit:           l.limit,
	}
	if g.Granted == 0 {
		// Nothing left: the edge stops asking until retry_after_ms (with
		// jitter) and denies locally meanwhile.
		resp.WaitingForCapacity = true
		resp.RetryAfterMs = max(g.RetryAfter.Milliseconds(), 1)
		telemetry.LeaseGrants.WithLabelValues("waiting").Inc()
		return resp
	}
	telemetry.LeaseGrants.WithLabelValues("granted").Inc()
	telemetry.TokensGranted.Add(float64(g.Granted))
	return resp
}

func errorResult(err error) *types.QuotaLeaseResult {
	st, ok := status.FromError(err)
	if !ok {
		st = status.New(codes.Unavailable, err.Error())
	}
	if errors.Is(err, ledger.ErrBadSpec) {
		st = status.New(codes.FailedPrecondition, err.Error())
	}
	return &types.QuotaLeaseResult{Code: int32(st.Code()), Message: st.Message()}
}

// edgeIdentity scopes an edge's self-reported ID by its mTLS certificate, so
// one edge cannot pose as another in the endgame split or in returns.
func edgeIdentity(ctx context.Context, edgeID string) string {
	if edgeID == "" {
		return ""
	}
	if p, ok := peer.FromContext(ctx); ok {
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(ti.State.PeerCertificates) > 0 {
			if cn := ti.State.PeerCertificates[0].Subject.CommonName; cn != "" {
				return cn + "/" + edgeID
			}
		}
	}
	return edgeID
}

// orgID turns an owner name into a ledger-safe identifier (owners are free
// text; ledger keys carry Redis Cluster hash tags).
func orgID(org string) string {
	sum := sha256.Sum256([]byte(org))
	return hex.EncodeToString(sum[:8])
}

func findLimit(svcCfg *types.ServiceConfig, id string) (*types.LimitSpec, bool) {
	for i := range svcCfg.Limits {
		if svcCfg.Limits[i].ID == id {
			return &svcCfg.Limits[i], true
		}
	}
	return nil, false
}

func findTier(svcCfg *types.ServiceConfig, tierID string) (*types.TierConfig, bool) {
	if svcCfg == nil {
		return nil, false
	}
	for i := range svcCfg.Tiers {
		if svcCfg.Tiers[i].TierID == tierID {
			return &svcCfg.Tiers[i], true
		}
	}
	if svcCfg.SafetyTier != nil && svcCfg.SafetyTier.TierID == tierID {
		return svcCfg.SafetyTier, true
	}
	return nil, false
}
