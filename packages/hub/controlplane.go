package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"gateway/packages/common/telemetry"
	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// controlChannel is the Redis channel every hub publishes changes to and
// subscribes to. Hubs share one Redis, so a change made through any hub
// reaches the edges watching every other hub, and hubs can be replicated
// behind a gRPC load balancer.
const controlChannel = "gate:control"

// watcherBuffer bounds the events queued for one edge. An edge that falls
// further behind is disconnected rather than slowing the hub; it reconnects
// and resynchronises from a fresh snapshot.
const watcherBuffer = 256

// ControlPlane streams config and tier changes to edges and resolves API
// keys for them.
type ControlPlane struct {
	types.UnimplementedControlPlaneServer
	cfg    ConfigRegistry
	tiers  TierStore
	rdb    *redis.Client // nil: deliver to local watchers only (tests)
	origin string        // identifies this hub's own messages

	mu       sync.Mutex
	watchers map[chan *types.ControlEvent]struct{}
}

// controlMessage is the hub-to-hub form of a change.
type controlMessage struct {
	Origin     string                          `json:"origin"`
	Config     *types.GatewayConfig            `json:"config,omitempty"`
	Invalidate []types.ConfigInvalidationScope `json:"invalidate,omitempty"`
	Tier       *types.TierChange               `json:"tier,omitempty"`
}

func NewControlPlane(cfg ConfigRegistry, tiers TierStore, rdb *redis.Client) *ControlPlane {
	return &ControlPlane{cfg: cfg, tiers: tiers, rdb: rdb, origin: newBootID(), watchers: map[chan *types.ControlEvent]struct{}{}}
}

// Run relays changes published by any hub to this hub's watchers until ctx
// ends.
func (c *ControlPlane) Run(ctx context.Context) {
	if c.rdb == nil {
		return
	}
	sub := c.rdb.Subscribe(ctx, controlChannel)
	defer sub.Close()
	msgs := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-msgs:
			var m controlMessage
			if err := json.Unmarshal([]byte(msg.Payload), &m); err != nil {
				slog.Warn("ignoring malformed control message", "err", err)
				continue
			}
			c.deliver(m)
		}
	}
}

// Watchers reports how many edges are connected to this hub.
func (c *ControlPlane) Watchers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.watchers)
}

// PublishConfig announces the active config after a reload.
func (c *ControlPlane) PublishConfig(cfg *types.GatewayConfig, invalidate []types.ConfigInvalidationScope) {
	c.publish(controlMessage{Origin: c.origin, Config: cfg, Invalidate: invalidate})
}

// PublishTier announces a key's new tier ("" when revoked).
func (c *ControlPlane) PublishTier(prefix, apiKey, tierID string) {
	c.publish(controlMessage{Origin: c.origin, Tier: &types.TierChange{Prefix: prefix, ApiKey: apiKey, TierId: tierID}})
}

// publish sends a change through Redis so every hub (this one included)
// delivers it. If Redis is unavailable the change still reaches this hub's
// edges; others converge when their cached tiers expire or on the next
// reconnect.
func (c *ControlPlane) publish(m controlMessage) {
	if c.rdb != nil {
		b, err := json.Marshal(m)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err = c.rdb.Publish(ctx, controlChannel, b).Err()
			cancel()
			if err == nil {
				return
			}
		}
		slog.Warn("control message not shared with other hubs", "err", err)
	}
	c.deliver(m)
}

// deliver applies a change from another hub and forwards it to this hub's
// watchers.
func (c *ControlPlane) deliver(m controlMessage) {
	var ev *types.ControlEvent
	switch {
	case m.Config != nil:
		if m.Origin != c.origin {
			if r, ok := c.cfg.(configPayloadReloader); ok {
				if err := r.ReloadFromPayload(m.Config); err != nil {
					slog.Error("config from another hub rejected", "err", err)
					return
				}
			}
		}
		snap, err := c.snapshot(false, m.Invalidate)
		if err != nil {
			slog.Error("config snapshot failed", "err", err)
			return
		}
		ev = snap
		telemetry.ControlPublished.WithLabelValues("config").Inc()
	case m.Tier != nil:
		ev = &types.ControlEvent{Event: &types.ControlEvent_Tier{Tier: m.Tier}}
		telemetry.ControlPublished.WithLabelValues("tier").Inc()
	default:
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for ch := range c.watchers {
		select {
		case ch <- ev:
		default: // too far behind: drop it; the edge reconnects and resyncs
			telemetry.WatchersDropped.Inc()
			delete(c.watchers, ch)
			close(ch)
		}
	}
}

func (c *ControlPlane) snapshot(resetTiers bool, invalidate []types.ConfigInvalidationScope) (*types.ControlEvent, error) {
	b, err := json.Marshal(c.cfg.Snapshot())
	if err != nil {
		return nil, err
	}
	snap := &types.ConfigSnapshot{ConfigJson: b, ResetTiers: resetTiers}
	for _, s := range invalidate {
		snap.Invalidate = append(snap.Invalidate, &types.CacheScope{Prefix: s.Prefix, ServiceId: s.ServiceID})
	}
	return &types.ControlEvent{Event: &types.ControlEvent_Config{Config: snap}}, nil
}

// Watch streams a config snapshot, then every change, to one edge.
func (c *ControlPlane) Watch(req *types.WatchRequest, stream types.ControlPlane_WatchServer) error {
	ch := make(chan *types.ControlEvent, watcherBuffer)
	c.mu.Lock()
	c.watchers[ch] = struct{}{}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.watchers, ch)
		c.mu.Unlock()
	}()

	first, err := c.snapshot(true, nil)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if err := stream.Send(first); err != nil {
		return err
	}
	slog.Info("edge connected", "edge", req.GetEdgeId())
	defer slog.Info("edge disconnected", "edge", req.GetEdgeId())
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return status.Error(codes.Unavailable, "edge fell behind; reconnect to resynchronise")
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

// ResolveKey returns the tier of an API key ("" when unknown).
func (c *ControlPlane) ResolveKey(ctx context.Context, req *types.ResolveKeyRequest) (*types.ResolveKeyResponse, error) {
	if _, ok := c.cfg.FindPrefix(req.GetPrefix()); !ok {
		return nil, status.Errorf(codes.NotFound, "unknown prefix %q", req.GetPrefix())
	}
	if !validKey(c.cfg, req.GetApiKey()) {
		return nil, status.Error(codes.InvalidArgument, "api_key does not match api_key_pattern")
	}
	tier, err := c.tiers.GetTier(ctx, req.GetPrefix(), req.GetApiKey())
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &types.ResolveKeyResponse{TierId: tier}, nil
}

var keyPatterns sync.Map // pattern source -> *regexp.Regexp

// validKey applies runtime.hub.api_key_pattern. Keys are interpolated into
// Redis keys, so the hub never trusts the edge to have checked.
func validKey(cfg ConfigRegistry, apiKey string) bool {
	src := cfg.HubPolicy().APIKeyPattern
	if src == "" {
		src = types.DefaultRuntimePolicy().Hub.APIKeyPattern
	}
	re, ok := keyPatterns.Load(src)
	if !ok {
		compiled, err := regexp.Compile(src)
		if err != nil {
			return false
		}
		re, _ = keyPatterns.LoadOrStore(src, compiled)
	}
	return re.(*regexp.Regexp).MatchString(apiKey)
}
