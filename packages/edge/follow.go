package edge

import (
	"context"
	"encoding/json"
	"log/slog"

	"gateway/packages/common/types"
)

// FollowHub applies the hub's config and tier changes until ctx ends. The
// edge opens for traffic once the first config snapshot arrives; losing the
// hub afterwards does not close it again (the failure policies take over).
func (s *Server) FollowHub(ctx context.Context, hub *HubClient) {
	hub.Watch(ctx, s.opts.EdgeID, func(ev *types.ControlEvent) { s.Apply(ctx, ev) })
}

// Apply handles one control-plane event.
func (s *Server) Apply(ctx context.Context, ev *types.ControlEvent) {
	switch e := ev.GetEvent().(type) {
	case *types.ControlEvent_Config:
		var cfg types.GatewayConfig
		if err := json.Unmarshal(e.Config.GetConfigJson(), &cfg); err != nil {
			slog.Error("ignoring undecodable config snapshot", "err", err)
			return
		}
		s.Config.Apply(&cfg)
		if e.Config.GetResetTiers() && s.Tiers != nil {
			s.Tiers.Reset()
		}
		if s.Responses != nil {
			for _, sc := range e.Config.GetInvalidate() {
				if err := s.Responses.Invalidate(ctx, sc.GetPrefix(), sc.GetServiceId()); err != nil {
					slog.Warn("response cache invalidation failed", "prefix", sc.GetPrefix(), "service", sc.GetServiceId(), "err", err)
				}
			}
		}
		if !s.startupReady.Load() {
			s.SetPendingDependency("")
			s.SetStartupReady(true)
			slog.Info("edge ready: config received from hub", "services", countServices(&cfg))
		}
	case *types.ControlEvent_Tier:
		if s.Tiers != nil {
			s.Tiers.Set(e.Tier.GetPrefix(), e.Tier.GetApiKey(), e.Tier.GetTierId())
		}
	}
}

func countServices(cfg *types.GatewayConfig) int {
	n := 0
	for _, p := range cfg.Prefixes {
		n += len(p.Services)
	}
	return n
}
