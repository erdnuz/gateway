package startup

import "context"

// HealthCheck defines the startup contract each service should implement
// before binding network listeners.
type HealthCheck interface {
	ValidateConfig(ctx context.Context) error
	CheckDependencies(ctx context.Context) error
}

// ValidateOnly can be used by services that only need config validation in
// a given phase.
type ValidateOnly interface {
	ValidateConfig(ctx context.Context) error
}

// DependencyOnly can be used by services that only run dependency checks in
// a given phase.
type DependencyOnly interface {
	CheckDependencies(ctx context.Context) error
}

// Run executes startup validation and dependency checks in sequence.
func Run(ctx context.Context, h HealthCheck) error {
	if err := h.ValidateConfig(ctx); err != nil {
		return err
	}
	if err := h.CheckDependencies(ctx); err != nil {
		return err
	}
	return nil
}
