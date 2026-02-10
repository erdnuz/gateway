// Package config manages the persistent configuration for the API Gateway,
// including service routing, rate limiting tiers, and resilience policies.
package config

import "time"

// ServiceStatus represents the operational state of a downstream service.
type ServiceStatus string

const (
	// StatusActive indicates the service is healthy and accepting traffic.
	StatusActive ServiceStatus = "ACTIVE"
	// StatusMaintenance indicates the service is in a planned maintenance window.
	StatusMaintenance ServiceStatus = "MAINTENANCE"
	// StatusDisabled indicates the service is fully offline and unreachable.
	StatusDisabled ServiceStatus = "DISABLED"
)

// --- Configuration Sub-Structures ---

// TierConfig defines the rate limiting rules and costs for a specific user group.
type TierConfig struct {
	Quota       uint64            `bson:"quota" json:"quota"`               // Max requests allowed in period
	QuotaPeriod time.Duration     `bson:"quota_period" json:"quota_period"` // Window size, e.g., 24h
	CacheTTL    time.Duration     `bson:"cache_ttl" json:"cache_ttl"`       // How long downstream responses are cached
	TokenCosts  map[string]uint64 `bson:"token_costs" json:"token_costs"`
}

// CORSConfig defines the Cross-Origin Resource Sharing policy for a service.
type CORSConfig struct {
	AllowedOrigins   []string `bson:"allowed_origins" json:"allowed_origins"`
	AllowedMethods   []string `bson:"allowed_methods" json:"allowed_methods"`
	AllowedHeaders   []string `bson:"allowed_headers" json:"allowed_headers"`
	ExposeHeaders    []string `bson:"expose_headers" json:"expose_headers"`
	AllowCredentials bool     `bson:"allow_credentials" json:"allow_credentials"`
	MaxAge           int      `bson:"max_age" json:"max_age"` // Cache duration for preflight
}

// HealthCheck configures active monitoring for the downstream service.
type HealthCheck struct {
	Path     string        `bson:"path" json:"path"`         // Endpoint for health probes, e.g., "/health"
	Interval time.Duration `bson:"interval" json:"interval"` // Time between probes
	Timeout  time.Duration `bson:"timeout" json:"timeout"`   // Time to wait for response
}

// ResilienceConfig manages network-level stability and retry policies.
type ResilienceConfig struct {
	MaxRetries    int           `bson:"max_retries" json:"max_retries"`       // Number of retry attempts
	RetryInterval time.Duration `bson:"retry_interval" json:"retry_interval"` // Wait time between retries
	ReadTimeout   time.Duration `bson:"read_timeout" json:"read_timeout"`     // Max time to read response
}

// HeaderConfig allows the gateway to modify headers during proxying.
type HeaderConfig struct {
	Inject map[string]string `bson:"inject" json:"inject"` // Key-value pairs to add to request
	Remove []string          `bson:"remove" json:"remove"` // Header keys to strip
}

// --- Main Configuration Structure ---

// ServiceConfig is the primary document representing a downstream API and its proxy rules.
type ServiceConfig struct {
	ServiceID  string           `bson:"service_id" json:"service_id"` // Unique identifier
	TargetURL  string           `bson:"target_url" json:"target_url"` // Downstream destination
	AuthType   string           `bson:"auth_type" json:"auth_type"`   // e.g., "API_KEY", "IP"
	Status     ServiceStatus    `bson:"status" json:"status"`         // Current operational state
	CORS       CORSConfig       `bson:"cors" json:"cors"`             // Browser security policy
	Tiers      []TierConfig     `bson:"tiers" json:"tiers"`           // Access control groups
	Resilience ResilienceConfig `bson:"resilience" json:"resilience"` // Stability settings
	Health     HealthCheck      `bson:"health" json:"health"`         // Monitoring settings
	Headers    HeaderConfig     `bson:"headers" json:"headers"`       // Header transformations
	CreatedAt  time.Time        `bson:"created_at" json:"created_at"`
	UpdatedAt  time.Time        `bson:"updated_at" json:"updated_at"`
}
