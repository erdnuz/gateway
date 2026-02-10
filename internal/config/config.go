package config

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type ConfigManager struct {
	collection *mongo.Collection
	redis      *redis.Client
}

// NewConfigManager initializes the manager and should be followed by SetupIndexes
func NewConfigManager(client *mongo.Client, rdb *redis.Client) *ConfigManager {
	return &ConfigManager{
		collection: client.Database("gateway").Collection("configs"),
		redis:      rdb,
	}
}

// SetupIndexes ensures high-performance lookups and uniqueness for ServiceID
func (m *ConfigManager) SetupIndexes(ctx context.Context) error {
	_, err := m.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{bson.E{Key: "service_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}

// Validate ensures the service configuration is logically sound and safe to deploy
func (s *ServiceConfig) Validate() error {
	if s.ServiceID == "" {
		return fmt.Errorf("service_id is required")
	}
	if s.TargetURL == "" {
		return fmt.Errorf("target_url is required")
	}

	// 2. Validate Tiers
	if len(s.Tiers) == 0 {
		return fmt.Errorf("at least one tier must be defined")
	}
	for _, t := range s.Tiers {
		if t.Quota == 0 || t.QuotaPeriod <= 0 {
			return fmt.Errorf("quota and quota_period must be positive")
		}
	}

	// 3. Validate Resilience/Health (Basic checks)
	if s.Health.Path != "" && s.Health.Interval <= 0 {
		return fmt.Errorf("health check interval must be positive if path is set")
	}

	return nil
}

// CreateServiceConfig saves a new service and notifies all gateway pods
func (m *ConfigManager) CreateServiceConfig(ctx context.Context, cfg ServiceConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	cfg.CreatedAt = time.Now()
	cfg.UpdatedAt = time.Now()

	_, err := m.collection.InsertOne(ctx, cfg)
	if err == nil {
		m.notifyReload(ctx, cfg.ServiceID)
	}
	return err
}

// GetConfig fetches a config by ServiceID
func (m *ConfigManager) GetConfig(ctx context.Context, serviceID string) (*ServiceConfig, error) {
	var cfg ServiceConfig
	err := m.collection.FindOne(ctx, bson.M{"service_id": serviceID}).Decode(&cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// UpdateConfig updates an existing config and triggers a global reload signal
func (m *ConfigManager) UpdateConfig(ctx context.Context, serviceID string, updates ServiceConfig) error {
	if err := updates.Validate(); err != nil {
		return err
	}

	filter := bson.M{"service_id": serviceID}
	// Use $set to only change specific fields and preserve CreatedAt
	update := bson.M{
		"$set": bson.M{
			"target_url": updates.TargetURL,
			"cors":       updates.CORS,
			"tiers":      updates.Tiers,
			"resilience": updates.Resilience,
			"health":     updates.Health,
			"headers":    updates.Headers,
			"status":     updates.Status,
			"updated_at": time.Now(),
		},
	}

	res, err := m.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	if res.ModifiedCount > 0 {
		m.notifyReload(ctx, serviceID)
	}
	return nil
}

// ListAllConfigs retrieves all configurations
func (m *ConfigManager) ListAllConfigs(ctx context.Context) ([]ServiceConfig, error) {
	var configs []ServiceConfig
	cursor, err := m.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	err = cursor.All(ctx, &configs)
	return configs, err
}

// DeleteConfig removes a service and signals pods to purge from local RAM
func (m *ConfigManager) DeleteConfig(ctx context.Context, serviceID string) error {
	res, err := m.collection.DeleteOne(ctx, bson.M{"service_id": serviceID})
	if err != nil {
		return err
	}

	if res.DeletedCount > 0 {
		m.notifyDeletion(ctx, serviceID)
	}
	return nil
}

// --- Internal Notification Helpers ---

func (m *ConfigManager) notifyReload(ctx context.Context, serviceID string) {
	if m.redis != nil {
		m.redis.Publish(ctx, "config_reload", "UPDATE:"+serviceID)
	}
}

func (m *ConfigManager) notifyDeletion(ctx context.Context, serviceID string) {
	if m.redis != nil {
		m.redis.Publish(ctx, "config_reload", "DELETE:"+serviceID)
	}
}
