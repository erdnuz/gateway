package hub

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/singleflight"
)

const (
	configCacheKey = "gateway:config:latest"
	configTTL      = 0 // No expiry as requested previously; manually invalidated
)

type ConfigManager struct {
	collection *mongo.Collection
	rdb        *redis.Client
	group      singleflight.Group
}

func NewConfigManager(db *mongo.Database, rdb *redis.Client) *ConfigManager {
	return &ConfigManager{
		collection: db.Collection("gateway_settings"),
		rdb:        rdb,
	}
}

// GetConfig retrieves from Redis, then Mongo fallback with singleflight
func (cm *ConfigManager) GetConfig(ctx context.Context) (*types.GatewayConfig, error) {
	// 1. Try Redis Cache
	cached, err := cm.rdb.Get(ctx, configCacheKey).Bytes()
	if err == nil {
		return cm.unmarshalConfig(cached)
	}

	// 2. Singleflight prevents "Thundering Herd" on Mongo
	val, err, _ := cm.group.Do(configCacheKey, func() (interface{}, error) {
		var config types.GatewayConfig

		// Find the singleton config document
		err := cm.collection.FindOne(ctx, bson.M{"_id": "root_config"}).Decode(&config)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil, nil
			}
			return nil, err
		}

		// 3. Repopulate Redis
		configBytes, _ := json.Marshal(config)
		cm.rdb.Set(ctx, configCacheKey, configBytes, configTTL)

		return config, nil
	})

	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, errors.New("configuration not found in database")
	}

	cfg := val.(types.GatewayConfig)
	return &cfg, nil
}

// UpdateConfig updates the Mongo document and invalidates Redis
func (cm *ConfigManager) UpdateConfig(ctx context.Context, config *types.GatewayConfig) error {
	// 1. Persist to MongoDB using Upsert
	opts := options.Update().SetUpsert(true)
	filter := bson.M{"_id": "root_config"}

	// We use the struct directly; Mongo driver handles the BSON mapping
	update := bson.M{
		"$set": bson.M{
			"prefixes":   config.Prefixes,
			"updated_at": time.Now(),
		},
	}

	_, err := cm.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return err
	}

	// 2. Invalidate Redis Cache
	return cm.rdb.Del(ctx, configCacheKey).Err()
}

// UpdateService performs a deep-update and saves the new root config
func (cm *ConfigManager) UpdateService(ctx context.Context, prefixStr string, serviceID string, service *types.ServiceConfig) error {
	config, err := cm.GetConfig(ctx)
	if err != nil || config == nil {
		return err
	}

	found := false
	for i, p := range config.Prefixes {
		if p.Prefix == prefixStr {
			for j, svc := range p.Services {
				if svc.ServiceID == serviceID {
					config.Prefixes[i].Services[j] = *service
					found = true
					break
				}
			}
		}
	}

	if !found {
		return errors.New("service or prefix not found in current config")
	}

	return cm.UpdateConfig(ctx, config)
}

func (cm *ConfigManager) unmarshalConfig(data []byte) (*types.GatewayConfig, error) {
	var config types.GatewayConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return &config, nil
}
