package config

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func setupTestDB(t *testing.T) (*mongo.Client, *ConfigManager) {
	uri := os.Getenv("MONGO_TEST_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}

	opts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatalf("Failed to connect to Mongo: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("Mongo ping failed: %v", err)
	}

	manager := NewConfigManager(client, nil)
	// Ensure indexes are set up for the test
	if err := manager.SetupIndexes(ctx); err != nil {
		t.Fatalf("Failed to setup indexes: %v", err)
	}

	return client, manager
}

func TestConfigManager(t *testing.T) {
	client, configManager := setupTestDB(t)
	defer client.Disconnect(context.Background())

	ctx := context.Background()
	testServiceID := "test-service-123"

	// Helper for fresh state
	cleanup := func() {
		configManager.collection.DeleteOne(ctx, bson.M{"service_id": testServiceID})
	}
	cleanup()
	defer cleanup()

	t.Run("Create and Get Config", func(t *testing.T) {
		cfg := ServiceConfig{
			ServiceID: testServiceID,
			TargetURL: "http://localhost:8000",
			CORS: CORSConfig{
				AllowedOrigins: []string{"*"},
			},
			Tiers: []TierConfig{
				{
					Quota:       100,
					QuotaPeriod: time.Hour,
				},
			},
		}

		err := configManager.CreateServiceConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("CreateServiceConfig failed: %v", err)
		}

		retrieved, err := configManager.GetConfig(ctx, testServiceID)
		if err != nil {
			t.Fatalf("GetConfig failed: %v", err)
		}

		if retrieved.ServiceID != cfg.ServiceID {
			t.Errorf("Expected %s, got %s", cfg.ServiceID, retrieved.ServiceID)
		}

	})

	t.Run("Update Config", func(t *testing.T) {
		// New configuration to update
		updatedCfg := ServiceConfig{
			ServiceID: testServiceID,
			TargetURL: "http://localhost:9999",
			Tiers: []TierConfig{
				{
					Quota:       500,
					QuotaPeriod: time.Hour,
				},
			},
		}

		err := configManager.UpdateConfig(ctx, testServiceID, updatedCfg)
		if err != nil {
			t.Fatalf("UpdateConfig failed: %v", err)
		}

		retrieved, _ := configManager.GetConfig(ctx, testServiceID)
		if retrieved.TargetURL != "http://localhost:9999" {
			t.Error("Update failed to change TargetURL")
		}
	})

	t.Run("Validation Failure", func(t *testing.T) {
		badCfg := ServiceConfig{
			ServiceID: "bad-cfg",
			TargetURL: "", // Invalid: empty
		}

		err := configManager.CreateServiceConfig(ctx, badCfg)
		if err == nil {
			t.Error("Expected validation error for empty TargetURL, got nil")
		}
	})

	t.Run("List All Configs", func(t *testing.T) {
		configs, err := configManager.ListAllConfigs(ctx)
		if err != nil {
			t.Fatalf("ListAllConfigs failed: %v", err)
		}

		found := false
		for _, cfg := range configs {
			if cfg.ServiceID == testServiceID {
				found = true
				break
			}
		}
		if !found {
			t.Error("ListAllConfigs did not return the expected config")
		}
	})

	t.Run("Delete Config", func(t *testing.T) {
		err := configManager.DeleteConfig(ctx, testServiceID)
		if err != nil {
			t.Fatalf("DeleteConfig failed: %v", err)
		}

		_, err = configManager.GetConfig(ctx, testServiceID)
		if err == nil {
			t.Error("Expected error when fetching deleted config, got nil")
		}
	})
}
