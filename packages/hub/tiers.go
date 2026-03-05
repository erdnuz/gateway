package hub

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/singleflight"
)

const cacheTTL = 1 * time.Hour

type TierManager struct {
	collection *mongo.Collection
	rdb        *redis.Client
	group      singleflight.Group
}

func NewTierManager(db *mongo.Database, rdb *redis.Client) *TierManager {
	ensureTierIndexes(context.Background(), db.Collection("user_tiers")) // Ensure indexes on startup
	return &TierManager{
		collection: db.Collection("user_tiers"),
		rdb:        rdb,
	}
}

func (tm *TierManager) getCacheKey(prefixID, apiKey string) string {
	return "tier:" + prefixID + ":" + apiKey
}

// SetTier creates or updates the user's tier assignment in MongoDB
func (tm *TierManager) SetTier(ctx context.Context, prefixID, apiKey, tierID string) error {
	filter := bson.M{
		"prefix_id": prefixID,
		"api_key":   apiKey,
	}
	update := bson.M{
		"$set": bson.M{
			"tier_id":    tierID,
			"updated_at": time.Now(),
		},
	}
	opts := options.Update().SetUpsert(true)

	_, err := tm.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return err
	}

	// Invalidate cache immediately to ensure the next request gets the new tier
	return tm.rdb.Del(ctx, tm.getCacheKey(prefixID, apiKey)).Err()
}

// GetTier uses Cache-Aside with singleflight to handle high-concurrency lookups
func (tm *TierManager) GetTier(ctx context.Context, prefixID, apiKey string) (string, error) {
	cacheKey := tm.getCacheKey(prefixID, apiKey)

	// 1. Check Redis Cache
	val, err := tm.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		return val, nil
	}

	// 2. Deduplicate concurrent DB hits for the same user
	result, err, _ := tm.group.Do(cacheKey, func() (interface{}, error) {
		var doc struct {
			TierID string `bson:"tier_id"`
		}

		filter := bson.M{"prefix_id": prefixID, "api_key": apiKey}
		err := tm.collection.FindOne(ctx, filter).Decode(&doc)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return "", nil // Or a default "free" tier
			}
			return "", err
		}

		// 3. Backfill Cache
		tm.rdb.Set(ctx, cacheKey, doc.TierID, cacheTTL)
		return doc.TierID, nil
	})

	if err != nil {
		return "", err
	}
	return result.(string), nil
}

// DeleteTier removes the mapping and clears the cache
func (tm *TierManager) DeleteTier(ctx context.Context, prefixID, apiKey string) error {
	filter := bson.M{"prefix_id": prefixID, "api_key": apiKey}
	_, err := tm.collection.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}
	return tm.rdb.Del(ctx, tm.getCacheKey(prefixID, apiKey)).Err()
}

func ensureTierIndexes(ctx context.Context, coll *mongo.Collection) error {
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "prefix_id", Value: 1},
			{Key: "api_key", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}

	_, err := coll.Indexes().CreateOne(ctx, indexModel)
	return err
}
