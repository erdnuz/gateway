package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type KafkaDeltaConsumer struct {
	rdb    *redis.Client
	reader *kafka.Reader
	apply  func(ctx context.Context, prefix, apiKey string, delta int64) error
}

type DeltaConsumer interface {
	Start(ctx context.Context) error
}

func NewKafkaDeltaConsumer(rdb *redis.Client, brokers []string, topic, groupID string, apply func(context.Context, string, string, int64) error) *KafkaDeltaConsumer {
	clean := make([]string, 0, len(brokers))
	for _, b := range brokers {
		v := strings.TrimSpace(b)
		if v != "" {
			clean = append(clean, v)
		}
	}
	if len(clean) == 0 {
		clean = []string{"localhost:9092"}
	}
	if strings.TrimSpace(topic) == "" {
		topic = "rate-updates"
	}
	if strings.TrimSpace(groupID) == "" {
		groupID = "hub-rate-consumers"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  clean,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &KafkaDeltaConsumer{rdb: rdb, reader: reader, apply: apply}
}

func (c *KafkaDeltaConsumer) Start(ctx context.Context) error {
	go func() {
		defer func() { _ = c.reader.Close() }()
		for {
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("hub kafka read failed: %v", err)
				continue
			}

			var ud types.UsageDelta
			if err := json.Unmarshal(msg.Value, &ud); err != nil {
				continue
			}
			if ud.Delta <= 0 {
				continue
			}
			if ud.Seq > 0 {
				ok, err := c.acceptSeq(ctx, ud.Prefix, ud.APIKey, ud.Seq)
				if err != nil {
					log.Printf("hub kafka seq check failed prefix=%s api_key=%s seq=%d err=%v", ud.Prefix, ud.APIKey, ud.Seq, err)
					continue
				}
				if !ok {
					continue
				}
			}
			if err := c.apply(ctx, ud.Prefix, ud.APIKey, ud.Delta); err != nil {
				log.Printf("hub kafka delta apply failed prefix=%s api_key=%s delta=%d seq=%d err=%v", ud.Prefix, ud.APIKey, ud.Delta, ud.Seq, err)
			}
		}
	}()
	return nil
}

func (c *KafkaDeltaConsumer) acceptSeq(ctx context.Context, prefix, apiKey string, seq int64) (bool, error) {
	lastKey := fmt.Sprintf("rate-last-seq:%s:%s", prefix, apiKey)
	last, err := c.rdb.Get(ctx, lastKey).Int64()
	if err != nil && err != redis.Nil {
		return false, err
	}
	if seq <= last {
		return false, nil
	}
	if err := c.rdb.Set(ctx, lastKey, seq, 0).Err(); err != nil {
		return false, err
	}
	return true, nil
}
