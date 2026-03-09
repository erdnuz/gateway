package edge

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"gateway/packages/common/types"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type RateSync interface {
	FlushPendingDelta(ctx context.Context, prefix, apiKey string)
	StartSOTSubscriber(ctx context.Context) error
}

type KafkaRateSync struct {
	rdb       *redis.Client
	topic     string
	writer    *kafka.Writer
	sotReader *kafka.Reader
}

func NewKafkaRateSync(rdb *redis.Client, brokers []string, topic, sotTopic, groupID string) *KafkaRateSync {
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
	if strings.TrimSpace(sotTopic) == "" {
		sotTopic = "rate-sot"
	}
	if strings.TrimSpace(groupID) == "" {
		groupID = "edge-rate-sot"
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(clean...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 20 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  clean,
		GroupID:  groupID,
		Topic:    sotTopic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &KafkaRateSync{rdb: rdb, topic: topic, writer: writer, sotReader: reader}
}

func (s *KafkaRateSync) FlushPendingDelta(ctx context.Context, prefix, apiKey string) {
	pendingKey := ratePendingKey(prefix, apiKey)
	delta, err := s.rdb.GetSet(ctx, pendingKey, 0).Int64()
	if err == redis.Nil || delta <= 0 {
		return
	}
	if err != nil {
		log.Printf("kafka rate sync getset failed prefix=%s api_key=%s err=%v", prefix, apiKey, err)
		return
	}

	seq, err := s.rdb.Incr(ctx, rateSeqKey(prefix, apiKey)).Result()
	if err != nil {
		_ = s.rdb.IncrBy(ctx, pendingKey, delta).Err()
		log.Printf("kafka rate sync seq increment failed prefix=%s api_key=%s err=%v", prefix, apiKey, err)
		return
	}

	ud := types.UsageDelta{Prefix: prefix, APIKey: apiKey, Delta: delta, Seq: seq}
	b, err := json.Marshal(ud)
	if err != nil {
		_ = s.rdb.IncrBy(ctx, pendingKey, delta).Err()
		log.Printf("kafka rate sync marshal failed prefix=%s api_key=%s err=%v", prefix, apiKey, err)
		return
	}

	msg := kafka.Message{Key: []byte(prefix + ":" + apiKey), Value: b}
	if err := s.writer.WriteMessages(ctx, msg); err != nil {
		_ = s.rdb.IncrBy(ctx, pendingKey, delta).Err()
		log.Printf("kafka rate sync publish failed prefix=%s api_key=%s seq=%d err=%v", prefix, apiKey, seq, err)
	}
}

func (s *KafkaRateSync) StartSOTSubscriber(ctx context.Context) error {
	if s.sotReader == nil {
		return nil
	}
	go func() {
		defer func() { _ = s.sotReader.Close() }()
		for {
			msg, err := s.sotReader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("edge kafka sot read failed: %v", err)
				continue
			}
			var sot types.SOTMsg
			if err := json.Unmarshal(msg.Value, &sot); err != nil {
				continue
			}
			syncKey := rateSyncKey(sot.Prefix, sot.APIKey)
			if err := s.rdb.Set(ctx, syncKey, sot.Total, 0).Err(); err != nil {
				log.Printf("edge kafka sot sync set failed prefix=%s api_key=%s err=%v", sot.Prefix, sot.APIKey, err)
			}
		}
	}()
	return nil
}
