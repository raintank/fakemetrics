package kafkamdm

import (
	"fmt"
	"hash"
	"hash/fnv"
	"time"

	"github.com/Shopify/sarama"
	p "github.com/grafana/metrictank/cluster/partitioner"
	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/met"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type KafkaMdm struct {
	out.OutStats
	topic   string
	brokers []string
	config  *sarama.Config
	client  sarama.SyncProducer
	hash    hash.Hash32
	part    *p.Kafka
}

func New(topic string, brokers []string, codec string, stats met.Backend, partitionScheme string) (*KafkaMdm, error) {
	// We are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = out.GetCompression(codec)
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	var partitioner *p.Kafka
	switch partitionScheme {
	case "byOrg":
		partitioner, err = p.NewKafka("byOrg")
	case "bySeries":
		partitioner, err = p.NewKafka("bySeries")
	default:
		err = fmt.Errorf("partitionScheme must be one of 'byOrg|bySeries'. got %s", partitionScheme)
	}
	if err != nil {
		return nil, err
	}

	return &KafkaMdm{
		OutStats: out.NewStats(stats, "kafka-mdm"),
		topic:    topic,
		brokers:  brokers,
		config:   config,
		client:   client,
		part:     partitioner,
		hash:     fnv.New32a(),
	}, nil
}

func (k *KafkaMdm) Close() error {
	return k.client.Close()
}

func (k *KafkaMdm) Flush(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		k.FlushDuration.Value(0)
		return nil
	}
	preFlush := time.Now()

	k.MessageMetrics.Value(1)
	var data []byte

	payload := make([]*sarama.ProducerMessage, len(metrics))

	for i, metric := range metrics {
		data, err := metric.MarshalMsg(data[:])
		if err != nil {
			return err
		}

		k.MessageBytes.Value(int64(len(data)))

		key, err := k.part.GetPartitionKey(metric, nil)
		if err != nil {
			log.Fatal(4, "Failed to get partition for metric. %s", err)
		}

		payload[i] = &sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(key),
			Topic: k.topic,
			Value: sarama.ByteEncoder(data),
		}

	}
	prePub := time.Now()
	err := k.client.SendMessages(payload)
	if err != nil {
		k.PublishErrors.Inc(1)
		if errors, ok := err.(sarama.ProducerErrors); ok {
			for i := 0; i < 10 && i < len(errors); i++ {
				log.Error(4, "ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
			}
		}
		return err
	}

	k.PublishedMessages.Inc(int64(len(metrics)))
	k.PublishDuration.Value(time.Since(prePub))
	k.PublishedMetrics.Inc(int64(len(metrics)))
	k.FlushDuration.Value(time.Since(preFlush))
	return nil
}
