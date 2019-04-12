package kafkamdm

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	p "github.com/grafana/metrictank/cluster/partitioner"
	"github.com/raintank/fakemetrics/out"
	"github.com/raintank/fakemetrics/out/kafkamdm/keycache"
	"github.com/raintank/met"
	"github.com/raintank/schema"
	"github.com/raintank/schema/msg"
	log "github.com/sirupsen/logrus"
)

type KafkaMdm struct {
	out.OutStats
	topic      string
	brokers    []string
	config     *sarama.Config
	client     sarama.SyncProducer
	hash       hash.Hash32
	part       *p.Kafka
	lmPart     LastNumPartitioner
	partScheme string
	v2         bool
	keyCache   *keycache.KeyCache
}

// map the last number in the metricname to the partition
// needless to say, caller beware of how many/which partitions there are
type LastNumPartitioner struct{}

func (p *LastNumPartitioner) Partition(m schema.PartitionedMetric, numPartitions int32) (int32, error) {
	name := m.KeyBySeries(nil)
	index := bytes.LastIndexByte(name, '.')
	if index < 0 {
		return 0, fmt.Errorf("invalid metricname for LastNumPartitioner: '%s'", name)
	}
	part, err := strconv.ParseInt(string(name[index:]), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid metricname for LastNumPartitioner: '%s'", name)
	}
	return int32(part), nil
}

// key is by metric name, but won't be used for partition setting
func (p *LastNumPartitioner) GetPartitionKey(m schema.PartitionedMetric, b []byte) ([]byte, error) {
	return m.KeyBySeries(b), nil
}

func New(topic string, brokers []string, codec string, stats met.Backend, partitionScheme string, v2 bool) (*KafkaMdm, error) {
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
	var part *p.Kafka
	var lmPart LastNumPartitioner
	switch partitionScheme {
	case "byOrg":
		part, err = p.NewKafka("byOrg")
	case "bySeries":
		part, err = p.NewKafka("bySeries")
	case "lastNum":
		lmPart = LastNumPartitioner{}
		// sets partition based on message partition field
		config.Producer.Partitioner = sarama.NewManualPartitioner
	default:
		err = fmt.Errorf("partitionScheme must be one of 'byOrg|bySeries|lastNum'. got %s", partitionScheme)
	}
	if err != nil {
		return nil, err
	}
	k := &KafkaMdm{
		OutStats:   out.NewStats(stats, "kafka-mdm"),
		topic:      topic,
		brokers:    brokers,
		config:     config,
		client:     client,
		hash:       fnv.New32a(),
		part:       part,
		lmPart:     lmPart,
		partScheme: partitionScheme,
		v2:         v2,
	}
	if v2 {
		k.keyCache = keycache.NewKeyCache(20*time.Minute, time.Duration(10)*time.Minute)
	}
	return k, nil
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

	payload := make([]*sarama.ProducerMessage, len(metrics))
	var notOk int

	for i, metric := range metrics {
		var data []byte
		var err error
		if k.v2 {
			var mkey schema.MKey
			mkey, err = schema.MKeyFromString(metric.Id)
			if err != nil {
				return err
			}
			ok := k.keyCache.Touch(mkey, preFlush)
			// we've seen this key recently. we can use the optimized format
			if ok {
				mp := schema.MetricPoint{
					MKey:  mkey,
					Value: metric.Value,
					Time:  uint32(metric.Time),
				}
				data = []byte{byte(msg.FormatMetricPoint)}
				data, err = mp.Marshal(data)

			} else {
				notOk++
				data, err = metric.MarshalMsg(data[:])
			}
		} else {
			data, err = metric.MarshalMsg(data[:])
		}
		if err != nil {
			return err
		}

		k.MessageBytes.Value(int64(len(data)))

		if k.partScheme == "lastNum" {
			part, err := k.lmPart.Partition(metric, 0)
			if err != nil {
				return fmt.Errorf("Failed to get partition for metric. %s", err)
			}

			payload[i] = &sarama.ProducerMessage{
				Partition: part,
				Topic:     k.topic,
				Value:     sarama.ByteEncoder(data),
			}
		} else {
			key, err := k.part.GetPartitionKey(metric, nil)
			if err != nil {
				return fmt.Errorf("Failed to get partition for metric. %s", err)
			}

			payload[i] = &sarama.ProducerMessage{
				Key:   sarama.ByteEncoder(key),
				Topic: k.topic,
				Value: sarama.ByteEncoder(data),
			}
		}

	}
	if notOk > 0 {
		fmt.Println(preFlush, notOk, "metrics could not be sent as v2 MetricPoint")
	}
	prePub := time.Now()
	err := k.client.SendMessages(payload)
	if err != nil {
		k.PublishErrors.Inc(1)
		if errors, ok := err.(sarama.ProducerErrors); ok {
			for i := 0; i < 10 && i < len(errors); i++ {
				log.Errorf("ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
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
