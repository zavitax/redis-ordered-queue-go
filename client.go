package redisOrderedQueue

import (
	"github.com/go-redis/redis/v8"
	"github.com/sahmad98/go-ringbuffer"
	"fmt"
	"context"
	"encoding/json"
	"time"
	"sync/atomic"
	"strings"
	"strconv"
)

type redisQueueWireMessage struct {
	Timestamp int64      		`json:"t"`
	Producer	int64     		`json:"c"`
	Sequence	int64      		`json:"s"`
	Data			*interface{}	`json:"d"`
};

type MessageMetadata struct {
	context		struct {
		timestamp	time.Time
		producer	int64
		sequence	int64
		latency		time.Duration
		lock			*lockHandle
	}
}

type redisQueueClient struct {
	options* Options
	redis* redis.Client

  groupStreamKey string
  groupSetKey string
  clientIndexKey string
  consumerGroupId string
	messagePriorityQueueKeyPrefix string
	clientId int64
	lastMessageSequenceNumber int64
	
	consumerWorkers []*redisQueueWorker
	consumerCancellationFunctions []*context.CancelFunc

	callGetMetrics redisScriptCall
  callAddGroupAndMessageToQueue redisScriptCall

	statTotalInvalidMessagesCount int64
	statLastMessageLatencies *ringbuffer.RingBuffer
}

func NewClient (ctx context.Context, options* Options) (*redisQueueClient, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	var c = &redisQueueClient{}

	c.options = options
	c.redis = redis.NewClient(c.options.redisOptions)

  c.groupStreamKey = fmt.Sprintf("%s::%s", c.options.redisKeyPrefix, "msg-group-stream")
  c.groupSetKey = fmt.Sprintf("%s::%s", c.options.redisKeyPrefix, "msg-group-set")
  c.clientIndexKey = fmt.Sprintf("%s::%s", c.options.redisKeyPrefix, "consumer-index-sequence")
  c.consumerGroupId = fmt.Sprintf("%s::%s", c.options.redisKeyPrefix, "consumer-group")
  c.messagePriorityQueueKeyPrefix = fmt.Sprintf("%s::%s", c.options.redisKeyPrefix, "msg-group-queue")

	c.lastMessageSequenceNumber = 0

	var err error

	if c.clientId, err = c.createClientId(ctx); err != nil {
		return nil, err
	}

	// Prepare Redis scripts calls
	if c.callGetMetrics, err = newScriptCall(ctx, c.redis, scriptGetMetrics); err != nil { c.redis.Close(); return nil, err }
	if c.callAddGroupAndMessageToQueue, err = newScriptCall(ctx, c.redis, scriptAddGroupAndMessageToQueue); err != nil { c.redis.Close(); return nil, err }

	// Ensure stream group exists
	if _, err = c.redis.Do(ctx, "XGROUP", "CREATE", c.groupStreamKey, c.consumerGroupId, 0, "MKSTREAM").Result(); err != nil {
		if (!strings.HasPrefix(err.Error(), "BUSYGROUP")) {
			return nil, err
		}
	}

	return c, nil
}

func (c *redisQueueClient) createClientId (ctx context.Context) (int64, error) {
	return c.redis.Incr(ctx, c.clientIndexKey).Result()
}

func (c* redisQueueClient) createPriorityMessageQueueKey (groupId string) (string) {
	return fmt.Sprintf("%s::%s", c.messagePriorityQueueKeyPrefix, groupId);
}

func (c *redisQueueClient) process_invalid_message (ctx context.Context, msgData *string) (error) {
	c.statTotalInvalidMessagesCount++

	if (c.options.handleInvalidMessage != nil) {
		return c.options.handleInvalidMessage(ctx, msgData)
	}

	return nil
}

func (c *redisQueueClient) process_message (ctx context.Context, lock *lockHandle, msgData string) (error) {
	var packet redisQueueWireMessage
	var err error

	if err = json.Unmarshal([]byte(msgData), &packet); err != nil {
		return c.process_invalid_message(ctx, &msgData);
	}

	var meta MessageMetadata

	meta.context.timestamp = time.UnixMilli(packet.Timestamp).UTC()
	meta.context.producer = packet.Producer
	meta.context.sequence = packet.Sequence
	meta.context.latency = time.Now().UTC().Sub(meta.context.timestamp)
	meta.context.lock = lock

	c.statLastMessageLatencies.Write(meta.context.latency)

	return c.options.handleMessage(ctx, packet.Data, &meta)
}

func (c *redisQueueClient) Send (ctx context.Context, data interface{}, priority int, groupId string) (error) {
	var packet redisQueueWireMessage

	packet.Timestamp = time.Now().UTC().UnixMilli()
	packet.Producer = c.clientId
	packet.Sequence = atomic.AddInt64(&c.lastMessageSequenceNumber, 1)
	packet.Data = &data

	var jsonString, err = json.Marshal(packet)
	
	if (err != nil) {
		return err
	}

	_, err = c.callAddGroupAndMessageToQueue(ctx, c.redis,
		[]interface{} { groupId, priority, jsonString },
		[]string { c.groupStreamKey, c.groupSetKey, c.createPriorityMessageQueueKey(groupId) },
	).Result();

	return err
}

func (c *redisQueueClient) StartConsumers (ctx context.Context) (error) {
	if (len(c.consumerWorkers) > 0) { return fmt.Errorf("Consumers already started"); }

	c.statLastMessageLatencies = ringbuffer.NewRingBuffer(100)

	for i := 0; i < c.options.consumerCount; i++ {	
		worker, err := newWorker(ctx, c)

		if (err != nil) {
			// Stop consumers which have been started
			c.StopConsumers(ctx)

			return err
		}

		context, cancelFunc := context.WithCancel(ctx)

		go worker.run(context)

		c.consumerCancellationFunctions = append(c.consumerCancellationFunctions, &cancelFunc)
		c.consumerWorkers = append(c.consumerWorkers, worker)
	}

	return nil
}

func (c *redisQueueClient) StopConsumers (ctx context.Context) (error) {
	if (len(c.consumerWorkers) == 0) { return fmt.Errorf("Consumers are not running"); }

	for _, cancelFunc := range(c.consumerCancellationFunctions) {
		(*cancelFunc)();
	}

	c.consumerCancellationFunctions = []*context.CancelFunc {}
	c.consumerWorkers = []*redisQueueWorker {}

	return nil
}

func (c *redisQueueClient) GetMetrics (ctx context.Context, options *GetMetricsOptions) (*Metrics, error) {
	data, err := c.callGetMetrics(ctx, c.redis, 
		[]interface{} { c.consumerGroupId, options.TopMessageGroupsLimit },
		[]string { c.groupStreamKey, c.groupSetKey },
	).Slice()

	if (err != nil) { return nil, err }

	result := &Metrics{
		BufferedMessageGroups: data[0].(int64),
		TrackedMessageGroups: data[1].(int64),
		WorkingConsumers: data[2].(int64),
		VisibleMessages: data[3].(int64),
		InvalidMessages: c.statTotalInvalidMessagesCount,
		TopMessageGroupsMessageBacklogLength: 0,
	}

	if list, ok := data[4].([]interface{}); ok {
		for i := 0; i < len(list); i += 2 {
			if backlog, err := strconv.ParseInt(list[i + 1].(string), 10, 0); err == nil {
				result.TopMessageGroups = append(result.TopMessageGroups, &MessageGroupMetrics{
					Group: list[i].(string),
					Backlog: backlog,
				})

				result.TopMessageGroupsMessageBacklogLength += backlog
			}
		}
	}

	latencies := make([]interface{}, c.statLastMessageLatencies.Size)
	copy(latencies, c.statLastMessageLatencies.Container)

	var sumLatencyMs int64
	var minLatencyMs int64 = 0
	var maxLatencyMs int64 = 0
	var numLatencies int64 = 0

	if (len(latencies) > 0 && latencies[0] != nil) {
		minLatencyMs = latencies[0].(time.Duration).Milliseconds()
	}

	for _, latency := range(latencies) {
		if (latency != nil) {
			numLatencies++

			ms := latency.(time.Duration).Milliseconds()
			sumLatencyMs += ms
			if (ms < minLatencyMs) { minLatencyMs = ms; }
			if (ms > maxLatencyMs) { maxLatencyMs = ms; }
		}
	}

	result.MinLatency = time.Duration(minLatencyMs) * time.Millisecond
	result.MaxLatency = time.Duration(maxLatencyMs) * time.Millisecond

	if (numLatencies > 0) {
		result.AvgLatency = time.Duration(sumLatencyMs / numLatencies) * time.Millisecond
	} else {
		result.AvgLatency = time.Duration(0)
	}

	return result, nil
}
