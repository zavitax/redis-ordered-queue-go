package redisOrderedQueue

import (
	"github.com/go-redis/redis/v8"
	"github.com/sahmad98/go-ringbuffer"
	"fmt"
	"context"
	"encoding/json"
	"time"
	"sync/atomic"
	"sync"
	"strings"
	"strconv"
)

type RedisQueueClient interface {
	StartConsumers (ctx context.Context) (error)
	StopConsumers (ctx context.Context) (error)
	Close () (error)
	GetMetrics (ctx context.Context, options *GetMetricsOptions) (*Metrics, error)
	Send (ctx context.Context, data interface{}, priority int, groupId string) (error)
}

type redisQueueWireMessage struct {
	Timestamp int64      		`json:"t"`
	Producer	int64     		`json:"c"`
	Sequence	int64      		`json:"s"`
	Data			*interface{}	`json:"d"`
};

type MessageMetadata struct {
	MessageContext struct {
		Timestamp	time.Time
		Producer	int64
		Sequence	int64
		Latency		time.Duration
		Lock			*lockHandle
	}
}

type redisQueueClient struct {
	mu sync.RWMutex

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

func NewClient (ctx context.Context, options* Options) (RedisQueueClient, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	var c = &redisQueueClient{}

	c.options = options
	c.redis = redis.NewClient(c.options.RedisOptions)

	redisKeyFormat := "%s::%s"
  c.groupStreamKey = fmt.Sprintf(redisKeyFormat, c.options.RedisKeyPrefix, "msg-group-stream")
  c.groupSetKey = fmt.Sprintf(redisKeyFormat, c.options.RedisKeyPrefix, "msg-group-set")
  c.clientIndexKey = fmt.Sprintf(redisKeyFormat, c.options.RedisKeyPrefix, "consumer-index-sequence")
  c.consumerGroupId = fmt.Sprintf(redisKeyFormat, c.options.RedisKeyPrefix, "consumer-group")
  c.messagePriorityQueueKeyPrefix = fmt.Sprintf(redisKeyFormat, c.options.RedisKeyPrefix, "msg-group-queue")

	c.lastMessageSequenceNumber = 0

	c.statLastMessageLatencies = ringbuffer.NewRingBuffer(100)

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

func (c *redisQueueClient) processInvalidMessage (ctx context.Context, msgData *string) (error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	atomic.AddInt64(&c.statTotalInvalidMessagesCount, 1)

	if (c.options.HandleInvalidMessage != nil) {
		return c.options.HandleInvalidMessage(ctx, msgData)
	}

	return nil
}

func (c *redisQueueClient) processMessage (ctx context.Context, lock *lockHandle, msgData string) (error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var packet redisQueueWireMessage
	var err error

	if err = json.Unmarshal([]byte(msgData), &packet); err != nil {
		return c.processInvalidMessage(ctx, &msgData);
	}

	var meta MessageMetadata

	meta.MessageContext.Timestamp = time.UnixMilli(packet.Timestamp).UTC()
	meta.MessageContext.Producer = packet.Producer
	meta.MessageContext.Sequence = packet.Sequence
	meta.MessageContext.Latency = time.Now().UTC().Sub(meta.MessageContext.Timestamp)
	meta.MessageContext.Lock = lock

	c.statLastMessageLatencies.Write(meta.MessageContext.Latency)

	return c.options.HandleMessage(ctx, packet.Data, &meta)
}

func (c *redisQueueClient) Close () (error) {
	c.StopConsumers(context.TODO())

	return c.redis.Close()
}

func (c *redisQueueClient) Send (ctx context.Context, data interface{}, priority int, groupId string) (error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

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
	c.mu.Lock()

	if (len(c.consumerWorkers) > 0) { return fmt.Errorf("Consumers already started"); }

	for i := 0; i < c.options.ConsumerCount; i++ {	
		worker, err := newWorker(ctx, c)

		if (err != nil) {
			// Stop consumers which have been started
			c.mu.Unlock()
			c.StopConsumers(ctx)

			return err
		}

		context, cancelFunc := context.WithCancel(ctx)

		go worker.run(context)

		c.consumerCancellationFunctions = append(c.consumerCancellationFunctions, &cancelFunc)
		c.consumerWorkers = append(c.consumerWorkers, worker)
	}

	c.mu.Unlock()

	return nil
}

func (c *redisQueueClient) StopConsumers (ctx context.Context) (error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if (len(c.consumerWorkers) == 0) { return fmt.Errorf("Consumers are not running"); }

	for _, cancelFunc := range(c.consumerCancellationFunctions) {
		(*cancelFunc)();
	}

	c.consumerCancellationFunctions = []*context.CancelFunc {}
	c.consumerWorkers = []*redisQueueWorker {}

	return nil
}

func (c *redisQueueClient) getMetricsParseTopMessageGroups (result *Metrics, data []interface{}) {
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
}

func (c *redisQueueClient) getMetricsParseLatencies (result *Metrics) {
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
}

func (c *redisQueueClient) GetMetrics (ctx context.Context, options *GetMetricsOptions) (*Metrics, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

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

	c.getMetricsParseTopMessageGroups(result, data)
	c.getMetricsParseLatencies(result)

	return result, nil
}
