package redisOrderedQueue_test

import (
	"github.com/go-redis/redis/v8"
	"context"
	"testing"
	"time"
	"fmt"
	"sync/atomic"
	"github.com/zavitax/redis-ordered-queue-go"
)

var redisOptions = &redis.Options{
	Addr: "127.0.0.1:6379",
	Password: "",
	DB: 0,
};

func createQueueOptions (
	handleMessage func (ctx context.Context, data *interface{}, meta *redisOrderedQueue.MessageMetadata) (error),
	handleInvalidMessage func (ctx context.Context, data *string) (error),
) (*redisOrderedQueue.Options) {
	result := &redisOrderedQueue.Options{
		RedisOptions: redisOptions,
		BatchSize: 10,
		GroupVisibilityTimeout: time.Second * 5,
		PollingTimeout: time.Second * 1,
		ConsumerCount: 10,
		RedisKeyPrefix: "{redis-ordered-queue}",
		HandleMessage: handleMessage,
		HandleInvalidMessage: handleInvalidMessage,
	}

	if (result.HandleMessage == nil) {
		result.HandleMessage = func (ctx context.Context, data *interface{}, meta *redisOrderedQueue.MessageMetadata) (error) {
			return nil
		}
	}

	return result
}

func createQueueClient (options *redisOrderedQueue.Options) (redisOrderedQueue.RedisQueueClient, error) {
	return redisOrderedQueue.NewClient(context.TODO(), options);
}

func TestConnectDisconnect (t *testing.T) {
	client, err := createQueueClient(createQueueOptions(nil, nil))

	if (err != nil) { t.Error(err); return }

	client.Close()
}

func TestSendReceive (t *testing.T) {
	var message = "test message content"
	var minReceivedMsgCount = int64(1)
	var receivedMsgCount int64

	options := createQueueOptions(
		func (ctx context.Context, data *interface{}, meta *redisOrderedQueue.MessageMetadata) (error) {
			if (data == nil) {
				t.Error("Received nil data");
				return nil
			}

			strData := (*data).(string)
			if (strData != message) {
				t.Errorf("Expected '%v' but received '%v'", message, strData)
				return nil
			}

			atomic.AddInt64(&receivedMsgCount, 1)

			return nil
		},
		nil,
	)

	client, err := createQueueClient(options)

	if (err != nil) { t.Error(err); return }

	client.Send(context.TODO(), message, 1, fmt.Sprintf("GO-GROUP-%v", 1));
	client.StartConsumers(context.TODO())

	for i := 0; i < 10 && receivedMsgCount < minReceivedMsgCount; i++ {
		time.Sleep(time.Second * 1)
	}

	client.StopConsumers(context.TODO())

	client.Close()

	if (receivedMsgCount < minReceivedMsgCount) {
		t.Errorf("Expected %v receivedMsgCount but received %v", minReceivedMsgCount, receivedMsgCount)
	}
}

func TestGroupVisibilityTimeoutRetry (t *testing.T) {
	var message = "test message content"
	var tryCountLimit = int64(3)
	var minReceivedMsgCount = int64(1)
	var retryCount int64
	var receivedMsgCount int64

	options := createQueueOptions(
		func (ctx context.Context, data *interface{}, meta *redisOrderedQueue.MessageMetadata) (error) {
			tryNum := atomic.AddInt64(&retryCount, 1)

			if (tryNum >= tryCountLimit) {
				atomic.AddInt64(&receivedMsgCount, 1)

				return nil
			}

			// Long sleep so GroupVisibilityTimeout will expire
			time.Sleep(time.Second * 10)

			return nil
		},
		nil,
	)

	options.GroupVisibilityTimeout = time.Second * 1

	client, err := createQueueClient(options)

	if (err != nil) { t.Error(err); return }

	client.Send(context.TODO(), message, 1, fmt.Sprintf("GO-GROUP-%v", 1));
	client.StartConsumers(context.TODO())

	for i := 0; i < 10 && receivedMsgCount < minReceivedMsgCount; i++ {
		time.Sleep(time.Second * 1)
	}

	client.StopConsumers(context.TODO())

	client.Close()

	if (receivedMsgCount < minReceivedMsgCount) {
		t.Errorf("Expected %v receivedMsgCount but received %v", minReceivedMsgCount, receivedMsgCount)
	}
}

func TestGetMetrics (t *testing.T) {
	var message = "test message content"
	var minReceivedMsgCount = int64(1)
	var receivedMsgCount int64

	options := createQueueOptions(
		func (ctx context.Context, data *interface{}, meta *redisOrderedQueue.MessageMetadata) (error) {
			if (data == nil) {
				t.Error("Received nil data");
				return nil
			}

			strData := (*data).(string)
			if (strData != message) {
				t.Errorf("Expected '%v' but received '%v'", message, strData)
				return nil
			}

			atomic.AddInt64(&receivedMsgCount, 1)

			return nil
		},
		nil,
	)

	client, err := createQueueClient(options)

	if (err != nil) { t.Error(err); return }

	client.Send(context.TODO(), message, 1, fmt.Sprintf("GO-GROUP-%v", 1));

	getMetricsOptions := &redisOrderedQueue.GetMetricsOptions{
		TopMessageGroupsLimit: 10,
	}

	metrics, err := client.GetMetrics(context.TODO(), getMetricsOptions)

	if (err != nil) {
		t.Error(err)
	}

	if (metrics.TrackedMessageGroups < 1) {
		t.Errorf("Expected 1 metrics.TrackedMessageGroups but received %v", metrics.TrackedMessageGroups)
	}

	if (metrics.VisibleMessages < 1) {
		t.Errorf("Expected 1 metrics.VisibleMessages but received %v", metrics.VisibleMessages)
	}

	if (metrics.TopMessageGroupsMessageBacklogLength < 1) {
		t.Errorf("Expected 1 metrics.TopMessageGroupsMessageBacklogLength but received %v", metrics.TopMessageGroupsMessageBacklogLength)
	}

	client.StartConsumers(context.TODO())

	for i := 0; i < 10 && receivedMsgCount < minReceivedMsgCount; i++ {
		time.Sleep(time.Second * 1)
	}

	client.StopConsumers(context.TODO())

	metrics, err = client.GetMetrics(context.TODO(), getMetricsOptions)

	if (err != nil) {
		t.Error(err)
	}

	if (metrics.TrackedMessageGroups > 0) {
		t.Errorf("Expected 0 metrics.TrackedMessageGroups but received %v", metrics.TrackedMessageGroups)
	}

	if (metrics.VisibleMessages > 0) {
		t.Errorf("Expected 0 metrics.VisibleMessages but received %v", metrics.VisibleMessages)
	}

	if (metrics.TopMessageGroupsMessageBacklogLength > 0) {
		t.Errorf("Expected 0 metrics.TopMessageGroupsMessageBacklogLength but received %v", metrics.TopMessageGroupsMessageBacklogLength)
	}

	client.Close()
}
