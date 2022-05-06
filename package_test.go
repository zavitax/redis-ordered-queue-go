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

func TestFlow (t *testing.T) {
	var redisOptions = &redis.Options{
		Addr: "127.0.0.1:6379",
		Password: "",
		DB: 0,
	};

	msgCount := int64(0)

	var opt = &redisOrderedQueue.Options{
		RedisOptions: redisOptions,
		BatchSize: 10,
		GroupVisibilityTimeout: time.Second * 5,
		PollingTimeout: time.Second * 1,
		ConsumerCount: 5000,
		RedisKeyPrefix: "{redis-ordered-queue}",
		HandleMessage: func (ctx context.Context, data *interface{}, meta *redisOrderedQueue.MessageMetadata) (error) {
			//fmt.Printf("handleMessage: data %v, meta %v\n", data, meta)
			numMsgs := atomic.AddInt64(&msgCount, 1)

			if (numMsgs % 1000 == 0) {
				fmt.Printf("handleMessage: %v messages processed\n", numMsgs)
			}

			time.Sleep(time.Millisecond * 1500)

			return nil
		},
		HandleInvalidMessage: func (ctx context.Context, data *string) (error) {
			fmt.Printf("handleInvalidMessage: data %v\n", *data)

			return nil
		},
	}

	var client, err = redisOrderedQueue.NewClient(context.TODO(), opt);

	if (err != nil) { t.Errorf("NewClient: %v", err); return }

	metricsLoop := func () {
		opt := &redisOrderedQueue.GetMetricsOptions{
			TopMessageGroupsLimit: 10,
		}

		for {
			met, _ := client.GetMetrics(context.TODO(), opt)
			fmt.Printf("Metrics: g: %v, msgs: %v, conc: %v, topbklg: %v, min: %v, avg: %v, max: %v, %v\n",
				met.TrackedMessageGroups,
				met.VisibleMessages,
				met.WorkingConsumers,
				met.TopMessageGroupsMessageBacklogLength,
				met.MinLatency,
				met.AvgLatency,
				met.MaxLatency,
				met)
			time.Sleep(time.Second * 1)
		}
	};

	go metricsLoop();

	for i := 0; i < 1000; i++ {
		fmt.Printf(".")
		client.Send(context.TODO(), "test", 1, fmt.Sprintf("GO-GROUP-%v", i));
	}

	client.StartConsumers(context.TODO())
	time.Sleep(time.Second * 60 * 5);
	client.StopConsumers(context.TODO())
}
