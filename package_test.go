package redisOrderedQueue

import (
	"github.com/go-redis/redis/v8"
	"context"
	"testing"
	"time"
	"fmt"
	"sync/atomic"
)

func TestFlow (t *testing.T) {
	var redisOptions = &redis.Options{
		Addr: "127.0.0.1:6379",
		Password: "",
		DB: 0,
	};

	msgCount := int64(0)

	var opt = &Options{
		redisOptions: redisOptions,
		batchSize: 10,
		groupVisibilityTimeout: time.Second * 5,
		pollingTimeout: time.Second * 1,
		consumerCount: 5000,
		redisKeyPrefix: "{redis-ordered-queue}",
		handleMessage: func (ctx context.Context, data *interface{}, meta *MessageMetadata) (error) {
			//fmt.Printf("handleMessage: data %v, meta %v\n", data, meta)
			numMsgs := atomic.AddInt64(&msgCount, 1)

			if (numMsgs % 100 == 0) {
				fmt.Printf("handleMessage: %v messages processed\n", numMsgs)
			}

			return nil
		},
		handleInvalidMessage: func (ctx context.Context, data *string) (error) {
			fmt.Printf("handleInvalidMessage: data %v\n", *data)

			return nil
		},
	}

	var client, err = NewClient(context.TODO(), opt);

	if (err != nil) { t.Errorf("NewClient: %v", err); return }

	metrics_loop := func () {
		opt := &GetMetricsOptions{
			TopMessageGroupsLimit: 10,
		}

		for {
			met, _ := client.GetMetrics(context.TODO(), opt)
			fmt.Printf("Metrics: %v\n", met)
			time.Sleep(time.Second * 5)
		}
	};

	go metrics_loop();

	client.StartConsumers(context.TODO())
	time.Sleep(time.Second * 60 * 5);
	client.StopConsumers(context.TODO())

	/*
	for i := 0; i < 1000; i++ {
		fmt.Printf(".")
		client.Send(context.TODO(), "test", 1, fmt.Sprintf("GO-GROUP-%v", i));
	}
	time.Sleep(time.Second);*/
}
