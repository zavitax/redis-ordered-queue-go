package redisOrderedQueue

import (
	"github.com/go-redis/redis/v8"
	"context"
	"testing"
	"time"
	"fmt"
)

func TestFlow (t *testing.T) {
	var redisOptions = &redis.Options{
		Addr: "localhost:6379",
		Password: "",
		DB: 0,
	};

	var opt = &Options{
		redisOptions: redisOptions,
		batchSize: 10,
		groupVisibilityTimeout: time.Minute,
		pollingTimeout: time.Second * 1,
		consumerCount: 1,
		redisKeyPrefix: "{redis-ordered-queue}",
		handleMessage: func (ctx context.Context, data *interface{}, meta *MessageMetadata) (error) {
			fmt.Printf("handleMessage: data %v, meta %v\n", data, meta)

			return nil
		},
		handleInvalidMessage: func (ctx context.Context, data *string) (error) {
			fmt.Printf("handleInvalidMessage: data %v\n", *data)

			return nil
		},
	}

	var client, err = NewClient(context.TODO(), opt);

	if (err != nil) { t.Errorf("NewClient: %v", err); return }

	client.StartConsumers(context.TODO())
	client.StopConsumers(context.TODO())

	for i := 0; i < 1000; i++ {
		fmt.Printf(".")
		client.Send(context.TODO(), "test", 1, fmt.Sprintf("GO-GROUP-%v", i));
	}
	time.Sleep(time.Second);
}
