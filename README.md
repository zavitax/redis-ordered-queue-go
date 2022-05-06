# redis-ordered-queue-go
Priority queue with message-group based partitioning and equal attention guarantee for each message group based on Redis

## What is it?

Redis-based ordered queue with support for message priority.

## Quality metrics

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-go&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-go)

[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-go&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-go)

[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-go&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-go)

[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-go&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-go)

[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-go&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-go)

[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-go&metric=bugs)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-go)

[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-go&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-go)

[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=zavitax_redis-ordered-queue-go&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=zavitax_redis-ordered-queue-go)

## Queue guarantees

### Low latency delivery

Based on traffic and amount of consumers, it is possible to reach very low delivery latency. I observed 2-15 millisecond latencies with thousands of consumers, message groups and messages.

### Equal attention to all message groups

Message groups are cycled through, each consumer looking at the next available message group in the buffer.

### No conflicts between message groups

Message groups are locked for processing before a consumer can process messages associated with that message group. This ensures that when a consumer is processing messages in a message group, no other consumers can see messages in the same message group.

The drawback is that if you only have one message group in your set-up, only one consumer can be active at each moment.

### At least once delivery

Message redelivery attempts will take place until it's acknowledged by the consumer.

### High performance, low memory footprint

The amount of consumers you can run on each worker node is limited only by the amount of allowed connections on your Redis server.

## Infrastructure

The library leverages `ioredis` for communication with the Redis server.

## Usage

```go
package main

import (
	"github.com/go-redis/redis/v8"
	"context"
	"testing"
	"time"
	"fmt"
	"sync/atomic"
	"github.com/zavitax/redis-ordered-queue-go"
)

func main () {
	var redisOptions = &redis.Options{
		Addr: "127.0.0.1:6379",
		Password: "",
		DB: 0,
	};

	msgCount := int64(0)

	var opt = &redisOrderedQueue.Options{
		RedisOptions: redisOptions,
		BatchSize: 10,
		GroupVisibilityTimeout: time.Second * 60,
		PollingTimeout: time.Second * 10,
		ConsumerCount: 5000,
		RedisKeyPrefix: "{redis-ordered-queue}",
		HandleMessage: func (ctx context.Context, data *interface{}, meta *redisOrderedQueue.MessageMetadata) (error) {
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
		client.Send(context.TODO(), "test message", 1, fmt.Sprintf("GO-GROUP-%v", i));
	}

	client.StartConsumers(context.TODO())
	time.Sleep(time.Second * 60 * 5);
	client.StopConsumers(context.TODO())
}
```
