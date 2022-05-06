package redisOrderedQueue

import (
	"context"
	"github.com/go-redis/redis/v8"
	"fmt"
	"time"
)

type Options struct {
	RedisOptions*	redis.Options
	BatchSize int
	GroupVisibilityTimeout time.Duration
	PollingTimeout time.Duration
	ConsumerCount int
	RedisKeyPrefix string
	HandleMessage func (ctx context.Context, data *interface{}, meta *MessageMetadata) (error)
	HandleInvalidMessage func (ctx context.Context, data *string) (error)
}

var validationError = fmt.Errorf("All Options values must be correctly specified")

func (o* Options) Validate() (error) {
	if (o == nil) {
		return validationError
	}

	if (o.RedisOptions == nil) {
		return validationError
	}

	if (o.BatchSize < 1) {
		return validationError
	}

	if (o.GroupVisibilityTimeout < time.Second) {
		return validationError
	}

	if (o.PollingTimeout < time.Second) {
		return validationError
	}

	if (o.ConsumerCount < 1) {
		return validationError
	}

	if (len(o.RedisKeyPrefix) < 1) {
		return validationError
	}

	if (o.HandleMessage == nil) {
		return validationError
	}

	return nil;
}
