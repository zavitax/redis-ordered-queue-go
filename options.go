package redisOrderedQueue

import (
	"context"
	"github.com/go-redis/redis/v8"
	"fmt"
	"time"
)

type Options struct {
	redisOptions*	redis.Options
	batchSize int
	groupVisibilityTimeout time.Duration
	pollingTimeout time.Duration
	consumerCount int
	redisKeyPrefix string
	handleMessage func (ctx context.Context, data *interface{}, meta *MessageMetadata) (error)
	handleInvalidMessage func (ctx context.Context, data *string) (error)
}

var validationError = fmt.Errorf("All Options values must be correctly specified")

func (o* Options) Validate() (error) {
	if (o == nil) {
		return validationError
	}

	if (o.redisOptions == nil) {
		return validationError
	}

	if (o.batchSize < 1) {
		return validationError
	}

	if (o.groupVisibilityTimeout < time.Second) {
		return validationError
	}

	if (o.pollingTimeout < time.Second) {
		return validationError
	}

	if (o.consumerCount < 1) {
		return validationError
	}

	if (len(o.redisKeyPrefix) < 1) {
		return validationError
	}

	if (o.handleMessage == nil) {
		return validationError
	}

	return nil;
}
