package redisOrderedQueue

import (
	"github.com/go-redis/redis/v8"
	"context"
)

type redisScriptCall func (ctx context.Context, client *redis.Client, args []interface{}, keys []string) (*redis.Cmd);

func newScriptCall (ctx context.Context, client *redis.Client, script string) (redisScriptCall, error) {
	var scriptHash, err = client.Do(ctx, "SCRIPT", "LOAD", script).Text()

	if (err != nil) {
		return nil, err
	}

	var invoke = func (ctx context.Context, client *redis.Client, args []interface{}, keys []string) (*redis.Cmd) {
		var packet []interface{}
		
		packet = append(packet, "EVALSHA")
		packet = append(packet, scriptHash)
		packet = append(packet, len(keys))
		
		for _, key := range(keys) {
			packet = append(packet, key)
		}
		
		for _, arg := range(args) {
			packet = append(packet, arg)
		}

		return client.Do(ctx, packet...);
	}

	return invoke, nil
}
