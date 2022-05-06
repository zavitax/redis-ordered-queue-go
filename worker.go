package redisOrderedQueue

import (
	"github.com/go-redis/redis/v8"
	"context"
	"fmt"
)

type lockHandle struct {
  groupId string;
  messageId string;
  queueId string;
  consumerId string;
};

type redisQueueWorker struct {
	parent *redisQueueClient
	redis *redis.Client

	consumerId string

  callClaimTimedOutGroup redisScriptCall
  callUnlockGroup redisScriptCall
  callDeleteMessage redisScriptCall
}

func newWorker (ctx context.Context, parent *redisQueueClient) (*redisQueueWorker, error) {
	var c = &redisQueueWorker{
		parent: parent,
		redis: redis.NewClient(parent.options.RedisOptions),
	}

	var err error
	var clientId int64

	clientId, err = c.parent.createClientId(ctx)
	if (err != nil) { c.redis.Close(); return nil, err };

	c.consumerId = fmt.Sprintf("consumer-%v", clientId)

	if c.callClaimTimedOutGroup, err = newScriptCall(ctx, c.redis, scriptClaimTimedOutGroup); err != nil { c.redis.Close(); return nil, err }
	if c.callUnlockGroup, err = newScriptCall(ctx, c.redis, scriptUnlockGroup); err != nil { c.redis.Close(); return nil, err }
	if c.callDeleteMessage, err = newScriptCall(ctx, c.redis, scriptDeleteMessage); err != nil { c.redis.Close(); return nil, err }

	return c, nil
}

func (c *redisQueueWorker) run (ctx context.Context) (error) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("worker.run: cancelled!\n")
			// Cancelled
			return nil

		default:
			// Process. We don't care for errors here - they can mean "no messages available"
			c.poll(ctx);
		}
	}
}

func (c *redisQueueWorker) poll (ctx context.Context) (error) {
	var lock, err = c.lock(ctx)
	
	if (err != nil) { return err }
	if (lock == nil) { return nil; }

	err = c.process_messages_batch(ctx, lock)
	if (err != nil) { return err }

	err = c.unlock(ctx, lock)
	if (err != nil) { return err }

	return nil
}

func (c *redisQueueWorker) process_messages_batch (ctx context.Context, lock *lockHandle) (error) {
	var messages, err = c.peekMessagesBatch(ctx, lock)

	if (err != nil) { return err; }

	for _, msgData := range(messages) {
		err = c.processMessage(ctx, lock, msgData);

		if (err == nil) {
			err = c.deleteMessage(ctx, lock, msgData);
		}
	}

	return nil
}

func (c *redisQueueWorker) processMessage (ctx context.Context, lock *lockHandle, msgData string) (error) {
	return c.parent.processMessage(ctx, lock, msgData);
}

func (c *redisQueueWorker) peekMessagesBatch (ctx context.Context, lock *lockHandle) ([]string, error) {
	var data, err = c.redis.Do(ctx, "ZRANGE", lock.queueId, 0, c.parent.options.BatchSize - 1).Slice();

	if (err != nil) { return nil, err }
	
	var result []string

	for _, item := range(data) {
		result = append(result, item.(string))
	}

	return result, nil
}

func (c *redisQueueWorker) deleteMessage (ctx context.Context, lock *lockHandle, msgData string) (error) {
	var _, err = c.callDeleteMessage(ctx, c.redis,
		[]interface{} { lock.groupId, msgData },
		[]string { c.parent.groupSetKey, lock.queueId },
	).Slice()

	if (err != nil) { return err; }
	
	return nil
}

func (c *redisQueueWorker) unlock (ctx context.Context, lock *lockHandle) (error) {
	var _, err = c.callUnlockGroup(ctx, c.redis,
		[]interface{}{ c.parent.consumerGroupId, c.consumerId, lock.messageId, lock.groupId },
		[]string { c.parent.groupStreamKey, c.parent.groupSetKey, lock.queueId },
	).Slice()

	if (err != nil) { return err; }

	return nil
}

func (c *redisQueueWorker) lock (ctx context.Context) (*lockHandle, error) {
	var claimed, err = c.claimTimedOutLock(ctx)

	if (err != nil) { return nil, err }
	if (claimed != nil) { return claimed, nil; }

	var data []interface{}
	data, err = c.redis.Do(ctx, "XREADGROUP",
		"GROUP", c.parent.consumerGroupId, c.consumerId,
		"COUNT", 1,
		"BLOCK", c.parent.options.PollingTimeout.Milliseconds(),
		"STREAMS", c.parent.groupStreamKey, ">").Slice();

	if (err != nil) { return nil, err }

	if (len(data) == 0) { return nil, nil }

	var streams = data[0].([]interface{})
	if (streams == nil || len(streams) < 2) { return nil, nil }

	var messages = streams[1].([]interface{});

	if (messages == nil || len(messages) == 0) { return nil, nil }
	var msg = messages[0].([]interface{});

	if (len(msg) < 2) { return nil, nil }

	var msgId = msg[0].(string);
	var content = msg[1].([]interface{});

	if (content == nil || len(content) < 2) { return nil, nil }

	var groupId = content[1].(string);

	var result = &lockHandle{
		messageId: msgId,
		groupId: groupId,
		queueId: c.parent.createPriorityMessageQueueKey(groupId),
		consumerId: c.consumerId,
	}

	return result, nil
}

func (c *redisQueueWorker) claimTimedOutLock (ctx context.Context) (*lockHandle, error) {
	var data, err = c.callClaimTimedOutGroup(ctx, c.redis, 
		[]interface{} { c.parent.consumerGroupId, c.consumerId, c.parent.options.GroupVisibilityTimeout.Milliseconds() },
		[]string { c.parent.groupStreamKey },
	).Slice()

	if (err != nil) { return nil, err; }
	if (len(data) < 2) { return nil, nil }

	var msgId = data[0].(string);
	var groupId = data[0].(string);

	if (len(msgId) == 0 || len(groupId) == 0) {
		return nil, fmt.Errorf("Invalid response data: %v", data);
	}

	var result = &lockHandle{
		messageId: msgId,
		groupId: groupId,
		queueId: c.parent.createPriorityMessageQueueKey(groupId),
		consumerId: c.consumerId,
	};

	return result, nil
}
