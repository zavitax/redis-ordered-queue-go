package redisOrderedQueue

var scriptDeleteMessage = `
  local groupSetKey = KEYS[1];
  local messageQueueKey = KEYS[2];

  local groupId = ARGV[1];
  local messageData = ARGV[2];

  local removedMsgsCount = redis.call('ZREM', messageQueueKey, messageData);

  return { removedMsgsCount };
`;

var scriptAddGroupAndMessageToQueue = `
  -- Processing group
  local groupStreamKey = KEYS[1];
  local groupSetKey = KEYS[2];

  -- Message data
  local priorityMessageQueueKey = KEYS[3];

  -- Processing group
  local groupId = ARGV[1];

  -- Message data
  local messagePriority = ARGV[2];
  local messageData = ARGV[3];

  -- Push message data into priority queue
  local addedMsgCount = redis.call('ZADD', priorityMessageQueueKey, messagePriority, messageData);
  local newScoreOfGroup = 0;

  if (addedMsgCount > 0) then
    -- Add to unlocked group set
    newScoreOfGroup = tonumber(redis.call('ZINCRBY', groupSetKey, addedMsgCount, groupId));

    if (newScoreOfGroup == 1) then
      -- Add to stream of groups for processing
      redis.call('XADD', groupStreamKey, '*', 'group', groupId);
    end
  end

  return { addedMsgCount, newScoreOfGroup, messageData };
`;

var scriptClaimTimedOutGroup = `
  -- Processing group
  local groupStreamKey = KEYS[1];

  -- Consumer group & consumer
  local consumerGroupId = ARGV[1];
  local consumerId = ARGV[2];

  -- Timeout milliseconds
  local timeoutMs = tonumber(ARGV[3]);

  local pending_msgs = redis.call('XPENDING', groupStreamKey, consumerGroupId, '-', '+', '1');

  if (#(pending_msgs) > 0) then
    -- Claim message by Id for this consumer
    local pending_msg_id = pending_msgs[1][1];
    local pending_msg_consumer_id = pending_msgs[1][2];
    local pending_msg_idle_ms = tonumber(pending_msgs[1][3]);

    if (pending_msg_idle_ms >= timeoutMs) then
      local claimed_msgs = redis.call('XCLAIM', groupStreamKey, consumerGroupId, consumerId, 0, pending_msg_id);

      if (#(claimed_msgs) > 0) then
        local msg_id = claimed_msgs[1][1];
        local msg_content = claimed_msgs[1][2];

        -- Check if we are snatching messages from a different consumer ID
        if (consumerId ~= pending_msg_consumer_id) then
          -- Check remaining pending msgs for this consumer, if no more msgs are pending, we can delete the consumer
          local pending_msgs_same_consumer = redis.call('XPENDING', groupStreamKey, consumerGroupId, '-', '+', '1', pending_msg_consumer_id);

          if (#(pending_msgs_same_consumer) == 0) then
            -- Delete consumer which has timed out
            redis.call('XGROUP', 'DELCONSUMER', groupStreamKey, consumerGroupId, pending_msg_consumer_id)
          end
        end

        local group_id = msg_content[2];

        return { msg_id, group_id };
      end
    end
  end

  return {};
`;

var scriptUnlockGroup = `
  -- Processing group
  local groupStreamKey = KEYS[1];
  local groupSetKey = KEYS[2];

  -- Message data
  local priorityMessageQueueKey = KEYS[3];

  -- Consumer group & consumer
  local consumerGroupId = ARGV[1];
  local consumerId = ARGV[2];
  local messageId = ARGV[3];
  local groupId = ARGV[4];

  redis.call('XACK', groupStreamKey, consumerGroupId, messageId);
  redis.call('XDEL', groupStreamKey, messageId);

  -- Check remaining messages in group queue, if there are no more messages remainint we can delete group set key
  local numMsgs = tonumber(redis.call('ZCARD', priorityMessageQueueKey));

  if (numMsgs == 0) then
    -- No msgs, remove group set key
    redis.call('ZREM', groupSetKey, groupId);

    -- Check remaining pending msgs for this consumer, if no more msgs are pending, we can delete the consumer
    local pending_msgs_same_consumer = redis.call('XPENDING', groupStreamKey, consumerGroupId, '-', '+', '1', consumerId);
  
    if (#(pending_msgs_same_consumer) == 0) then
      -- Delete consumer which has timed out
      redis.call('XGROUP', 'DELCONSUMER', groupStreamKey, consumerGroupId, consumerId)
    end
  
    return { 1, numMsgs };
  else
    -- Has msgs, update group set score to reflect current number of messages
    redis.call('ZADD', groupSetKey, 'CH', numMsgs, groupId);

    -- Has msgs, re-add group to the tail of the stream
    redis.call('XADD', groupStreamKey, '*', 'group', groupId);

    return { 0, numMsgs };
  end
`;

var scriptGetMetrics = `
  -- Processing group
  local groupStreamKey = KEYS[1];
  local groupSetKey = KEYS[2];

  -- Message data
  local consumerGroupId = ARGV[1];

  -- Processing group
  local topMessageGroupsLimit = tonumber(ARGV[2]);

  local sum=0
  local z=redis.call('ZRANGE', groupSetKey, 0, -1, 'WITHSCORES')

  for i=2, #z, 2 do 
      sum=sum+z[i]
  end

  local topGroups = redis.call('ZREVRANGE', groupSetKey, 0, topMessageGroupsLimit, 'WITHSCORES')

  local consumers = redis.call('XINFO', 'CONSUMERS', groupStreamKey, consumerGroupId)

  return {
    redis.call('XLEN', groupStreamKey),
    redis.call('ZCARD', groupSetKey),
    #(consumers),
    sum,
    topGroups
  }
`;
