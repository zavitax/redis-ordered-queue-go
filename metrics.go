package redisOrderedQueue

import (
	"time"
)

type GetMetricsOptions struct {
	TopMessageGroupsLimit	int
}

type MessageGroupMetrics struct {
	Group string
	Backlog int64
}

type Metrics struct {
	BufferedMessageGroups int64
	TrackedMessageGroups int64
	WorkingConsumers int64
	VisibleMessages int64
	InvalidMessages int64
	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration
	TopMessageGroups []*MessageGroupMetrics
	TopMessageGroupsMessageBacklogLength int64
}
