// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package queues

import (
	"errors"
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

// ExecutableDLQObserver records telemetry like metrics and logs for an ExecutableDLQ.
type ExecutableDLQObserver struct {
	*ExecutableDLQ
	logger           log.Logger
	metricsHandler   metrics.Handler
	numHistoryShards int
	timeSource       clock.TimeSource

	terminalFailureTime *time.Time
}

func NewExecutableDLQObserver(
	executableDLQ *ExecutableDLQ,
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
	numHistoryShards int,
) *ExecutableDLQObserver {
	return &ExecutableDLQObserver{
		ExecutableDLQ:       executableDLQ,
		logger:              logger,
		metricsHandler:      metricsHandler,
		numHistoryShards:    numHistoryShards,
		timeSource:          timeSource,
		terminalFailureTime: nil,
	}
}

func (o *ExecutableDLQObserver) Execute() error {
	err := o.ExecutableDLQ.Execute()
	if errors.Is(err, ErrTerminalTaskFailure) {
		o.metricsHandler.Counter(metrics.TaskTerminalFailures.GetMetricName()).Record(1)
		logger := o.getLogger()
		logger.Error("A terminal error occurred while processing this task", tag.Error(err))
		now := o.timeSource.Now()
		o.terminalFailureTime = &now
	} else if errors.Is(err, ErrSendTaskToDLQ) {
		o.metricsHandler.Counter(metrics.TaskDLQFailures.GetMetricName()).Record(1)
		logger := o.getLogger()
		logger.Error("Failed to send history task to the DLQ", tag.Error(err))
	} else if err == nil && o.terminalFailureTime != nil {
		latency := o.timeSource.Now().Sub(*o.terminalFailureTime)
		o.metricsHandler.Timer(metrics.TaskDLQSendLatency.GetMetricName()).Record(latency)
		logger := o.getLogger()
		logger.Info("Task sent to DLQ")
	}
	return err
}

func (o *ExecutableDLQObserver) getLogger() log.Logger {
	task := o.ExecutableDLQ.GetTask()
	namespaceID := task.GetNamespaceID()
	workflowID := task.GetWorkflowID()
	runID := task.GetRunID()
	shardID := tasks.GetShardIDForTask(task, o.numHistoryShards)
	taskID := task.GetTaskID()
	logger := log.With(
		o.logger,
		tag.WorkflowNamespaceID(namespaceID),
		tag.WorkflowID(workflowID),
		tag.WorkflowRunID(runID),
		tag.ShardID(int32(shardID)),
		tag.TaskID(taskID),
	)
	return logger
}
