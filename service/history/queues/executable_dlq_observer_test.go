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

package queues_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// testLogger records info and error logs.
	testLogger struct {
		log.Logger
		infoLogs  []logRecord
		errorLogs []logRecord
	}
	// testMetricsHandler records calls to [metrics.Handler.Counter] and [metrics.Handler.Timer].
	testMetricsHandler struct {
		metrics.Handler
		counts map[string]int
		times  map[string][]time.Duration
	}
	// counterRecorder is a fake [metrics.CounterIface].
	counterRecorder struct {
		name   string
		counts map[string]int
	}
	// timerRecorder is a fake [metrics.TimerIface].
	timerRecorder struct {
		name  string
		times map[string][]time.Duration
	}
	// logRecord represents a call to a method of [log.Logger]
	logRecord struct {
		msg  string
		tags []tag.Tag
	}
)

func (t *testLogger) Info(msg string, tags ...tag.Tag) {
	t.infoLogs = append(t.infoLogs, logRecord{
		msg:  msg,
		tags: tags,
	})
}

func (t *testLogger) Error(msg string, tags ...tag.Tag) {
	t.errorLogs = append(t.errorLogs, logRecord{
		msg:  msg,
		tags: tags,
	})
}

func (t *testMetricsHandler) Timer(s string) metrics.TimerIface {
	return timerRecorder{
		name:  s,
		times: t.times,
	}
}

func (t *testMetricsHandler) Counter(s string) metrics.CounterIface {
	return counterRecorder{
		name:   s,
		counts: t.counts,
	}
}

func (c counterRecorder) Record(int64, ...metrics.Tag) {
	c.counts[c.name] += 1
}

func (t timerRecorder) Record(duration time.Duration, _ ...metrics.Tag) {
	t.times[t.name] = append(t.times[t.name], duration)
}

var errTerminal = new(serialization.DeserializationError)

func TestNewExecutableDLQObserver(t *testing.T) {
	// Test that the metrics and logging behave correctly for the ExecutableDLQObserver.

	t.Parallel()

	task := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			"test-namespace-id",
			"test-workflow-id",
			"test-run-id",
		),
		TaskID: 42,
	}
	executable := queuestest.NewFakeExecutable(task, errTerminal)
	dlq := &queuestest.FakeDLQ{}
	clusterMetadata := queuestest.NewClusterMetadata("test-cluster-name")
	executableDLQ := queues.NewExecutableDLQ(executable, dlq, clusterMetadata)
	logger := &testLogger{}
	metricsHandler := &testMetricsHandler{
		counts: map[string]int{},
		times:  map[string][]time.Duration{},
	}
	timeSource := clock.NewEventTimeSource()
	numHistoryShards := 5
	dlqObserver := queues.NewExecutableDLQObserver(executableDLQ, logger, metricsHandler, timeSource, numHistoryShards)
	dlq.Err = errors.New("some error writing to DLQ")

	// 1. First call to execute should indicate that the task failed, logging the task and cause of the failure.
	err := dlqObserver.Execute()
	assert.ErrorIs(t, err, queues.ErrTerminalTaskFailure)
	assert.Equal(t, metricsHandler.counts, map[string]int{
		metrics.TaskTerminalFailures.GetMetricName(): 1,
	}, "Should record terminal failure in metrics")
	assert.Equal(t, metricsHandler.times, map[string][]time.Duration{})
	shardID := tasks.GetShardIDForTask(task, numHistoryShards)
	expectedTags := []tag.Tag{
		tag.WorkflowNamespaceID("test-namespace-id"),
		tag.WorkflowID("test-workflow-id"),
		tag.WorkflowRunID("test-run-id"),
		tag.ShardID(int32(shardID)),
		tag.TaskID(42),
	}
	errorLogs := []logRecord{
		{
			msg:  "A terminal error occurred while processing this task",
			tags: append(expectedTags, tag.Error(err)),
		},
	}
	assert.Equal(t, logger.errorLogs, errorLogs, "Should log error for terminal failure")
	assert.Empty(t, logger.infoLogs)

	// 2. Second call to Execute should attempt to send the task to the DLQ. In this case, it will fail, and we should
	// log the error and record a metric.
	err = dlqObserver.Execute()
	assert.ErrorIs(t, err, queues.ErrSendTaskToDLQ)
	assert.Equal(t, metricsHandler.counts, map[string]int{
		metrics.TaskTerminalFailures.GetMetricName(): 1,
		metrics.TaskDLQFailures.GetMetricName():      1,
	}, "Should record metric for failure to send task to DLQ")
	assert.Equal(t, metricsHandler.times, map[string][]time.Duration{})
	errorLogs = append(errorLogs, logRecord{
		msg:  "Failed to send history task to the DLQ",
		tags: append(expectedTags, tag.Error(err)),
	})
	assert.Equal(
		t,
		logger.errorLogs,
		errorLogs,
		"Should log an error that we failed to send the task to the DLQ",
	)
	assert.Empty(t, logger.infoLogs)

	// 3. The third call to Execute should successfully send the task to the DLQ, recording the time it took from when
	// the task originally failed to when it was finally sent to the DLQ.
	dlq.Err = nil
	timeSource.Advance(time.Second)
	err = dlqObserver.Execute()
	assert.NoError(t, err)
	assert.Equal(t, metricsHandler.counts, map[string]int{
		metrics.TaskTerminalFailures.GetMetricName(): 1,
		metrics.TaskDLQFailures.GetMetricName():      1,
	})
	assert.Equal(t, metricsHandler.times, map[string][]time.Duration{
		metrics.TaskDLQSendLatency.GetMetricName(): {time.Second},
	}, "Should record how long it took to send the task to the DLQ")
	assert.Equal(t, logger.errorLogs, errorLogs)
	assert.Equal(t, logger.infoLogs, []logRecord{
		{
			msg:  "Task sent to DLQ",
			tags: expectedTags,
		},
	}, "Should log when the task is finally successfully sent to the DLQ")
}
