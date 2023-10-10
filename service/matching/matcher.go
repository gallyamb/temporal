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

package matching

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

// TaskMatcher matches a task producer with a task consumer
// Producers are usually rpc calls from history or taskReader
// that drains backlog from db. Consumers are the task queue pollers
type TaskMatcher struct {
	config *taskQueueConfig

	// synchronous task channel to match producer/consumer
	taskC chan *internalTask
	// synchronous task channel to match query task - the reason to have a
	// separate channel for this is that there are cases where consumers
	// are interested in queryTasks but not others. One example is when a
	// namespace is not active in a cluster.
	queryTaskC chan *internalTask

	// dynamicRate is the dynamic rate & burst for rate limiter
	dynamicRateBurst quotas.MutableRateBurst
	// dynamicRateLimiter is the dynamic rate limiter that can be used to force refresh on new rates.
	dynamicRateLimiter *quotas.DynamicRateLimiterImpl
	// forceRefreshRateOnce is used to force refresh rate limit for first time
	forceRefreshRateOnce sync.Once
	// rateLimiter that limits the rate at which tasks can be dispatched to consumers
	rateLimiter quotas.RateLimiter

	fwdr           *Forwarder
	metricsHandler metrics.Handler // namespace metric scope
	numPartitions  func() int      // number of task queue partitions
	//spooledTaskCreateTime atomic.Int64
	backlogTasksCreateTime map[int64]int
	backlogTasksLock       sync.RWMutex
	taskqueue              *taskQueueID
	pollers                atomic.Int32
}

const (
	defaultTaskDispatchRPS    = 100000.0
	defaultTaskDispatchRPSTTL = time.Minute
)

var (
	// Sentinel error to redirect while blocked in matcher.
	errInterrupted = errors.New("interrupted offer")
)

// newTaskMatcher returns a task matcher instance. The returned instance can be used by task producers and consumers to
// find a match. Both sync matches and non-sync matches should use this implementation
func newTaskMatcher(config *taskQueueConfig, fwdr *Forwarder, metricsHandler metrics.Handler, queue *taskQueueID) *TaskMatcher {
	dynamicRateBurst := quotas.NewMutableRateBurst(
		defaultTaskDispatchRPS,
		int(defaultTaskDispatchRPS),
	)
	dynamicRateLimiter := quotas.NewDynamicRateLimiter(
		dynamicRateBurst,
		defaultTaskDispatchRPSTTL,
	)
	limiter := quotas.NewMultiRateLimiter([]quotas.RateLimiter{
		dynamicRateLimiter,
		quotas.NewDefaultOutgoingRateLimiter(
			config.AdminNamespaceTaskQueueToPartitionDispatchRate,
		),
		quotas.NewDefaultOutgoingRateLimiter(
			config.AdminNamespaceToPartitionDispatchRate,
		),
	})
	return &TaskMatcher{
		config:                 config,
		dynamicRateBurst:       dynamicRateBurst,
		dynamicRateLimiter:     dynamicRateLimiter,
		rateLimiter:            limiter,
		metricsHandler:         metricsHandler,
		fwdr:                   fwdr,
		taskC:                  make(chan *internalTask),
		queryTaskC:             make(chan *internalTask),
		numPartitions:          config.NumReadPartitions,
		backlogTasksCreateTime: make(map[int64]int),
		taskqueue:              queue,
	}
}

// Offer offers a task to a potential consumer (poller)
// If the task is successfully matched with a consumer, this
// method will return true and no error. If the task is matched
// but consumer returned error, then this method will return
// true and error message. This method should not be used for query
// task. This method should ONLY be used for sync match.
//
// When a local poller is not available and forwarding to a parent
// task queue partition is possible, this method will attempt forwarding
// to the parent partition.
//
// Cases when this method will block:
//
// Ratelimit:
// When a ratelimit token is not available, this method might block
// waiting for a token until the provided context timeout. Rate limits are
// not enforced for forwarded tasks from child partition.
//
// Forwarded tasks that originated from db backlog:
// When this method is called with a task that is forwarded from a
// remote partition and if (1) this task queue is root (2) task
// was from db backlog - this method will block until context timeout
// trying to match with a poller. The caller is expected to set the
// correct context timeout.
//
// returns error when:
//   - ratelimit is exceeded (does not apply to query task)
//   - context deadline is exceeded
//   - task is matched and consumer returns error in response channel
func (tm *TaskMatcher) Offer(ctx context.Context, task *internalTask) (bool, error) {
	if !task.isForwarded() {
		if err := tm.rateLimiter.Wait(ctx); err != nil {
			tm.metricsHandler.Counter(metrics.SyncThrottlePerTaskQueueCounter.GetMetricName()).Record(1)
			return false, err
		}
	}

	select {
	case tm.taskC <- task: // poller picked up the task
		if task.responseC != nil {
			// if there is a response channel, block until resp is received
			// and return error if the response contains error
			err := <-task.responseC

			tm.log("sync match ready poller")
			if err == nil && !task.isForwarded() {
				mh := tm.metricsHandler.WithTags(metrics.StringTag("source", enumsspb.TASK_SOURCE_HISTORY.String()), metrics.StringTag("forwarded", "false"))
				mh.Timer(metrics.TaskDispatchLatencyPerTaskQueue.GetMetricName()).Record(time.Since(*task.event.Data.CreateTime))
			}
			return true, err
		}
		return false, nil
	default:
		// no poller waiting for tasks, try forwarding this task to the
		// root partition if possible
		select {
		case token := <-tm.fwdrAddReqTokenC():
			tm.log("sync match forward")
			if err := tm.fwdr.ForwardTask(ctx, task); err == nil {
				tm.log("sync match matched remotely")
				// task was remotely sync matched on the parent partition
				token.release()
				mh := tm.metricsHandler.WithTags(metrics.StringTag("source", enumsspb.TASK_SOURCE_HISTORY.String()), metrics.StringTag("forwarded", "true"))
				mh.Timer(metrics.TaskDispatchLatencyPerTaskQueue.GetMetricName()).Record(time.Since(*task.event.Data.CreateTime))
				return true, nil
			}
			token.release()
		default:
			if !tm.isForwardingAllowed() && // we are the root partition and forwarding is not possible
				task.source == enumsspb.TASK_SOURCE_DB_BACKLOG && // task was from backlog (stored in db)
				task.isForwarded() { // task came from a child partition
				// a forwarded backlog task from a child partition, block trying
				// to match with a poller until ctx timeout
				return tm.offerOrTimeout(ctx, task)
			}
		}

		return false, nil
	}
}

func (tm *TaskMatcher) offerOrTimeout(ctx context.Context, task *internalTask) (bool, error) {
	select {
	case tm.taskC <- task: // poller picked up the task
		if task.responseC != nil {
			select {
			case err := <-task.responseC:
				tm.log("remote sync match success")
				return true, err
			case <-ctx.Done():
				return false, nil
			}
		}
		return false, nil
	case <-ctx.Done():
		tm.log("remote sync match timeout")
		return false, nil
	}
}

// OfferQuery will either match task to local poller or will forward query task.
// Local match is always attempted before forwarding is attempted. If local match occurs
// response and error are both nil, if forwarding occurs then response or error is returned.
func (tm *TaskMatcher) OfferQuery(ctx context.Context, task *internalTask) (*matchingservice.QueryWorkflowResponse, error) {
	select {
	case tm.queryTaskC <- task:
		<-task.responseC
		return nil, nil
	default:
	}

	fwdrTokenC := tm.fwdrAddReqTokenC()

	for {
		select {
		case tm.queryTaskC <- task:
			<-task.responseC
			return nil, nil
		case token := <-fwdrTokenC:
			resp, err := tm.fwdr.ForwardQueryTask(ctx, task)
			token.release()
			if err == nil {
				return resp, nil
			}
			if err == errForwarderSlowDown {
				// if we are rate limited, try only local match for the
				// remainder of the context timeout left
				fwdrTokenC = nil
				continue
			}
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// MustOffer blocks until a consumer is found to handle this task
// Returns error only when context is canceled or the ratelimit is set to zero (allow nothing)
// The passed in context MUST NOT have a deadline associated with it
func (tm *TaskMatcher) MustOffer(ctx context.Context, task *internalTask, interruptCh chan struct{}) error {
	if err := tm.rateLimiter.Wait(ctx); err != nil {
		return err
	}
	tm.registerBacklogTask(task)
	defer tm.unregisterBacklogTask(task)
	tm.log("spooled task arrived")

	// attempt a match with local poller first. When that
	// doesn't succeed, try both local match and remote match
	select {
	case tm.taskC <- task:
		tm.log("must offer ready")
		mh := tm.metricsHandler.WithTags(metrics.StringTag("source", task.source.String()), metrics.StringTag("forwarded", "false"))
		mh.Timer(metrics.TaskDispatchLatencyPerTaskQueue.GetMetricName()).Record(time.Since(*task.event.Data.CreateTime))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

//innerCtx, innerCancel := context.WithTimeout(ctx, time.Second*1)
//defer func() {innerCancel()}()

forLoop:
	for {
		select {
		case tm.taskC <- task:
			tm.log("must offer middle")
			mh := tm.metricsHandler.WithTags(metrics.StringTag("source", task.source.String()), metrics.StringTag("forwarded", "false"))
			mh.Timer(metrics.TaskDispatchLatencyPerTaskQueue.GetMetricName()).Record(time.Since(*task.event.Data.CreateTime))
			return nil
		case token := <-tm.fwdrAddReqTokenC():
			tm.log("must offer forwarding")
			childCtx, cancel := context.WithTimeout(ctx, time.Second*2)
			err := tm.fwdr.ForwardTask(childCtx, task)
			token.release()
			if err != nil {
				tm.log("must offer forward error")
				tm.metricsHandler.Counter(metrics.ForwardTaskErrorsPerTaskQueue.GetMetricName()).Record(1)
				// forwarder returns error only when the call is rate limited. To
				// avoid a busy loop on such rate limiting events, we only attempt to make
				// the next forwarded call after this childCtx expires. Till then, we block
				// hoping for a local poller match
				select {
				case tm.taskC <- task:
					tm.log("must offer local match within forward")
					cancel()
					mh := tm.metricsHandler.WithTags(metrics.StringTag("source", task.source.String()), metrics.StringTag("forwarded", "false"))
					mh.Timer(metrics.TaskDispatchLatencyPerTaskQueue.GetMetricName()).Record(time.Since(*task.event.Data.CreateTime))
					return nil
				case <-childCtx.Done():
				case <-ctx.Done():
					cancel()
					return ctx.Err()
				case <-interruptCh:
					cancel()
					return errInterrupted
				}
				cancel()
				continue forLoop
			}
			tm.log("must offer matched remotely")
			cancel()
			// at this point, we forwarded the task to a parent partition which
			// in turn dispatched the task to a poller. Make sure we delete the
			// task from the database
			task.finish(nil)
			mh := tm.metricsHandler.WithTags(metrics.StringTag("source", task.source.String()), metrics.StringTag("forwarded", "true"))
			mh.Timer(metrics.TaskDispatchLatencyPerTaskQueue.GetMetricName()).Record(time.Since(*task.event.Data.CreateTime))
			return nil
		case <-ctx.Done():
			return ctx.Err()
		//case <- innerCtx.Done():
		//	innerCtx, innerCancel = context.WithTimeout(ctx, time.Second*1)
		//	continue forLoop
		case <-interruptCh:
			return errInterrupted
		}
	}
}

// Poll blocks until a task is found or context deadline is exceeded
// On success, the returned task could be a query task or a regular task
// Returns errNoTasks when context deadline is exceeded
func (tm *TaskMatcher) Poll(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, bool, error) {
	return tm.poll(ctx, pollMetadata, false)
}

// PollForQuery blocks until a *query* task is found or context deadline is exceeded
// Returns errNoTasks when context deadline is exceeded
func (tm *TaskMatcher) PollForQuery(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, bool, error) {
	return tm.poll(ctx, pollMetadata, true)
}

// UpdateRatelimit updates the task dispatch rate
func (tm *TaskMatcher) UpdateRatelimit(rpsPtr *float64) {
	if rpsPtr == nil {
		return
	}

	rps := *rpsPtr
	nPartitions := float64(tm.numPartitions())
	if nPartitions > 0 {
		// divide the rate equally across all partitions
		rps = rps / nPartitions
	}
	burst := int(math.Ceil(rps))

	minTaskThrottlingBurstSize := tm.config.MinTaskThrottlingBurstSize()
	if burst < minTaskThrottlingBurstSize {
		burst = minTaskThrottlingBurstSize
	}

	tm.dynamicRateBurst.SetRPS(rps)
	tm.dynamicRateBurst.SetBurst(burst)
	tm.forceRefreshRateOnce.Do(func() {
		// Dynamic rate limiter only refresh its rate every 1m. Before that initial 1m interval, it uses default rate
		// which is 10K and is too large in most cases. We need to force refresh for the first time this rate is set
		// by poller. Only need to do that once. If the rate change later, it will be refresh in 1m.
		tm.dynamicRateLimiter.Refresh()
	})
}

// Rate returns the current rate at which tasks are dispatched
func (tm *TaskMatcher) Rate() float64 {
	return tm.rateLimiter.Rate()
}

func (tm *TaskMatcher) poll(
	ctx context.Context, pollMetadata *pollMetadata, queryOnly bool,
) (t *internalTask, f bool, e error) {
	taskC, queryTaskC := tm.taskC, tm.queryTaskC
	if queryOnly {
		taskC = nil
	}

	tm.pollers.Add(1)
	defer tm.pollers.Add(-1)

	tm.log("poller came")

	start := time.Now()
	defer tm.metricsHandler.Timer(metrics.PollLatencyPerTaskQueue.GetMetricName()).Record(
		time.Since(start), metrics.StringTag("duplicate", strconv.FormatBool(f)))
	// We want to effectively do a prioritized select, but Go select is random
	// if multiple cases are ready, so split into multiple selects.
	// The priority order is:
	// 1. ctx.Done
	// 2. taskC and queryTaskC
	// 3. forwarding
	// 4. block looking locally for remainder of context lifetime
	// To correctly handle priorities and allow any case to succeed, all select
	// statements except for the last one must be non-blocking, and the last one
	// must include all the previous cases.

	// 1. ctx.Done
	select {
	case <-ctx.Done():
		tm.metricsHandler.Counter(metrics.PollTimeoutPerTaskQueueCounter.GetMetricName()).Record(1)
		return nil, false, errNoTasks
	default:
	}

	// 2. taskC and queryTaskC
	select {
	case task := <-taskC:
		tm.log("polled local ready task")
		if task.responseC != nil {
			tm.metricsHandler.Counter(metrics.PollSuccessWithSyncPerTaskQueueCounter.GetMetricName()).Record(1)
		}
		tm.metricsHandler.Counter(metrics.PollSuccessPerTaskQueueCounter.GetMetricName()).Record(1)
		return task, false, nil
	case task := <-queryTaskC:
		tm.metricsHandler.Counter(metrics.PollSuccessWithSyncPerTaskQueueCounter.GetMetricName()).Record(1)
		tm.metricsHandler.Counter(metrics.PollSuccessPerTaskQueueCounter.GetMetricName()).Record(1)
		return task, false, nil
	default:
	}

	// 3. forwarding (and all other clauses repeated)
	select {
	case <-ctx.Done():
		tm.metricsHandler.Counter(metrics.PollTimeoutPerTaskQueueCounter.GetMetricName()).Record(1)
		return nil, false, errNoTasks
	case task := <-taskC:
		tm.log("polled local middle")
		if task.responseC != nil {
			tm.metricsHandler.Counter(metrics.PollSuccessWithSyncPerTaskQueueCounter.GetMetricName()).Record(1)
		}
		tm.metricsHandler.Counter(metrics.PollSuccessPerTaskQueueCounter.GetMetricName()).Record(1)
		return task, false, nil
	case task := <-queryTaskC:
		tm.metricsHandler.Counter(metrics.PollSuccessWithSyncPerTaskQueueCounter.GetMetricName()).Record(1)
		tm.metricsHandler.Counter(metrics.PollSuccessPerTaskQueueCounter.GetMetricName()).Record(1)
		return task, false, nil
	case token := <-tm.fwdrPollReqTokenC():
		tm.log("Forwarding Poll with backlog Age: %s len of tackC: %d", tm.getBacklogAge(), len(taskC))
		if task, err := tm.fwdr.ForwardPoll(ctx, pollMetadata); err == nil {
			token.release()
			tm.log("polled remote")
			return task, true, nil
		}
		token.release()
	}

	// 4. blocking local poll
	select {
	case <-ctx.Done():
		tm.metricsHandler.Counter(metrics.PollTimeoutPerTaskQueueCounter.GetMetricName()).Record(1)
		return nil, false, errNoTasks
	case task := <-taskC:
		tm.log("polled local blocked")
		if task.responseC != nil {
			tm.metricsHandler.Counter(metrics.PollSuccessWithSyncPerTaskQueueCounter.GetMetricName()).Record(1)
		}
		tm.metricsHandler.Counter(metrics.PollSuccessPerTaskQueueCounter.GetMetricName()).Record(1)
		return task, false, nil
	case task := <-queryTaskC:
		tm.metricsHandler.Counter(metrics.PollSuccessWithSyncPerTaskQueueCounter.GetMetricName()).Record(1)
		tm.metricsHandler.Counter(metrics.PollSuccessPerTaskQueueCounter.GetMetricName()).Record(1)
		return task, false, nil
	}
}

func (tm *TaskMatcher) fwdrPollReqTokenC() <-chan *ForwarderReqToken {
	if tm.fwdr == nil {
		return nil
	}

	ba := tm.getBacklogAge()
	if tm.config.NewForward() && ba != NO_BACKLOG {
		return nil
	}

	tm.log("Allowing Poll forwarding with backlog Age: %s len of tackC: %d", ba, len(tm.taskC))
	return tm.fwdr.PollReqTokenC()
}

func (tm *TaskMatcher) fwdrAddReqTokenC() <-chan *ForwarderReqToken {
	if tm.fwdr == nil {
		return nil
	}

	ba := tm.getBacklogAge()
	if tm.config.NewForward() && ba != NO_BACKLOG {
		tm.log("Rejected Task forwarding with backlog Age: %s len of tackC: %d", ba, len(tm.taskC))
		return nil
	}

	tm.log("Allowing Task forwarding with backlog Age: %s len of tackC: %d", ba, len(tm.taskC))
	return tm.fwdr.AddReqTokenC()
}

func (tm *TaskMatcher) isForwardingAllowed() bool {
	return tm.fwdr != nil
}

func (tm *TaskMatcher) registerBacklogTask(task *internalTask) {
	tm.backlogTasksLock.Lock()
	defer tm.backlogTasksLock.Unlock()

	tm.log("registering")

	ts := task.event.Data.CreateTime.UnixNano()
	tm.backlogTasksCreateTime[ts] += 1
}

func (tm *TaskMatcher) unregisterBacklogTask(task *internalTask) {
	tm.backlogTasksLock.Lock()
	defer tm.backlogTasksLock.Unlock()

	tm.log("unregistering")
	ts := task.event.Data.CreateTime.UnixNano()
	counter := tm.backlogTasksCreateTime[ts]
	if counter == 1 {
		delete(tm.backlogTasksCreateTime, ts)
	} else {
		tm.backlogTasksCreateTime[ts] = counter - 1
	}
}

const NO_BACKLOG time.Duration = -1

func (tm *TaskMatcher) getBacklogAge() time.Duration {
	tm.backlogTasksLock.RLock()
	defer tm.backlogTasksLock.RUnlock()

	if len(tm.backlogTasksCreateTime) == 0 {
		return NO_BACKLOG
	}
	//fmt.Printf("backlog len: %d\n", len(tm.backlogTasksCreateTime))
	oldest := int64(math.MaxInt64)
	for createTime := range tm.backlogTasksCreateTime {
		if createTime < oldest {
			oldest = createTime
		}
	}

	return time.Since(time.Unix(0, oldest))
}

func (tm *TaskMatcher) log(msg string, args ...any) {
	if tm.taskqueue.taskType != enums.TASK_QUEUE_TYPE_ACTIVITY  || strings.HasPrefix(tm.taskqueue.Name.BaseNameString(), "temporal") {
		return
	}
	fmt.Printf("%s %s (%d) \t", time.Now().Format("15:04:05.999"), tm.name(), tm.pollers.Load())
	fmt.Printf(msg+" ["+tm.getBacklogAge().String()+"] \n", args...)
}

func (tm *TaskMatcher) name() any {
	if tm.fwdr == nil {
		return "[ROOT]"
	}
	return fmt.Sprintf("[P%d]",tm.taskqueue.Partition())
}
