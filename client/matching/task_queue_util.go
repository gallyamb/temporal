// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.

package matching

import (
	"math/rand"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqname"
)

const (
	defaultNumTaskQueuePartitions = 4
)

type (
	// TaskQueuePartitionFinder is used to pick a partition for a TaskQueue to add task to.
	TaskQueuePartitionFinder struct {
		readPartitions    dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		namespaceIDToName func(id namespace.ID) (namespace.Name, error)

		lock       sync.Mutex
		taskQueues map[taskQueueKey]*loadBalancer
	}

	// TaskQueueClusterFinder is used to find which cluster a given TaskQueue is hosted in.
	TaskQueueClusterFinder struct {
		writePartitions  dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		useNSForOffset   dynamicconfig.StringPropertyFn
		cassClusterCount int32
	}

	taskQueueKey struct {
		NamespaceID string
		Name        tqname.Name
		Type        enumspb.TaskQueueType
	}

	loadBalancer struct {
		taskQueue    taskQueueKey
		pollerCounts []int // keep track of poller count of each partition
		lock         sync.Mutex
	}

	pollToken struct {
		fullName    string
		partitionID int
		pollerCount int
		balancer    *loadBalancer
	}
)

func NewTaskQueuePartitionFinder(
	readPartitions dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters,
	namespaceIDToName func(id namespace.ID) (namespace.Name, error),
) *TaskQueuePartitionFinder {
	return &TaskQueuePartitionFinder{
		readPartitions:    readPartitions,
		namespaceIDToName: namespaceIDToName,

		lock:       sync.Mutex{},
		taskQueues: make(map[taskQueueKey]*loadBalancer),
	}
}

func NewTaskQueueClusterFinder(
	useNSForOffset dynamicconfig.StringPropertyFn,
	cassClusterCount int32,
) *TaskQueueClusterFinder {
	return &TaskQueueClusterFinder{
		useNSForOffset:   useNSForOffset,
		cassClusterCount: cassClusterCount,
	}
}

func newLoadBalancer(taskQueueKey taskQueueKey) *loadBalancer {
	return &loadBalancer{
		taskQueue: taskQueueKey,
		lock:      sync.Mutex{},
	}
}

// PickReadPartition picks a partition that has the fewest pollers waiting on it.
// Caller is responsible to call pollToken.Release() after complete the poll.
func (f *TaskQueuePartitionFinder) PickReadPartition(
	namespaceID string,
	taskQueue taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
	forwardedFrom string,
) *pollToken {
	if forwardedFrom != "" || taskQueue.Kind == enumspb.TASK_QUEUE_KIND_STICKY {
		// no partition for sticky task queue and forwarded request
		return &pollToken{fullName: taskQueue.GetName()}
	}

	parsedName, err := tqname.Parse(taskQueue.GetName())
	if err != nil || err == nil && !parsedName.IsRoot() {
		// parse error or partition already picked, use as-is
		return &pollToken{fullName: taskQueue.GetName()}
	}

	key := taskQueueKey{NamespaceID: namespaceID, Name: parsedName, Type: taskQueueType}

	f.lock.Lock()
	loadBalancer, ok := f.taskQueues[key]
	if !ok {
		loadBalancer = newLoadBalancer(key)
		f.taskQueues[key] = loadBalancer
	}
	f.lock.Unlock()

	partitionCount := f.GetReadPartitionCount(namespaceID, parsedName.BaseNameString(), taskQueueType)
	return loadBalancer.PickReadPartition(partitionCount)
}

// PickReadPartition picks a partition for poller to poll task from, and keeps load balanced between partitions.
// Caller is responsible to call pollToken.Release() after complete the poll.
func (b *loadBalancer) PickReadPartition(partitionCount int32) *pollToken {
	b.lock.Lock()
	defer b.lock.Unlock()

	// ensure we reflect dynamic config change if it ever happens
	b.ensurePartitionCountLocked(partitionCount)

	// pick a random partition to start with
	startPartitionID := rand.Intn(int(partitionCount))
	pickedPartitionID := startPartitionID
	minPollerCount := b.pollerCounts[pickedPartitionID]
	for i := 1; i < int(partitionCount) && minPollerCount > 0; i++ {
		currPartitionID := (startPartitionID + i) % int(partitionCount)
		if b.pollerCounts[currPartitionID] < minPollerCount {
			pickedPartitionID = currPartitionID
			minPollerCount = b.pollerCounts[currPartitionID]
		}
	}

	b.pollerCounts[pickedPartitionID]++

	return &pollToken{
		fullName:    b.taskQueue.Name.WithPartition(pickedPartitionID).FullName(),
		partitionID: pickedPartitionID,
		pollerCount: b.pollerCounts[pickedPartitionID],
		balancer:    b,
	}
}

// caller to ensure that lock is obtained before call this function
func (b *loadBalancer) ensurePartitionCountLocked(partitionCount int32) {
	if len(b.pollerCounts) == int(partitionCount) {
		return
	}

	if len(b.pollerCounts) < int(partitionCount) {
		// add more partition entries
		for i := len(b.pollerCounts); i < int(partitionCount); i++ {
			b.pollerCounts = append(b.pollerCounts, 0)
		}
	} else {
		// truncate existing partition entries
		b.pollerCounts = b.pollerCounts[:partitionCount]
	}
}

func (b *loadBalancer) Release(partitionID int) {
	b.lock.Lock()
	defer b.lock.Unlock()
	// partitionID could be out of range if dynamic config reduce taskQueue partition count
	if len(b.pollerCounts) > partitionID && b.pollerCounts[partitionID] > 0 {
		b.pollerCounts[partitionID]--
	}
}

func (t *pollToken) Release() {
	if t.balancer != nil {
		// t.balancer == nil is valid for example sticky task queue.
		t.balancer.Release(t.partitionID)
	}
}

func (t *pollToken) GetFullName() string {
	return t.fullName
}

func (f *TaskQueuePartitionFinder) GetReadPartitionCount(
	namespaceID string,
	taskQueueBaseName string,
	taskQueueType enumspb.TaskQueueType,
) int32 {
	partitionCount, err := getPartitionCount(namespaceID, taskQueueBaseName, taskQueueType, f.readPartitions, f.namespaceIDToName)
	if err != nil || partitionCount <= 0 {
		// This matches with the behavior prior to https://github.com/temporalio/temporal/pull/3199 and is sanctioned by
		// the original author of this file as an interim replacement until proper followup improvements take place. The
		// only condition for this error handling path is if the above helper cannot resolve the namespace ID. It has no
		// implications on whether the dynamic config is missing the corresponding default value. See the aforementioned
		// PR for more details.
		return defaultNumTaskQueuePartitions
	}
	return partitionCount
}

func getPartitionCount(
	namespaceID string,
	taskQueueBaseName string,
	taskQueueType enumspb.TaskQueueType,
	nPartitions dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters,
	namespaceIDToName func(id namespace.ID) (namespace.Name, error),
) (int32, error) {
	namespace, err := namespaceIDToName(namespace.ID(namespaceID))
	if err != nil {
		return 0, err
	}
	partitionCount := nPartitions(string(namespace), taskQueueBaseName, taskQueueType)
	return int32(partitionCount), nil
}
