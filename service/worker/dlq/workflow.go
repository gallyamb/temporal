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

// Package dlq contains the workflow for deleting and re-enqueueing DLQ tasks. Both of these operations are performed by
// the same workflow to avoid concurrent deletion and re-enqueueing of the same task.
package dlq

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	workercommon "go.temporal.io/server/service/worker/common"
)

type (
	// WorkflowParams is the single argument to the DLQ workflow.
	WorkflowParams struct {
		// WorkflowType options are available via the WorkflowType* constants.
		WorkflowType string
		// DeleteParams is only used for WorkflowTypeDelete.
		DeleteParams DeleteParams
	}
	// DeleteParams contain the target DLQ and the max message ID to delete up to.
	DeleteParams struct {
		TaskCategory  int
		SourceCluster string
		TargetCluster string
		MaxMessageID  int64
	}
	// HistoryServiceClient is a subset of the [historyservice.HistoryServiceClient] interface.
	HistoryServiceClient interface {
		DeleteDLQTasks(
			ctx context.Context,
			in *historyservice.DeleteDLQTasksRequest,
			opts ...grpc.CallOption,
		) (*historyservice.DeleteDLQTasksResponse, error)
	}
	workerComponent struct {
		client HistoryServiceClient
	}
)

const (
	// WorkflowName is the name of the DLQ workflow.
	WorkflowName = "temporal-sys-dlq-workflow"
	// WorkflowTypeDelete is what the value of WorkflowParams.WorkflowType should be to delete DLQ tasks.
	WorkflowTypeDelete = "delete"

	errorTypeInvalidWorkflowType = "dlq-invalid-workflow-type"
	errorTypeUnavailable         = "history-service-unavailable"
	errorTypeUnknown             = "unknown-error"
	deleteTasksActivityName      = "dlq-delete-tasks-activity"

	deleteTasksTimeout = 15 * time.Second
)

// Module provides a [workercommon.WorkerComponent] annotated with [workercommon.WorkerComponentTag] to the graph, given
// a [HistoryServiceClient] dependency.
var Module = workercommon.AnnotateWorkerComponentProvider(newComponent)

func newComponent(client HistoryServiceClient) workercommon.WorkerComponent {
	return &workerComponent{
		client: client,
	}
}

//revive:disable:import-shadowing this doesn't actually shadow imports because it's a method, not a function
func (c *workerComponent) workflow(ctx workflow.Context, params WorkflowParams) error {
	if params.WorkflowType == WorkflowTypeDelete {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: deleteTasksTimeout,
		})
		future := workflow.ExecuteActivity(ctx, deleteTasksActivityName, params.DeleteParams)
		return future.Get(ctx, nil)
	}
	return temporal.NewNonRetryableApplicationError(params.WorkflowType, errorTypeInvalidWorkflowType, nil)
}

func (c *workerComponent) deleteTasks(ctx context.Context, params DeleteParams) error {
	req := &historyservice.DeleteDLQTasksRequest{
		DlqKey: &commonspb.HistoryDLQKey{
			TaskCategory:  int32(params.TaskCategory),
			SourceCluster: params.SourceCluster,
			TargetCluster: params.TargetCluster,
		},
		InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
			MessageId: params.MaxMessageID,
		},
	}
	_, err := c.client.DeleteDLQTasks(ctx, req)
	if err != nil {
		var svcErr serviceerror.ServiceError
		// Use this instead of serviceerror.ToStatus because that doesn't work for wrapped errors
		if errors.As(err, &svcErr) {
			if svcErr.Status().Code() == codes.Unavailable {
				return temporal.NewApplicationErrorWithCause(
					"delete tasks activity failed with unavailable error",
					errorTypeUnavailable,
					err,
				)
			}
		}
		// Consider all other errors as non-retryable
		return temporal.NewNonRetryableApplicationError("error deleting DLQ tasks", errorTypeUnknown, err)
	}
	return err
}

func (c *workerComponent) Register(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(c.workflow, workflow.RegisterOptions{
		Name: WorkflowName,
	})
	registry.RegisterActivityWithOptions(c.deleteTasks, activity.RegisterOptions{
		Name: deleteTasksActivityName,
	})
}

func (c *workerComponent) DedicatedWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// use default worker
	return nil
}
