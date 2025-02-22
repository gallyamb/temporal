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

package queuestest

import (
	"context"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/queues"
)

// FakeDLQ is a DLQ which records the requests it receives and returns the given error upon DLQ.EnqueueTask.
type FakeDLQ struct {
	// Requests to write to the DLQ
	Requests       []*persistence.EnqueueTaskRequest
	EnqueueTaskErr error
	CreateQueueErr error
}

var _ queues.DLQ = (*FakeDLQ)(nil)

func (d *FakeDLQ) EnqueueTask(
	_ context.Context,
	request *persistence.EnqueueTaskRequest,
) (*persistence.EnqueueTaskResponse, error) {
	d.Requests = append(d.Requests, request)
	return nil, d.EnqueueTaskErr
}

func (d *FakeDLQ) CreateQueue(
	context.Context,
	*persistence.CreateQueueRequest,
) (*persistence.CreateQueueResponse, error) {
	return nil, d.CreateQueueErr
}
