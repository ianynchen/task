package task

import (
	"container/list"
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func exec1(ctx context.Context, cancelFunc context.CancelFunc, request interface{}, response interface{}) {
	log.Println("executing 1")
	time.Sleep(1000 * time.Millisecond)
	list := response.(*list.List)
	list.PushBack(1)
}

func exec2(ctx context.Context, cancelFunc context.CancelFunc, request interface{}, response interface{}) {
	log.Println("executing 2")
	time.Sleep(1000 * time.Millisecond)
	list := response.(*list.List)
	list.PushBack(2)
}

func exec3(ctx context.Context, cancelFunc context.CancelFunc, request interface{}, response interface{}) {
	log.Println("executing 3")
	time.Sleep(1000 * time.Millisecond)
	list := response.(*list.List)
	list.PushBack(3)
}

func exec4(ctx context.Context, cancelFunc context.CancelFunc, request interface{}, response interface{}) {
	log.Println("executing 4")
	time.Sleep(1000 * time.Millisecond)
	list := response.(*list.List)
	list.PushBack(4)
}

func exec2WithError(ctx context.Context, cancelFunc context.CancelFunc, request interface{}, response interface{}) {
	log.Println("executing 2")
	time.Sleep(1000 * time.Millisecond)
	status := ctx.Value(StatusKey).(*ExecutionStatus)
	status.Err = errors.New("err")
	cancelFunc()
}

func exec3WithError(ctx context.Context, cancelFunc context.CancelFunc, request interface{}, response interface{}) {
	log.Println("executing 3")
	time.Sleep(1000 * time.Millisecond)
	status := ctx.Value(StatusKey).(*ExecutionStatus)
	status.Err = errors.New("err")
	cancelFunc()
}

func TestSerial(t *testing.T) {

	ctx := context.WithValue(context.Background(), StatusKey, &ExecutionStatus{})
	testStatus := ctx.Value(StatusKey).(*ExecutionStatus)
	log.Println("error: ", testStatus.Err)
	l := list.New()
	step := NewSerializedStep(ctx, []ExecutionFunc{exec1, exec2, exec3, exec4}, nil, l)
	step.Execute()

	assert.Equal(t, nil, ctx.Err())
	var first, second *list.Element
	count := 0
	for e := l.Front(); e != nil; e = e.Next() {
		first = second
		second = e
		count = count + 1

		log.Println("value: ", e.Value)
		if first != nil && second != nil {
			assert.True(t, first.Value.(int) < second.Value.(int))
		}
	}
	assert.Equal(t, 4, count)
	status := step.Status()
	assert.Equal(t, nil, status.Err)
}

func TestSerialWithError(t *testing.T) {

	ctx := context.WithValue(context.Background(), StatusKey, &ExecutionStatus{})
	l := list.New()
	step := NewSerializedStep(ctx, []ExecutionFunc{exec1, exec2WithError, exec3, exec4}, nil, l)
	step.Execute()

	assert.Equal(t, nil, ctx.Err())
	var first, second *list.Element
	count := 0
	for e := l.Front(); e != nil; e = e.Next() {
		first = second
		second = e
		count++

		log.Println("value: ", e.Value)
		if first != nil && second != nil {
			assert.True(t, first.Value.(int) < second.Value.(int))
		}
	}
	assert.Equal(t, 1, count)
	status := step.Status()
	assert.NotEqual(t, nil, status)
}

func TestParallel(t *testing.T) {

	ctx := context.WithValue(context.Background(), StatusKey, &ExecutionStatus{})
	l := list.New()
	step := NewParallelStep(ctx, []ExecutionFunc{exec1, exec2, exec3, exec4}, nil, l)
	step.Execute()

	assert.Equal(t, nil, ctx.Err())
	count := 0
	for e := l.Front(); e != nil; e = e.Next() {
		count++
		log.Println("value: ", e.Value)
	}
	assert.Equal(t, 4, count)
	status := step.Status()
	assert.Equal(t, nil, status.Err)
}

func TestParalleleWithError(t *testing.T) {

	ctx := context.WithValue(context.Background(), StatusKey, &ExecutionStatus{})
	l := list.New()
	step := NewParallelStep(ctx, []ExecutionFunc{exec1, exec2WithError, exec3WithError, exec4}, nil, l)
	step.Execute()

	assert.Equal(t, nil, ctx.Err())
	count := 0
	for e := l.Front(); e != nil; e = e.Next() {
		count++

		log.Println("value: ", e.Value)
	}
	assert.Equal(t, 2, count)
	assert.NotEqual(t, nil, step.Status().Err)
}
