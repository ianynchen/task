package task

import (
	"context"
	"sync"
)

const (
	/*SERIAL child steps are to be executed serially
	 */
	SERIAL int = iota
	/*PARALLELE child steps are to be executed in parallele
	 */
	PARALLELE
)

/*
ExecutionFunc completes an execution step,
ctx: context to use
cancelFunc: cancel function to call in case of error
request: request object
response: return result of the execution
*/
type ExecutionFunc func(ctx context.Context, cancelFunc context.CancelFunc, request interface{}, response interface{})

/*
CtxKey key type for context
*/
type CtxKey string

/*
StatusKey status key used for context object
*/
const StatusKey = CtxKey("status")

/*
ExecutionStep a step in execution
*/
type ExecutionStep interface {
	Execute()
}

/*
ExecutionStatus contains error if any for executed steps
*/
type ExecutionStatus struct {
	Err error
}

type simpleStep struct {
	context       context.Context
	cancelFunc    context.CancelFunc
	executions    []ExecutionFunc
	executionMode int
	request       interface{}
	response      interface{}
}

/*
NewParalleleStep creates a simple step in execution
parent: parent context, each step will create a new child context using WithCancel
executions: functions to be executed in this ExecutionStep
request: request object
response: response object
*/
func NewParallelStep(parent context.Context, executions []ExecutionFunc, request interface{}, response interface{}) simpleStep {
	ctx, cancelFunc := context.WithCancel(parent)
	return simpleStep{
		context:       ctx,
		cancelFunc:    cancelFunc,
		executions:    executions,
		executionMode: PARALLELE,
		request:       request,
		response:      response,
	}
}

/*
NewSerializedStep creates a simple step in execution
parent: parent context, each step will create a new child context using WithCancel
executions: functions to be executed in this ExecutionStep, will be executed in sequence
request: request object
response: response object
*/
func NewSerializedStep(parent context.Context, executions []ExecutionFunc, request interface{}, response interface{}) simpleStep {
	ctx, cancelFunc := context.WithCancel(parent)
	return simpleStep{
		context:       ctx,
		cancelFunc:    cancelFunc,
		executions:    executions,
		executionMode: SERIAL,
		request:       request,
		response:      response,
	}
}

func (step *simpleStep) Execute() {
	switch step.executionMode {
	case SERIAL:
		step.executeSerial()
	case PARALLELE:
		step.executeParallel()
	}
}

func (step simpleStep) Status() *ExecutionStatus {
	status := step.context.Value(StatusKey)
	if status != nil {
		if statusStruct, ok := status.(*ExecutionStatus); ok {
			return statusStruct
		}
		return nil
	}
	return nil
}

func (step *simpleStep) IsHealthy() bool {
	status := step.Status()
	if status != nil {
		return status.Err == nil
	}
	return false
}

func (step *simpleStep) executeSerial() {
	for _, execFunc := range step.executions {
		execFunc(step.context, step.cancelFunc, step.request, step.response)

		if !step.IsHealthy() {
			break
		}
	}
}

func (step *simpleStep) executeParallel() {
	var wg sync.WaitGroup
	for _, execFunc := range step.executions {
		wg.Add(1)
		go func(function ExecutionFunc) {
			function(step.context, step.cancelFunc, step.request, step.response)
			wg.Done()
		}(execFunc)
	}
	wg.Wait()
}
