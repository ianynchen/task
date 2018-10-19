package task

import (
	"errors"
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
request: request object
produces a response and a possible error
*/
type ExecutionFunc func(request interface{}) (interface{}, error)

/*
MergeFunc merges multiple responses into one and produces a possible error
*/
type MergeFunc func(responses []interface{}) (interface{}, error)

func execute(execFuncs []ExecutionFunc, request interface{}, merger MergeFunc) (interface{}, error) {
	outputs := make([]tuple, len(execFuncs))
	var wg sync.WaitGroup
	wg.Add(len(execFuncs))
	for idx, function := range execFuncs {
		go func(function ExecutionFunc, request interface{}, outputs []tuple, index int) {
			response, err := function(request)
			outputs[index] = tuple{value: response, err: err}
			wg.Done()
		}(function, request, outputs, idx)
	}
	wg.Wait()

	responses, err := mergeResults(outputs)

	if err != nil {
		return nil, err
	}
	return merger(responses)
}

func mergeResults(results []tuple) ([]interface{}, error) {
	var err error
	response := make([]interface{}, len(results))
	for idx, result := range results {
		response[idx] = result.value

		if err == nil && result.err != nil {
			err = result.err
		}
	}
	return response, err
}

/*
ExecutionStep a step in execution
*/
type ExecutionStep interface {
	Execute(request interface{}) (interface{}, error)
}

type simpleTask struct {
	executions    []ExecutionFunc
	merger        MergeFunc
	executionMode int
}

/*
NewSimpleParallelTask creates a simple step in execution
executions: functions to be executed in this ExecutionStep
*/
func NewSimpleParallelTask(executions []ExecutionFunc, merger MergeFunc) simpleTask {
	return simpleTask{
		executions:    executions,
		merger:        merger,
		executionMode: PARALLELE,
	}
}

/*
NewSimpleSerializedTask creates a simple step in execution
executions: functions to be executed in this ExecutionStep, will be executed in sequence
*/
func NewSimpleSerializedTask(executions []ExecutionFunc) simpleTask {
	return simpleTask{
		executions:    executions,
		executionMode: SERIAL,
	}
}

func (task *simpleTask) Execute(request interface{}) (interface{}, error) {
	switch task.executionMode {
	case SERIAL:
		return task.executeSerial(request)
	case PARALLELE:
		return task.executeParallel(request)
	}
	return nil, errors.New("incorrect execution mode")
}

func (task *simpleTask) executeSerial(request interface{}) (interface{}, error) {
	for _, execFunc := range task.executions {
		c := make(chan tuple)
		go func(exec ExecutionFunc) {
			defer close(c)
			response, err := exec(request)
			c <- tuple{value: response, err: err}
		}(execFunc)

		var err error
		select {
		case t := <-c:
			// use response from previous step as request for next step
			request = t.value
			err = t.err
		}

		if err != nil {
			return nil, err
		}
	}
	return request, nil
}

func (task *simpleTask) executeParallel(request interface{}) (interface{}, error) {
	return execute(task.executions, request, task.merger)
}
