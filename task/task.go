package task

import (
	"log"
	"sync"
)

type Task interface {
	Execute(request interface{}) (interface{}, error)
}

type ExecutionError struct {
}

type NoPostProcessorError struct {
}

func (e NoPostProcessorError) Error() string {
	return "task has sub-tasks but no post process, does not know how to merge results from sub-tasks"
}

func (e ExecutionError) Error() string {
	return "cannot execute non-root tasks"
}

type ProcessorFunc func(request interface{}) (interface{}, error)

type PostProcessorFunc func(responses []interface{}) (interface{}, error)

type tuple struct {
	value interface{}
	err   error
}

type multiStageTask struct {
	parentSteps   []*multiStageTask
	childSteps    []*multiStageTask
	processor     ProcessorFunc
	postProcessor PostProcessorFunc
	done          chan error
}

func NewTask(processor ProcessorFunc, postProcessor PostProcessorFunc) *multiStageTask {
	return &multiStageTask{
		parentSteps:   nil,
		childSteps:    nil,
		processor:     processor,
		postProcessor: postProcessor,
		done:          make(chan error),
	}
}

func containsTask(s []*multiStageTask, e *multiStageTask) bool {
	if s == nil {
		return false
	}
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (current *multiStageTask) AddParent(parents ...*multiStageTask) {
	for _, parent := range parents {
		if parent == nil {
			continue
		}
		if !containsTask(current.parentSteps, parent) {
			if current.parentSteps == nil {
				current.parentSteps = make([]*multiStageTask, 0)
			}
			current.parentSteps = append(current.parentSteps, parent)
		}

		if !containsTask(parent.childSteps, current) {
			if parent.childSteps == nil {
				parent.childSteps = make([]*multiStageTask, 0)
			}
			parent.childSteps = append(parent.childSteps, current)
		}
	}
	current.printChildren()
}

func (current *multiStageTask) AddChild(children ...*multiStageTask) {
	for _, child := range children {
		if child == nil {
			continue
		}
		if !containsTask(current.childSteps, child) {
			if current.childSteps == nil {
				current.childSteps = make([]*multiStageTask, 0)
			}
			current.childSteps = append(current.childSteps, child)
		}

		if !containsTask(child.parentSteps, current) {
			if child.parentSteps == nil {
				child.parentSteps = make([]*multiStageTask, 0)
			}
			child.parentSteps = append(child.parentSteps, current)
		}
	}
	current.printChildren()
}

func (current *multiStageTask) printChildren() {
	log.Println("children is: ", current.childSteps)
	if current.childSteps != nil {
		for _, child := range current.childSteps {
			log.Println("child is: ", child)
		}
	}
}

/*
IsRoot returns true if this task is a root task: having no parents, or
false otherwise.
*/
func (current multiStageTask) IsRoot() bool {
	return current.parentSteps == nil || len(current.parentSteps) == 0
}

func (current multiStageTask) HasChildren() bool {
	return current.childSteps != nil && len(current.childSteps) > 0
}

func (current *multiStageTask) Execute(request interface{}) (interface{}, error) {

	if current.IsRoot() {
		return current.execute(request)
	}
	return nil, ExecutionError{}
}

func (current *multiStageTask) execute(request interface{}) (interface{}, error) {

	// if this task has multiple child tasks (more than 1), but don't have
	// a postProcessor to merge the results, panic
	if current.childSteps != nil && len(current.childSteps) > 1 && current.postProcessor == nil {
		panic(&NoPostProcessorError{})
	}

	// execute self
	var selfResponse = request
	var err error

	if current.processor != nil {
		c := make(chan tuple)
		go func() {
			defer close(c)
			response, err := current.processor(request)
			c <- tuple{value: response, err: err}
		}()
		select {
		case t := <-c:
			selfResponse = t.value
			err = t.err
			log.Println("execute response is: ", selfResponse)
			log.Println("execute error is: ", err)
		}
	}

	// execute children
	if !current.HasChildren() {
		log.Println("don't have children")
		return selfResponse, err
	}

	if err != nil {
		log.Println("error is not nil")
		return nil, err
	}

	if current.HasChildren() {
		out := make(chan tuple)
		var wg sync.WaitGroup
		log.Println("add ", len(current.childSteps), " to wait group")
		log.Println("out channel is ", out)
		wg.Add(len(current.childSteps))

		log.Println("has children size: ", len(current.childSteps))
		for _, child := range current.childSteps {
			if child != nil {
				go func(child *multiStageTask, request interface{}) {
					log.Println("inside child, out channel is ", out)
					log.Println("inside child, child: ", child)
					response, err := child.execute(selfResponse)
					wg.Done()
					out <- tuple{value: response, err: err}
				}(child, selfResponse)
			}
		}

		go func() {
			wg.Wait()
			defer close(out)
		}()

		responses, subTaskError := mergeResultIntoSlice(out)
		if current.postProcessor == nil && len(responses) == 1 {
			return responses[0], subTaskError
		} else {
			return current.postProcessor(responses)
		}
	}
	return selfResponse, err
}

func mergeResultIntoSlice(c chan tuple) ([]interface{}, error) {
	var values []interface{}
	var err error
	log.Println("gathering results from all child tasks")
	for t := range c {
		log.Println("gathering result: ", t.value, "error: ", t.err)
		values = append(values, t.value)
		if err == nil && t.err != nil {
			err = t.err
		}
	}
	return values, err
}
