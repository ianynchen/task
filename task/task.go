package task

import (
	"log"
	"sync"

	"github.com/google/uuid"
)

/*
ExecutionError thrown when trying to execute a non-root task.
*/
type ExecutionError struct {
}

/*
NoPostProcessorError thrown when a task has more than 1 sub-tasks, but isn't
configured with a post processor to merge responses from sub-tasks into
a single response
*/
type NoPostProcessorError struct {
}

func (e NoPostProcessorError) Error() string {
	return "task has sub-tasks but no post process, does not know how to merge results from sub-tasks"
}

func (e ExecutionError) Error() string {
	return "cannot execute non-root tasks"
}

type tuple struct {
	value interface{}
	err   error
}

type multiStageTask struct {
	Name          string
	parentSteps   []*multiStageTask
	childSteps    []*multiStageTask
	processor     ExecutionFunc
	postProcessor MergeFunc
	done          chan error
}

/*
NewTask creates a new task wil processor and possibly post-processor
*/
func NewTask(processor ExecutionFunc, postProcessor MergeFunc) *multiStageTask {
	return &multiStageTask{
		parentSteps:   nil,
		childSteps:    nil,
		processor:     processor,
		postProcessor: postProcessor,
		done:          make(chan error),
		Name:          uuid.New().String(),
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

/*
AddChild adds a sub-task to a parent
*/
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
}

/*
IsRoot returns true if this task is a root task: having no parents, or
false otherwise.
*/
func (current multiStageTask) IsRoot() bool {
	return current.parentSteps == nil || len(current.parentSteps) == 0
}

/*
HasChildren returns true if this task has child task(s)
*/
func (current multiStageTask) HasChildren() bool {
	return current.childSteps != nil && len(current.childSteps) > 0
}

/*
executeSelf executes a task asynchronously without invoking its children if any
*/
func (current *multiStageTask) executeSelf(request interface{}) (interface{}, error) {

	// if this task has multiple child tasks (more than 1), but don't have
	// a postProcessor to merge the results, panic
	if current.childSteps != nil && len(current.childSteps) > 1 && current.postProcessor == nil {
		panic(&NoPostProcessorError{})
	}

	// execute self
	var response = request
	var err error

	if current.processor != nil {
		out := make(chan tuple)
		go func() {
			defer close(out)
			response, err := current.processor(request)
			out <- tuple{value: response, err: err}
		}()
		select {
		case t := <-out:
			response = t.value
			err = t.err
		}
	}
	return response, err
}

/*
Execute synchronous form to execute a parent task and get results
*/
func (current *multiStageTask) Execute(request interface{}) (interface{}, error) {
	return current.execute(request)
}

func (current *multiStageTask) hasMoreThanOneChild() bool {
	return current.childSteps != nil && len(current.childSteps) > 1
}

func (current *multiStageTask) execute(request interface{}) (interface{}, error) {

	response, err := current.executeSelf(request)
	log.Println("in task: ", current.Name, " executeSelf returned: ", response, " err: ", err)

	// execute children
	if !current.HasChildren() || err != nil {
		log.Println("task: ", current.Name, " don't have children or error")
		log.Println("task: ", current.Name, " returning: ", response, " err: ", err)
		return response, err
	}

	log.Println("task: ", current.Name, " has children: ", current.HasChildren())
	if current.HasChildren() {
		log.Println("task: ", current.Name, " has children, now processing")
		outputs := make([]interface{}, len(current.childSteps))
		var firstError error
		for index, child := range current.childSteps {
			resp, childErr := child.execute(response)

			if firstError == nil && childErr != nil {
				firstError = childErr
			}
			outputs[index] = resp
		}

		if firstError != nil {
			return nil, firstError
		}

		if current.hasMoreThanOneChild() {
			return current.postProcessor(outputs)
		}
		return outputs[0], firstError
	}
	return response, err
}

func mergeChan(channels []<-chan tuple) <-chan tuple {
	out := make(chan tuple)

	for _, c := range channels {
		go func(c <-chan tuple) {
			for v := range c {
				out <- v
			}
		}(c)
	}
	var wg sync.WaitGroup
	wg.Add(len(channels))
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
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
