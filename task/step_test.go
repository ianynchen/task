package task

import (
	"container/list"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func exec1(request interface{}) (interface{}, error) {
	log.Println("executing 1")
	time.Sleep(1000 * time.Millisecond)
	list := request.(*list.List)
	list.PushBack(1)
	return list, nil
}

func exec2(request interface{}) (interface{}, error) {
	log.Println("executing 2")
	time.Sleep(1000 * time.Millisecond)
	list := request.(*list.List)
	list.PushBack(2)
	return list, nil
}

func exec3(request interface{}) (interface{}, error) {
	log.Println("executing 3")
	time.Sleep(1000 * time.Millisecond)
	list := request.(*list.List)
	list.PushBack(3)
	return list, nil
}

func exec4(request interface{}) (interface{}, error) {
	log.Println("executing 4")
	time.Sleep(1000 * time.Millisecond)
	list := request.(*list.List)
	list.PushBack(4)
	return list, nil
}

func exec2WithError(request interface{}) (interface{}, error) {
	log.Println("executing 2")
	time.Sleep(1000 * time.Millisecond)
	return nil, errors.New("err2")
}

func exec3WithError(request interface{}) (interface{}, error) {
	log.Println("executing 3")
	time.Sleep(1000 * time.Millisecond)
	return nil, errors.New("err3")
}

func TestSerial(t *testing.T) {

	l := list.New()
	task := NewSimpleSerializedTask([]ExecutionFunc{exec1, exec2, exec3, exec4})
	result, err := task.Execute(l)

	assert.Nil(t, err)
	var first, second *list.Element
	count := 0
	for e := result.(*list.List).Front(); e != nil; e = e.Next() {
		first = second
		second = e
		count = count + 1

		log.Println("value: ", e.Value)
		if first != nil && second != nil {
			assert.True(t, first.Value.(int) < second.Value.(int))
		}
	}
	assert.Equal(t, 4, count)
}

func TestSerialWithError(t *testing.T) {

	l := list.New()
	task := NewSimpleSerializedTask([]ExecutionFunc{exec1, exec2WithError, exec3, exec4})
	result, err := task.Execute(l)

	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestParallel(t *testing.T) {

	l := list.New()
	task := NewSimpleParallelTask([]ExecutionFunc{exec1, exec2, exec3, exec4}, func(results []interface{}) (interface{}, error) {
		return results[0], nil
	})
	result, err := task.Execute(l)

	assert.Nil(t, err)
	count := 0
	for e := result.(*list.List).Front(); e != nil; e = e.Next() {
		count++
		log.Println("value: ", e.Value)
	}
	assert.Equal(t, 4, count)
}

func TestParalleleWithError(t *testing.T) {

	l := list.New()
	task := NewSimpleParallelTask([]ExecutionFunc{exec1, exec2WithError, exec3WithError, exec4}, func(results []interface{}) (interface{}, error) {
		return results[0], nil
	})
	result, err := task.Execute(l)

	assert.NotNil(t, err)
	assert.Nil(t, result)
}
