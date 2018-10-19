package task

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddChild(t *testing.T) {
	task1 := NewTask(nil, nil)
	task2 := NewTask(nil, nil)
	task3 := NewTask(nil, nil)
	task4 := NewTask(nil, nil)
	task5 := NewTask(nil, nil)
	task6 := NewTask(nil, nil)
	task7 := NewTask(nil, nil)
	task8 := NewTask(nil, nil)

	task1.AddChild(task2, task3)
	task2.AddChild(task5, task6, task7)
	task5.AddChild(task8)

	assert.Equal(t, 0, len(task4.parentSteps))
	assert.Equal(t, 0, len(task4.childSteps))
	assert.Equal(t, 0, len(task1.parentSteps))
	assert.Equal(t, 2, len(task1.childSteps))
	assert.Equal(t, 1, len(task2.parentSteps))
	assert.Equal(t, 3, len(task2.childSteps))
	assert.Equal(t, 1, len(task5.parentSteps))
	assert.Equal(t, 1, len(task5.childSteps))
}

func processString(input interface{}) (interface{}, error) {
	content := input.(string)
	if len(content) == 0 {
		return "a", nil
	}
	last := content[len(content)-1]
	content = content + string(last+1)
	return content, nil
}

func stringlen(input interface{}) (interface{}, error) {
	content := input.(string)
	return len(content), nil
}

func square(input interface{}) (interface{}, error) {
	content := input.(int)
	return content * content, nil
}

func TestSerialExecution(t *testing.T) {
	task1 := NewTask(processString, nil)
	task1.Name = "task1"
	task2 := NewTask(stringlen, nil)
	task2.Name = "task2"
	task3 := NewTask(square, nil)
	task3.Name = "task3"
	assert.False(t, task3.HasChildren())

	task1.AddChild(task2)
	task2.AddChild(task3)
	log.Println("task2 has children: ", task2.HasChildren())
	resp, err := task1.Execute("abcdefg")
	assert.Nil(t, err)
	assert.NotNil(t, resp)

	log.Println("received from test: ", resp, " error: ", err)
	value, ok := resp.(int)
	assert.True(t, ok)
	assert.Equal(t, 64, value)
}

func cubic(input interface{}) (interface{}, error) {
	value := input.(int)
	return value * value * value, nil
}

func BenchmarkSerialerialTasks(b *testing.B) {
	task1 := NewTask(processString, nil)
	task2 := NewTask(stringlen, nil)
	task3 := NewTask(square, nil)

	task1.AddChild(task2)
	task2.AddChild(task3)

	b.StopTimer()
	for i := 0; i < b.N; i++ {
		response, _ := task1.Execute("abcdefg")
		value, _ := response.(int)
		log.Println("value is ", value)
	}
}

func TestParallelExecution(t *testing.T) {
	task1 := NewTask(processString, nil)
	task2 := NewTask(stringlen, func(inputs []interface{}) (interface{}, error) {
		value := 0
		for _, input := range inputs {
			value += input.(int)
		}
		return value, nil
	})
	task3 := NewTask(square, nil)
	task4 := NewTask(cubic, nil)
	assert.False(t, task3.HasChildren())

	task1.AddChild(task2)
	task2.AddChild(task3, task4)
	response, err := task1.Execute("abcdefg")
	assert.Nil(t, err)
	assert.NotNil(t, response)

	value, ok := response.(int)
	assert.True(t, ok)
	assert.Equal(t, 576, value)
}
