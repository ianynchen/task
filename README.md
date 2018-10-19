# Task

## Introduction

Task is a simple go routine package to allow users to chain tasks sequentially or in parallel and execute them in goroutines. This package is created in an attempt to simplify both error handling as well as concurrent handling.

In golang program, you'd do the following to handle errors:

```go
response, err := executeSomething()

if err != nil {
    //handle error
} else {
    anotherResponse, anotherError := executeSomethingElse()

    if anotherError != nil {
        //handle error again
    } else {
        // process further
    }
}
```

Error handling in the above case becomes teidous, and breaks into normal processing logic. With task, this process can be simplified:

## Usage

Task comes in two flavors:

1. Simple tasks, where you provide a set of functions, and they are executed either sequentially or in parallel. In case of sequential execution, the execution loop stops when first error is encountered.
2. Staged tasks, where you can chain multiple tasks together in the form of a tree, and tasks along a chain are executed in sequence, while sibling nodes are executed in parallel.

### Simple Tasks

```go
l := list.New()
task := NewSimpleSerializedTask([]ExecutionFunc{exec1, exec2, exec3, exec4})
response, err := task.Execute()
```

You start by invoking ```NewSimpleSerializedTask``` which will execute the functions of your choice in sequence, and breaks on the first error.

### Multi-Stage Tasks

This is a more complex form, and allows one to chain multiple tasks into a tree strucutre. sibling tasks will be executed in parallel, while parent child will be executed sequentially.

```go
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

task1.AddChild(task2)
task2.AddChild(task3, task4)
response, err := task1.Execute("abcdefg")
```
