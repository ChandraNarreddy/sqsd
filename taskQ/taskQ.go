package taskQ

import (
	"sqsd/task"
)

type TaskQueueIfc interface {
	Stack(task.TaskIfc) error
	Pop() (task.TaskIfc, error)
}

// TaskQueue implementation uses a buffered channel for stacking
// incoming tasks in buffer
// Workers can pop tasks from the channel
type TaskQueue struct {
	//maxWaiting       uint64
	taskQueueChannel chan task.TaskIfc
}

func (c *TaskQueue) Stack(task task.TaskIfc) error {
	// this is a blocking call in case the channel is already full
	c.taskQueueChannel <- task
	return nil
}

func (c *TaskQueue) Pop() (task.TaskIfc, error) {
	// this is a blocking call in case there are no tasks in the channel
	return <-c.taskQueueChannel, nil
}

// creates a buffered channel with maxWaiting as size
func CreateTaskQ(size uint8) *TaskQueue {
	return &TaskQueue{
		//maxWaiting:       size,
		taskQueueChannel: make(chan task.TaskIfc, size),
	}
}
