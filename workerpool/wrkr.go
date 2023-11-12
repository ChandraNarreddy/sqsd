package workerpool

import (
	"sqsd/task"
	"sqsd/taskQ"
)

type WorkerIfc interface {
	PopTaskFromTaskQueue(taskQ.TaskQueueIfc) (task.TaskIfc, error)
	PerformTask(task.TaskPerformerIfc, task.TaskIfc) error
	WorkerID() string
}

type Worker struct {
	workerID string
}

func (c *Worker) PopTaskFromTaskQueue(taskQ taskQ.TaskQueueIfc) (task.TaskIfc, error) {
	return taskQ.Pop()
}

func (c *Worker) PerformTask(taskPerformer task.TaskPerformerIfc, task task.TaskIfc) error {
	return taskPerformer.PerformTask(task)
}

func (c *Worker) WorkerID() string {
	return c.workerID
}
