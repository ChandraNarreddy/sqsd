package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"sqsd/task"
	"sqsd/taskQ"
)

type WorkerPoolIfc interface {
	CreateWorkers(task.TaskPerformerIfc, taskQ.TaskQueueIfc) error
}

type WorkerPool struct {
	numWorkers uint8
	context    context.Context
}

func (c *WorkerPool) CreateWorkers(taskPerformer task.TaskPerformerIfc, taskQueue taskQ.TaskQueueIfc) error {
	var i uint8 = 0
	for ; i < c.numWorkers; i++ {
		wrkr := &Worker{
			workerID: fmt.Sprintf("%d", i),
		}
		go func(wrkr WorkerIfc, performer task.TaskPerformerIfc) {
			//debug log statement here that worker has started
			slog.Debug("Worker is in action", "workerID", wrkr.WorkerID())
			for {
				br := false
				select {
				case <-c.context.Done():
					//info log here that the quit signal has been received
					slog.Info("Caught quit signal, terminating", "workerID", wrkr.WorkerID())
					br = true
				default:
					// this will be a blocking call
					task, popErr := wrkr.PopTaskFromTaskQueue(taskQueue)
					if popErr != nil {
						//error log about popping failure
						slog.Error("Failed to pop a task from taskQ", "workerID", wrkr.WorkerID(), "error", popErr.Error())
					} else {
						if performTaskErr := wrkr.PerformTask(performer, task); performTaskErr != nil {
							//error log about worker's failure to perform the task
							slog.Error("Failed to perform the task", "taskID", task.GetTaskID(), "workerID", wrkr.WorkerID(), "error", performTaskErr.Error())
						}
					}
				}
				if br == true {
					return
				}
			}
		}(wrkr, taskPerformer)
	}
	return nil
}

func CreateWorkerPool(ctx context.Context, number uint8, taskPerformer task.TaskPerformerIfc, taskQ taskQ.TaskQueueIfc) error {
	pool := &WorkerPool{
		numWorkers: number,
		context:    ctx,
	}
	return pool.CreateWorkers(taskPerformer, taskQ)
}
