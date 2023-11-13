package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sqsd/sqspoll"
	"sqsd/task"
	"sqsd/taskQ"
	"sqsd/workerpool"
)

func main() {

	queueName := flag.String("sqs", "", "SQS queue Name; mandatory ")
	region := flag.String("region", "us-west-2", "region; us-west-2 is the default")
	visibilityTimeoutSeconds := flag.Int("vis", 60*1, "SQS visibility timeout in seconds. This timeout will be set in the ReceiveMessage call to SQS. Default 60*1")
	errorVisibilityTimeoutSeconds := flag.Int("errVis", 60*5, "Time in seconds to wait before a message is made visible in the queue after an attempt to process it fails with an error. Default is 60*5")
	maxNumberOfMessagesToFetch := flag.Int("maxPoll", 10, "Max messages to fetch at once; 10 by default")
	httpPath := flag.String("forward", "", "path to forward the request, needs to be a valid URL")
	mimeType := flag.String("mime", "application/json", "mime type for the request; defaults to application/json")
	maxRetries := flag.Int("maxRetries", 10, "max retries; default 10")
	maxRetriesSQSDeletion := flag.Int("maxRetriesSQSDeletion", 1, "max retries for deletion of messages from SQS; default 1")
	maxConcurrentConnections := flag.Int("maxConcurrentConns", 50, "max concurrent connections to the upstream")
	connectionTimeOutSecs := flag.Int("connTimeout", 5, "maximum connection timeout in seconds; default 5 seconds")
	responseTimeoutSecs := flag.Int("responseTimeout", 120, "maximum time to wait for response in seconds; default 120 seconds")
	taskQBufferSize := flag.Int("bufferSize", 100, "Internal task queue's buffer size to hold incoming SQS messages; default 100")
	workerPoolSize := flag.Int("workersCount", 20, "Number of concurrent workers to spawn; default 20")
	logLevel := flag.Int("logLevel", 0, "logging level. Pass -4 for DEBUG, 0 for INFO, 4 for WARN, 8 for ERROR; default 0 - INFO.")
	flag.Parse()

	if *queueName == "" {
		log.Fatal("queueName is not defined, quitting!!")
	}
	if *httpPath == "" {
		log.Fatal("httpPath is not defined, quitting!!")
	}

	//create a logger here
	var programLevel = new(slog.LevelVar) // Info by default
	switch *logLevel {
	case -4:
		programLevel.Set(slog.LevelDebug)
	case 0:
		programLevel.Set(slog.LevelInfo)
	case 4:
		programLevel.Set(slog.LevelWarn)
	case 8:
		programLevel.Set(slog.LevelError)
	default:
		log.Fatalf("logLevel %d passed is not supported", *logLevel)
	}

	slogger := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{AddSource: true, Level: programLevel})
	slog.SetDefault(slog.New(slogger))

	// create task queue channel here
	taskQ := taskQ.CreateTaskQ(uint8(*taskQBufferSize))
	//create a SQS poller here
	sqsPoller, SQSPollerErr := sqspoll.CreateSQSPoller(*queueName, *region, uint16(*visibilityTimeoutSeconds), uint16(*maxNumberOfMessagesToFetch))
	if SQSPollerErr != nil {
		log.Fatalf("SQSD failed to run due to SQS poller creation failure - %#v", SQSPollerErr)
	}
	//creating a poller goroutine here to poll for messages and dumping them to TaskQueue
	pollerQuitChan := make(chan int)
	go func(sqsPoller *sqspoll.SQSPoller) {
		br := false
		for {
			select {
			case <-pollerQuitChan:
				slog.Info("sqsPoller caught quit signal, terminating")
				br = true
			default:
				pollerErr := sqsPoller.FetchAndPushToQueue(taskQ)
				if pollerErr != nil {
					//error log here about poller failure to fetch and push to queue
					slog.Error("Error in fetching and pushing tasks to queue", "error", pollerErr.Error())
				}
			}
			if br {
				break
			}
		}
	}(sqsPoller)

	//create a task performer here
	taskPerformer, createTaskPerformerErr := task.CreateSQSDTaskPerformer(*queueName, *region, *httpPath, *mimeType,
		uint8(*maxRetries), uint8(*maxRetriesSQSDeletion), uint8(*maxConcurrentConnections), uint8(*connectionTimeOutSecs),
		uint8(*responseTimeoutSecs), uint16(*errorVisibilityTimeoutSeconds))
	if createTaskPerformerErr != nil {
		log.Fatalf("Failed to create task performer, quitting!!\nError stack - \n%#v", createTaskPerformerErr)
	}
	//create a worker pool here and create workers
	wrkrCtx, wrksCtxCancel := context.WithCancel(context.Background())
	wrkrPoolErr := workerpool.CreateWorkerPool(wrkrCtx, uint8(*workerPoolSize), taskPerformer, taskQ)
	if wrkrPoolErr != nil {
		log.Fatalf("Failed to create worker pool, quitting!!\nError stack - \n%#v", wrkrPoolErr)
	}

	//create a channel for OS interrupt events and listen for them
	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, os.Interrupt, os.Kill)
	<-quitSig

	pollerQuitChan <- 1
	close(pollerQuitChan)
	wrksCtxCancel()
}
