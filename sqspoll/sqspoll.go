package sqspoll

import (
	"context"
	"log/slog"
	"sqsd/task"
	"sqsd/taskQ"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	SQSTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go/aws"
)

type SQSPollerIfc interface {
	FetchAndPushToQueue(taskQ.TaskQueueIfc) error
}

type SQSReceiveMessageIfc interface {
	ReceiveMessage(ctx context.Context,
		params *sqs.ReceiveMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
}

type SQSPoller struct {
	sqsClient SQSReceiveMessageIfc
	//queueName                  string
	//region                     string
	visibilityTimeoutSeconds   uint16
	queueUrl                   string
	maxNumberOfMessagesToFetch uint16
}

func (c *SQSPoller) FetchAndPushToQueue(taskQ taskQ.TaskQueueIfc) error {
	recvMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:       aws.String(c.queueUrl),
		AttributeNames: []SQSTypes.QueueAttributeName{SQSTypes.QueueAttributeNameAll},
		MessageAttributeNames: []string{
			string(SQSTypes.QueueAttributeNameAll),
		},
		MaxNumberOfMessages: int32(c.maxNumberOfMessagesToFetch),
		VisibilityTimeout:   int32(c.visibilityTimeoutSeconds),
	}
	recvMsgOutput, recvMsgErr := c.sqsClient.ReceiveMessage(context.TODO(), recvMessageInput)
	if recvMsgErr != nil {
		// error log about receive message error
		slog.Error("Error encountered in receive message call", "error", recvMsgErr.Error())
		return recvMsgErr
	}
	for _, msg := range recvMsgOutput.Messages {
		task := &task.SQSDTask{}
		task.SetSQSMessage(msg)
		task.SetTaskID(*msg.MessageId)
		// this will be a blocking call
		stackingErr := taskQ.Stack(task)
		if stackingErr != nil {
			// error log about stacking failure
			slog.Error("Error stacking task to message queue", "error", stackingErr.Error())
		}
	}
	return nil
}

// CreateSQSClient is a helper function for creating an SQS client type for polling from AWS SQS
func CreateSQSPoller(queueName string, region string, visibilityTimeoutSeconds uint16, maxNumberOfMessagesToFetch uint16) (*SQSPoller, error) {
	if visibilityTimeoutSeconds > 12*60*60 {
		slog.Info("visibility timeout cannot be more than 12 hours, setting it to 12 hours")
		visibilityTimeoutSeconds = 12 * 60 * 60
	}
	cfg, cfgErr := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if cfgErr != nil {
		//panic("configuration error, " + err.Error())
		slog.Error("SQS Poller AWS Config Error", "error", cfgErr.Error())
		return nil, cfgErr
	}
	client := sqs.NewFromConfig(cfg)
	qURLInput := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}
	qURLResult, qURLErr := client.GetQueueUrl(context.TODO(), qURLInput)
	if qURLErr != nil {
		//error log here about queue being unresolvable or non-existant.
		slog.Error("queueURL is invalid, could not create SQSPoller", "error", qURLErr.Error())
		return nil, qURLErr
	}
	queueURL := qURLResult.QueueUrl
	return &SQSPoller{
		sqsClient:                  client,
		visibilityTimeoutSeconds:   visibilityTimeoutSeconds,
		queueUrl:                   *queueURL,
		maxNumberOfMessagesToFetch: maxNumberOfMessagesToFetch,
	}, nil
}
