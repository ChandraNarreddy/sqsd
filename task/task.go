package task

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	SQSTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go/aws"
)

type SQSTaskStatus uint8

type TaskIfc interface {
	GetSQSMessage() SQSTypes.Message
	SetSQSMessage(SQSTypes.Message)
	GetTaskID() string
	SetTaskID(string)
	GetTaskStatus() SQSTaskStatus
	SetTaskStatus(SQSTaskStatus)
}

type SQSDTask struct {
	sqsMessage SQSTypes.Message
	taskStatus SQSTaskStatus
	taskID     string
}

// this is a mapping of the standard SQS message attribute keys to the header
// keys that SQSD will pass to the backend
var SQSAttributeMap = map[string]string{
	"ApproximateFirstReceiveTimestamp": "X-Aws-Sqsd-First-Received-At",
	"ApproximateReceiveCount":          "X-Aws-Sqsd-Receive-Count",
	"SenderId":                         "X-Aws-Sqsd-Sender-Id",
	"MessageGroupId":                   "X-Aws-Sqsd-Message-Attr-Group-Id",
	"SequenceNumber":                   "X-Aws-Sqsd-Attr-Sequence-Number",
	"SentTimestamp":                    "X-Aws-Sqsd-Attr-Sent-Timestamp",
	"MessageDeduplicationId":           "X-Aws-Sqsd-Attr-Message-Deduplication-Id",
}

func (c *SQSDTask) SetSQSMessage(message SQSTypes.Message) {
	c.sqsMessage = message
}

func (c *SQSDTask) GetSQSMessage() SQSTypes.Message {
	return c.sqsMessage
}

func (c *SQSDTask) SetTaskID(taskID string) {
	c.taskID = taskID
}

func (c *SQSDTask) GetTaskID() string {
	return c.taskID
}

func (c *SQSDTask) SetTaskStatus(status SQSTaskStatus) {
	c.taskStatus = status
}

func (c *SQSDTask) GetTaskStatus() SQSTaskStatus {
	return c.taskStatus
}

type TaskPerformerIfc interface {
	PerformTask(TaskIfc) error
}

type SQSHandlrIfc interface {
	DeleteMessage(ctx context.Context,
		params *sqs.DeleteMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context,
		params *sqs.ChangeMessageVisibilityInput,
		optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

type SQSDTaskPerformer struct {
	httpClient                    *http.Client
	sqsClient                     SQSHandlrIfc
	queueURL                      string
	httpPath                      string
	queueName                     string
	maxRetries                    uint8
	maxRetriesSQSDeletion         uint8
	mimeType                      string
	maxConcurrentConnections      uint8
	connectionTimeOutSecs         uint8
	responseTimeoutSecs           uint8
	errorVisibilityTimeoutSeconds uint16
}

func (c *SQSDTaskPerformer) PerformTask(task TaskIfc) error {
	SQSDTask, ok := task.(*SQSDTask)
	if !ok {
		slog.Error("Failed to perform task", "taskID", task.GetTaskID(), "error", "Task passed is not of type SQSDTask")
		return fmt.Errorf("task passed is not of type SQSDTask")
	}
	if recvCount, ok := SQSDTask.sqsMessage.Attributes["ApproximateReceiveCount"]; ok {
		count, convErr := strconv.Atoi(recvCount)
		if convErr != nil {
			slog.Error("Error performing task", "taskID", task.GetTaskID(), "error", fmt.Sprintf("ApproximateReceiveCount attribute in the message is not a valid integer - %#v", convErr.Error()))
			return fmt.Errorf("error while parsing ApproximateReceiveCount attribute in the message to an integer")
		}
		if count > int(c.maxRetries) {
			slog.Debug("Max retries already attempted on this message, not attempting further", "messageID", *task.GetSQSMessage().MessageId)
			return nil
		}
	}
	req, reqErr := http.NewRequest("POST", c.httpPath, strings.NewReader(*SQSDTask.sqsMessage.Body))
	if reqErr != nil {
		//error logging here
		slog.Error("Error performing task", "taskID", task.GetTaskID(), "error", "Error while creating request to downstream")
		return fmt.Errorf("error while creating a request to the downstream")
	}
	// add SQS message attributes as headers
	req.Header.Add("User-Agent", "ZenSQSD")
	req.Header.Add("X-Aws-Sqsd-Msgid", *SQSDTask.sqsMessage.MessageId)
	req.Header.Add("X-Aws-Sqsd-Queue", c.queueName)
	req.Header.Add("Content-Type", c.mimeType)
	for k, v := range SQSAttributeMap {
		if SQSDTask.sqsMessage.Attributes[k] != "" {
			req.Header.Add(v, SQSDTask.sqsMessage.Attributes[k])
		}
	}

	//for custom SQS attributes, all attribs which are of type binary are base-64-encoded and added
	// with X-Aws-Sqsd-Attr- as the key prefix. All non-binary types are returned anyway as string values and hence passed as is.
	for k, v := range SQSDTask.sqsMessage.MessageAttributes {
		if *v.DataType == "Binary" && v.BinaryValue != nil {
			req.Header.Add("X-Aws-Sqsd-Attr-"+k, base64.RawStdEncoding.EncodeToString(v.BinaryValue))
		} else if *v.DataType != "Binary" && v.StringValue != nil {
			req.Header.Add("X-Aws-Sqsd-Attr-"+k, *v.StringValue)
		}
	}

	resp, respErr := c.httpClient.Do(req)
	if respErr != nil {
		//log here DEBUG
		slog.Debug("Error obtaining response from upstream", "messageID", *task.GetSQSMessage().MessageId, "error", respErr.Error())
		//change visibility timeout of the message here
		_, opErr := c.sqsClient.ChangeMessageVisibility(context.TODO(),
			&sqs.ChangeMessageVisibilityInput{
				QueueUrl:          &c.queueURL,
				ReceiptHandle:     task.GetSQSMessage().ReceiptHandle,
				VisibilityTimeout: *aws.Int32(int32(c.errorVisibilityTimeoutSeconds)),
			},
		)
		if opErr != nil {
			slog.Error("Error changing visibility timeout on message", "messageID", *task.GetSQSMessage().MessageId, "error", opErr.Error())
		}
		return fmt.Errorf("error obtaining response from upstream for message ID %#v", *task.GetSQSMessage().MessageId)
	}
	_, readBodyErr := io.ReadAll(resp.Body)
	if readBodyErr != nil {
		// here throwing an error log to warn about error while reading the body
		slog.Debug("Failed to completely read response body from server", "messageID", *task.GetSQSMessage().MessageId, "error", readBodyErr.Error())
		// we will continue to process the request as usual since we are not concerned with response body
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		slog.Debug("Non-200 response received from upstream", "messageID", *task.GetSQSMessage().MessageId, "code", resp.StatusCode)
		//change visibility timeout of the message here
		_, opErr := c.sqsClient.ChangeMessageVisibility(context.TODO(),
			&sqs.ChangeMessageVisibilityInput{
				QueueUrl:          &c.queueURL,
				ReceiptHandle:     task.GetSQSMessage().ReceiptHandle,
				VisibilityTimeout: *aws.Int32(int32(c.errorVisibilityTimeoutSeconds)),
			},
		)
		if opErr != nil {
			slog.Error("Error changing visibility timeout on message", "messageID", *task.GetSQSMessage().MessageId, "error", opErr.Error())
		}
		return fmt.Errorf("Non-200 response received from upstream for message ID %#v", *task.GetSQSMessage().MessageId)
	}
	//debug log about successful message POST
	slog.Debug("200 OK response received from upstream", "messageID", *task.GetSQSMessage().MessageId)

	messageSuccessfullyDeleted := false
	// giving the delete message operation maxRetriesSQSDeletion chances
	for j := 0; j < int(c.maxRetriesSQSDeletion); j++ {
		deleteMsgInput := &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(c.queueURL),
			ReceiptHandle: SQSDTask.sqsMessage.ReceiptHandle,
		}
		_, deleteErr := c.sqsClient.DeleteMessage(context.TODO(), deleteMsgInput)
		if deleteErr != nil {
			//error log about delete operation failing
			slog.Debug("Failed to delete message from SQS", "attmept#", j+1, "messageID", *task.GetSQSMessage().MessageId, "error", deleteErr.Error())
			if j == int(c.maxRetriesSQSDeletion-1) {
				// error log here
				slog.Debug("Max tries exhausted, Failed to delete message from SQS. Giving up now", "messageID", *task.GetSQSMessage().MessageId, "last_attempt_error", deleteErr.Error())
			}
		} else {
			//debug log here about successful deletion from SQS
			slog.Debug("Deleted message from SQS", "attmept#", j+1, "messageID", *task.GetSQSMessage().MessageId)
			messageSuccessfullyDeleted = true
			break
		}
	}
	if !messageSuccessfullyDeleted {
		return fmt.Errorf("failed to delete message with ID %s from SQS", *task.GetSQSMessage().MessageId)
	}
	return nil
}

// CreateSQSDTaskPerformer is a helper function that creates a SQSDTaskPerformer
func CreateSQSDTaskPerformer(queueName string, region string, httpPath string, mimeType string, maxRetries uint8,
	maxRetriesSQSDeletion uint8, maxConcurrentConnections uint8, connectionTimeOutSecs uint8, responseTimeoutSecs uint8,
	errorVisibilityTimeoutSeconds uint16) (*SQSDTaskPerformer, error) {

	//validate the httpPath parameter to be a valid URL first here
	_, pathErr := url.Parse(httpPath)
	if pathErr != nil {
		//log here about path being incorrect
		slog.Error("http path is not a valid URL", "path", httpPath, "error", pathErr)
		return nil, fmt.Errorf("http path %s is not a valid URL - %#v", httpPath, pathErr)
	}
	if errorVisibilityTimeoutSeconds > 12*60*60 {
		slog.Info("error visibility timeout cannot be more than 12 hours, setting it to 12 hours")
		errorVisibilityTimeoutSeconds = 12 * 60 * 60
	}
	timeOutDuration := time.Duration(int64(connectionTimeOutSecs+responseTimeoutSecs) * 1000 * 1000 * 1000)
	client := &http.Client{
		Timeout: timeOutDuration,
		Transport: &http.Transport{
			DisableKeepAlives: false,
			MaxIdleConns:      int(maxConcurrentConnections + 5),
			MaxConnsPerHost:   int(maxConcurrentConnections),
		},
	}

	cfg, cfgErr := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if cfgErr != nil {
		//panic("configuration error, " + err.Error())
		slog.Error("SQSDTaskPerformer AWS Config Error", "error", cfgErr.Error())
		return nil, cfgErr
	}
	sqsClient := sqs.NewFromConfig(cfg)
	qURLInput := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}
	qURLResult, qURLErr := sqsClient.GetQueueUrl(context.TODO(), qURLInput)
	if qURLErr != nil {
		slog.Error("queueURL is invalid, could not create SQSDTaskPerformer", "error", qURLErr.Error())
		return nil, qURLErr
	}
	queueURL := qURLResult.QueueUrl

	//Creating the task performer here
	return &SQSDTaskPerformer{
		httpClient:                    client,
		sqsClient:                     sqsClient,
		queueURL:                      *queueURL,
		httpPath:                      httpPath,
		queueName:                     queueName,
		mimeType:                      mimeType,
		maxRetries:                    maxRetries,
		maxRetriesSQSDeletion:         maxRetriesSQSDeletion,
		maxConcurrentConnections:      maxConcurrentConnections,
		connectionTimeOutSecs:         connectionTimeOutSecs,
		responseTimeoutSecs:           responseTimeoutSecs,
		errorVisibilityTimeoutSeconds: errorVisibilityTimeoutSeconds,
	}, nil
}
