# sqsd

[![Go](https://github.com/ChandraNarreddy/sqsd/actions/workflows/go.yml/badge.svg)](https://github.com/ChandraNarreddy/sqsd/actions/workflows/go.yml)

SQSD is an open source implementation of AWS' SQS Daemon used in their Elastic BeanStalk worker tier environments.

## Salient Features -

* Employs a task queue model backed by go's channel-goroutines concurrency mechanism. Task queue's buffer and number of goroutines are both configurable.
* Upstream worker endpoint can be any legal URL as opposed to a locally bound server endpoint.
* 


## How to run - 

* Ensure that AWS credentials are available as environment variables or as ec2 role credentials.
* sqsd requires the name of the sqs queue and the URL address to the workers as mandatory parameters.
```bash
$ sqsd -sqs zensqsd_test -forward http://localhost:8080/
```
#### Options

| **Argument**  |    **Default**     | **Required** |                                                   **Description**                                                    |
|---|--------------|------------------------|----------------------------------------------------------------------------------------------------------------------|
|  -sqs | None                              | yes           | SQS queue Name                                                                                                  |
| -region  | `us-west-2`                          | no                 | AWS region of the SQS Queue                                                                                       |
|  -vis | `60` | no           | SQS visibility timeout in seconds. This timeout will be set in the ReceiveMessage call to SQS. Default 60*1                                                                                 |
| -errVis  | `300`                                 | no          | Time in seconds to wait before a message is made visible in the queue after an attempt to process it fails with an error. Default is 300                                                                                                      |
| -maxPoll  | `10`                                 | no          | Max messages to fetch at once; 10 by default                                                                                                      |
| -forward  | None                                 | no          | path to forward the request, must be a valid URL.                                                                                                      |
| -mime  | `application/json`                                 | no          | mime type for the request; defaults to application/json.                                                                                                      |
|  -maxRetries | `10`                  | no           | max retries; default 10.                                                                      |
|  -maxRetriesSQSDeletion | `1`                            | no           | max retries for deletion of messages from SQS; default 1                                   |
|  -maxConcurrentConns | `50`                             | no           | max concurrent connections to the upstream                                                  |
|  -connTimeout | `5`                         | no           | maximum connection timeout in seconds; default 5 seconds.                                                                      |
| -responseTimeout | `120`                           | no          | maximum time to wait for response in seconds; default 120 seconds.                                                                                     |
| -bufferSize | `100`          | no           | Internal task queue's buffer size to hold incoming SQS messages; default 100.                                                                                                   |
| -workersCount  | `20`                        | no           | Number of concurrent workers to spawn; default 20                                                                  |
|  -logLevel | `0`                            | no           | logging level. Pass -4 for DEBUG, 0 for INFO, 4 for WARN, 8 for ERROR; default 0 - INFO.                                                                  |

## Docker version -

You can use docker images published here to invoke SQSD like so - 
```bash
$ docker run -it --entrypoint /sqsd.cmd sqsd:latest -sqs <your_sqsq_name> -forward <your_upstream_worker_url>
```


