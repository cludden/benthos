// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package writer

import (
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	sess "github.com/Jeffail/benthos/lib/util/aws/session"
	"github.com/Jeffail/benthos/lib/util/retries"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/cenkalti/backoff"
)

//------------------------------------------------------------------------------

// KinesisFirehoseConfig contains configuration fields for the Kinesis output type.
type KinesisFirehoseConfig struct {
	sessionConfig  `json:",inline" yaml:",inline"`
	DeliveryStream string `json:"delivery_stream" yaml:"delivery_stream"`
	retries.Config `json:",inline" yaml:",inline"`
}

// NewKinesisFirehoseConfig creates a new Config with default values.
func NewKinesisFirehoseConfig() KinesisConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"
	return KinesisConfig{
		sessionConfig: sessionConfig{
			Config: sess.NewConfig(),
		},
		Stream:       "",
		HashKey:      "",
		PartitionKey: "",
		Config:       rConf,
	}
}

//------------------------------------------------------------------------------

// KinesisFirehose is a benthos writer.Type implementation that writes messages to an
// Amazon Kinesis Firehose delivery stream.
type KinesisFirehose struct {
	conf KinesisFirehoseConfig

	session  *session.Session
	firehose firehoseiface.FirehoseAPI

	backoff            backoff.BackOff
	endpoint           *string
	deliveryStreamName *string

	log   log.Modular
	stats metrics.Type

	mThrottled       metrics.StatCounter
	mThrottledF      metrics.StatCounter
	mPartsThrottled  metrics.StatCounter
	mPartsThrottledF metrics.StatCounter
}

// NewKinesisFirehose creates a new Amazon Kinesis writer.Type.
func NewKinesisFirehose(
	conf KinesisFirehoseConfig,
	log log.Modular,
	stats metrics.Type,
) (*KinesisFirehose, error) {
	k := KinesisFirehose{
		conf:               conf,
		log:                log,
		stats:              stats,
		mPartsThrottled:    stats.GetCounter("parts.send.throttled"),
		mThrottled:         stats.GetCounter("send.throttled"),
		deliveryStreamName: aws.String(conf.DeliveryStream),
	}

	var err error
	if k.backoff, err = conf.Config.Get(); err != nil {
		return nil, err
	}
	return &k, nil
}

//------------------------------------------------------------------------------

// toRecords converts an individual benthos message into a slice of Kinesis
// batch put entries by promoting each message part into a single part message
// and passing each new message through the partition and hash key interpolation
// process, allowing the user to define the partition and hash key per message
// part.
func (a *KinesisFirehose) toRecords(msg types.Message) ([]*firehose.Record, error) {
	entries := make([]*firehose.Record, msg.Len())

	err := msg.Iter(func(i int, p types.Part) error {
		entry := firehose.Record{
			Data: p.Get(),
		}

		if len(entry.Data) > mebibyte {
			a.log.Errorf("part %d exceeds the maximum Kinesis payload limit of 1 MiB\n", i)
			return types.ErrMessageTooLarge
		}

		entries[i] = &entry
		return nil
	})

	return entries, err
}

//------------------------------------------------------------------------------

// Connect creates a new Kinesis client and ensures that the target Kinesis
// stream exists.
func (a *KinesisFirehose) Connect() error {
	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession()
	if err != nil {
		return err
	}

	a.session = sess
	a.firehose = firehose.New(sess)

	a.log.Infof("Sending messages to Kinesis Firehose delivery stream: %v\n", a.conf.DeliveryStream)
	return nil
}

// Write attempts to write message contents to a target Kinesis stream in batches of 500.
// If throttling is detected, failed messages are retried according to the configurable
// backoff settings.
func (a *KinesisFirehose) Write(msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	records, err := a.toRecords(msg)
	if err != nil {
		return err
	}

	input := &firehose.PutRecordBatchInput{
		Records:            records,
		DeliveryStreamName: a.deliveryStreamName,
	}

	// trim input record length to max kinesis batch size
	if len(records) > kinesisMaxRecordsCount {
		input.Records, records = records[:kinesisMaxRecordsCount], records[kinesisMaxRecordsCount:]
	} else {
		records = nil
	}

	var failed []*firehose.Record
	a.backoff.Reset()
	for len(input.Records) > 0 {
		wait := a.backoff.NextBackOff()

		// batch write to kinesis
		output, err := a.firehose.PutRecordBatch(input)
		if err != nil {
			a.log.Warnf("kinesis firehose error: %v\n", err)
			// bail if a message is too large or all retry attempts expired
			if wait == backoff.Stop {
				return err
			}
			continue
		}

		// requeue any individual records that failed due to throttling
		failed = nil
		if output.FailedPutCount != nil {
			for i, entry := range output.RequestResponses {
				if entry.ErrorCode != nil {
					failed = append(failed, input.Records[i])
				}
			}
		}
		input.Records = failed

		// if throttling errors detected, pause briefly
		l := len(failed)
		if l > 0 {
			a.mThrottled.Incr(1)
			a.mPartsThrottled.Incr(int64(l))
			a.log.Warnf("scheduling retry of failed records (%d)\n", l)
			if wait == backoff.Stop {
				return types.ErrTimeout
			}
			time.Sleep(wait)
		}

		// add remaining records to batch
		if n := len(records); n > 0 && l < kinesisMaxRecordsCount {
			if remaining := kinesisMaxRecordsCount - l; remaining < n {
				input.Records, records = append(input.Records, records[:remaining]...), records[remaining:]
			} else {
				input.Records, records = append(input.Records, records...), nil
			}
		}
	}
	return err
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *KinesisFirehose) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *KinesisFirehose) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
