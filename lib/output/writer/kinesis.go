package writer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

//------------------------------------------------------------------------------

// KinesisConfig contains configuration fields for the Kinesis output type.
type KinesisConfig struct {
	StreamName string
}

// NewKinesisConfig creates a new KinesisConfig with default values.
func NewKinesisConfig() KinesisConfig {
	return KinesisConfig{}
}

// Kinesis is a writer that type that writes messages into Kinesis.
type Kinesis struct {
	sync.Mutex

	backoff backoff.BackOff
	client  kinesisiface.KinesisAPI
	stream  *string

	log   log.Modular
	stats metrics.Type
}

//------------------------------------------------------------------------------

// toRecords converts a benthos message into a slice of kinesis records
func (k *Kinesis) toRecords(msg types.Message) ([]*kinesis.PutRecordsRequestEntry, error) {
	return nil, errors.New("not implemented")
}

//------------------------------------------------------------------------------

// Connect attempts to create a create a new Kinesis client and ensures that the
// target stream exists
func (k *Kinesis) Connect() error {
	k.Lock()
	defer k.Unlock()

	if k.client != nil {
		return nil
	}

	return errors.New("not implemented")
}

// Write will attempt to write a message to Kinesis, wait for acknowledgement, and
// returns an error if applicable
func (k *Kinesis) Write(msg types.Message) error {
	records, err := k.toRecords(msg)
	if err != nil {
		k.log.Errorf("error converting message to kinesis records: %v", err)
		return types.ErrBadMessageBytes
	}

	input := kinesis.PutRecordsInput{
		Records:    records,
		StreamName: k.stream,
	}

	var output *kinesis.PutRecordsOutput
	var e error
	k.backoff.Reset()
	for len(input.Records) > 0 {
		// batch write records
		err := backoff.Retry(func() error {
			// batch write to kinesis
			output, e = k.client.PutRecords(&input)
			if e != nil {
				k.log.Debugf("kinesis error: %v\n", err)
			}
			return e
		}, k.backoff)

		// handle kinesis error
		if err != nil {
			k.log.Errorf("kinesis error: %v\n", err)
			return err
		}

		// retry any individual records that failed due to throttling
		failed := []*kinesis.PutRecordsRequestEntry{}
		if output.FailedRecordCount != nil {
			for i, entry := range output.Records {
				if entry.ErrorCode != nil {
					failed = append(failed, input.Records[i])
					if *entry.ErrorCode != kinesis.ErrCodeProvisionedThroughputExceededException && *entry.ErrorCode != kinesis.ErrCodeKMSThrottlingException {
						err = fmt.Errorf("record failed with code [%s] %s: %+v", *entry.ErrorCode, *entry.ErrorMessage, input.Records[i])
						k.log.Warnf("kinesis record error: %v\n", err)
					}
				}
			}
		}

		// if throttling errors detected, pause briefly
		if l := len(failed); l > 0 {
			k.log.Infoln("scheduling retry of failed records")
			time.Sleep(k.backoff.NextBackOff())
		}
		input.Records = failed
	}

	return nil
}

// CloseAsync shuts down the Kinesis writer and stops processing messages.
func (k *Kinesis) CloseAsync() {

}

// WaitForClose blocks until the Kinesis writer has closed down.
func (k *Kinesis) WaitForClose(timeout time.Duration) error {
	return errors.New("not implemented")
}

//------------------------------------------------------------------------------
