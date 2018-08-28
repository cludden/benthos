package writer

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cenkalti/backoff"
)

type mockKinesis struct {
	kinesisiface.KinesisAPI
	fn func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

func (m *mockKinesis) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	return m.fn(input)
}

func TestKinesisWriteSinglePartMessage(t *testing.T) {
	k := Kinesis{
		backoff: backoff.NewExponentialBackOff(),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				if exp, act := 1, len(input.Records); exp != act {
					return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
				}
				if exp, act := "123", input.Records[0].PartitionKey; exp != *act {
					return nil, fmt.Errorf("expected record to have partition key %s, got %s", exp, *act)
				}
				return &kinesis.PutRecordsOutput{}, nil
			},
		},
		log:          log.Noop(),
		partitionKey: text.NewInterpolatedString("${!json_field:id}"),
		hashKey:      text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	part := message.NewPart([]byte(`{"foo":"bar","id":123}`))
	msg.Append(part)

	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
}

func TestKinesisWriteMultiPartMessage(t *testing.T) {
	parts := []struct {
		data []byte
		key  string
	}{
		{[]byte(`{"foo":"bar","id":123}`), "123"},
		{[]byte(`{"foo":"baz","id":456}`), "456"},
	}
	k := Kinesis{
		backoff: backoff.NewExponentialBackOff(),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				if exp, act := len(parts), len(input.Records); exp != act {
					return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
				}
				for i, p := range parts {
					if exp, act := p.key, input.Records[i].PartitionKey; exp != *act {
						return nil, fmt.Errorf("expected record %d to have partition key %s, got %s", i, exp, *act)
					}
				}
				return &kinesis.PutRecordsOutput{}, nil
			},
		},
		log:          log.Noop(),
		partitionKey: text.NewInterpolatedString("${!json_field:id}"),
		hashKey:      text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	for _, p := range parts {
		part := message.NewPart(p.data)
		msg.Append(part)
	}

	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
}

func TestKinesisWriteError(t *testing.T) {
	var calls int
	k := Kinesis{
		backoff: backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 2),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				calls++
				return nil, errors.New("blah")
			},
		},
		log:          log.Noop(),
		partitionKey: text.NewInterpolatedString("${!json_field:id}"),
		hashKey:      text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	msg.Append(message.NewPart([]byte(`{"foo":"bar"}`)))

	if exp, err := "blah", k.Write(msg); err.Error() != exp {
		t.Errorf("Expected err to equal %s, got %v", exp, err)
	}
	if exp, act := 3, calls; act != exp {
		t.Errorf("Expected kinesis.PutRecords to have call count %d, got %d", exp, act)
	}
}

func TestKinesisWritePayloadExceeded(t *testing.T) {
	k := Kinesis{
		backoff: backoff.NewExponentialBackOff(),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				if exp, act := 1, len(input.Records); exp != act {
					return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
				}
				details := `ValidationException: 1 validation error detected: Value
				'java.nio.HeapByteBuffer[pos=0 lim=1346166 cap=1346166]' at 'records.1.member.data'
				failed to satisfy constraint: Member must have length less than or equal to
				1048576\n\tstatus code: 400, request id: ac0b32349-0e27-4347-a548-4d1c603b7fff`
				return nil, awserr.New(kinesis.ErrCodeInvalidArgumentException, details, errors.New("ValidationException"))
			},
		},
		log:          log.Noop(),
		partitionKey: text.NewInterpolatedString("${!json_field:id}"),
		hashKey:      text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	msg.Append(message.NewPart([]byte(`{"foo":"bar"}`)))

	if exp, act := types.ErrMessageTooLarge, k.Write(msg); act != exp {
		t.Errorf("expected err to equal %v, got %v", exp, act)
	}
}

func TestKinesisWriteMessageThrottling(t *testing.T) {
	var calls [][]*kinesis.PutRecordsRequestEntry
	k := Kinesis{
		backoff: backoff.NewExponentialBackOff(),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				records := make([]*kinesis.PutRecordsRequestEntry, len(input.Records))
				copy(records, input.Records)
				calls = append(calls, records)
				var failed int64
				var output kinesis.PutRecordsOutput
				for i := 0; i < len(input.Records); i++ {
					entry := kinesis.PutRecordsResultEntry{}
					if i > 0 {
						failed++
						entry.SetErrorCode(kinesis.ErrCodeProvisionedThroughputExceededException)
					}
					output.SetFailedRecordCount(failed)
					output.Records = append(output.Records, &entry)
				}
				return &output, nil
			},
		},
		log:          log.Noop(),
		partitionKey: text.NewInterpolatedString("${!json_field:id}"),
		hashKey:      text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	msg.Append(message.NewPart([]byte(`{"foo":"bar","id":123}`)))
	msg.Append(message.NewPart([]byte(`{"foo":"baz","id":456}`)))
	msg.Append(message.NewPart([]byte(`{"foo":"qux","id":789}`)))

	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
	if exp, act := msg.Len(), len(calls); act != exp {
		t.Errorf("Expected kinesis.PutRecords to have call count %d, got %d", exp, calls)
	}
	for i, c := range calls {
		if exp, act := msg.Len()-i, len(c); act != exp {
			t.Errorf("Expected kinesis.PutRecords call %d input to have Records with length %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisWriteBackoffMaxRetriesExceeded(t *testing.T) {
	var calls int
	k := Kinesis{
		backoff: backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 2),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				calls++
				var output kinesis.PutRecordsOutput
				output.FailedRecordCount = aws.Int64(1)
				output.Records = append(output.Records, &kinesis.PutRecordsResultEntry{
					ErrorCode: aws.String(kinesis.ErrCodeProvisionedThroughputExceededException),
				})
				return &output, nil
			},
		},
		log:          log.Noop(),
		partitionKey: text.NewInterpolatedString("${!json_field:id}"),
		hashKey:      text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	msg.Append(message.NewPart([]byte(`{"foo":"bar","id":123}`)))

	if err := k.Write(msg); err == nil {
		t.Error(errors.New("Expected kinesis.Write to error"))
	}
	if exp := 3; calls != exp {
		t.Errorf("Expected kinesis.PutRecords to have call count %d, got %d", exp, calls)
	}
}
