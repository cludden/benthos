package reader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/message/metadata"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/gabs"
	"github.com/cenkalti/backoff"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
)

var (
	metaMysqlLogFile     = "mysql_log_file"
	metaMysqlLogPosition = "mysql_log_position"
	metaMysqlServerID    = "mysql_server_id"
)

//------------------------------------------------------------------------------

// MySQLConfig contains configuration fields for the MySQL input type.
type MySQLConfig struct {
	// Specifies the number of row events to include in a single message. The
	// default BatchSize is 1.
	BatchSize int `json:"batch_size" yaml:"batch_size"`
	// Specifies the buffering window duration when using a BatchSize greater
	// than 1. Default is "1s".
	BufferTimeout string `json:"buffer_timeout" yaml:"buffer_timeout"`
	// Specifies the name of the cache resource used for storing consumer state.
	Cache string `json:"cache" yaml:"cache"`
	// Specifies the unique mysql server id.
	ConsumerID uint32 `json:"consumer_id" yaml:"consumer_id"`
	// Specifies a list of databases to subscribe to.
	Databases []string `json:"databases" yaml:"databases"`
	// Specifies the mysql host name.
	Host string `json:"host" yaml:"host"`
	// An optional cache prefix that can be used when sharing a single cache
	// resource.
	KeyPrefix string `json:"key_prefix" yaml:"key_prefix"`
	// Boolean flag that allows the consumer to fall back to using the latest
	// binlog position when no previous offset is available.
	Latest bool `json:"latest" yaml:"latest"`
	// Path to the mysqldump executable.This field is required if the desired
	// starting position is `dump`
	MySQLDumpPath string `json:"mysqldump_path" yaml:"mysqldump_path"`
	// MySQL user credentials
	Password string `json:"password" yaml:"password"`
	// Specifies the number of row events to buffer to improve read performance.
	PrefetchCount uint `json:"prefetch_count" yaml:"prefetch_count"`
	// MySQL port
	Port uint32 `json:"port" yaml:"port"`
	// Optional duration string used to throttle how often the consumer offsets
	// are written to the cache.
	SyncInterval string `json:"sync_interval" yaml:"sync_interval"`
	// An optional table whitelist. This field is only honored when subscribed
	// to a single database.
	Tables []string `json:"tables" yaml:"tables"`
	// MySQL user name
	Username string `json:"username" yaml:"username"`
}

// NewMySQLConfig creates a new MySQLConfig with default values
func NewMySQLConfig() MySQLConfig {
	return MySQLConfig{
		Host: "localhost",
		Port: 3306,
	}
}

//------------------------------------------------------------------------------

// MySQL is an input type that reads from a MySQL binary log stream.
type MySQL struct {
	sync.RWMutex
	canal.DummyEventHandler

	ack              chan mysql.Position
	backoff          backoff.BackOff
	batchSize        int
	bufferTimer      *time.Timer
	bufferTimeout    time.Duration
	canal            *canal.Canal
	closed           chan error
	internalMessages chan *message.Part
	interruptChan    chan struct{}
	key              string
	lastPosition     mysql.Position
	syncInterval     time.Duration
	unacked          []*message.Part

	conf  MySQLConfig
	cache types.Cache
	stats metrics.Type
	log   log.Modular
}

// NewMySQL creates a new MySQL input type.
func NewMySQL(conf MySQLConfig, cache types.Cache, log log.Modular, stats metrics.Type) (*MySQL, error) {
	// create base reader
	m := MySQL{
		ack:              make(chan mysql.Position),
		key:              fmt.Sprintf("%s%d", conf.KeyPrefix, conf.ConsumerID),
		interruptChan:    make(chan struct{}),
		closed:           make(chan error, 2),
		conf:             conf,
		cache:            cache,
		internalMessages: make(chan *message.Part, conf.PrefetchCount),
		stats:            stats,
		log:              log.NewModule(".input.mysql"),
	}

	// create sync backoff config
	b := backoff.NewExponentialBackOff()
	m.backoff = backoff.WithMaxRetries(b, 4)

	// set batch size (using 1 as default)
	batchSize := conf.BatchSize
	if batchSize == 0 {
		batchSize = 1
	}
	m.batchSize = batchSize

	// set buffering window (using 1s as default)
	dur := conf.BufferTimeout
	if dur == "" {
		dur = "1s"
	}
	timeout, err := time.ParseDuration(dur)
	if err != nil {
		return nil, err
	}
	m.bufferTimeout = timeout
	m.bufferTimer = time.NewTimer(timeout)

	// set consumer position sync interval
	var syncInterval time.Duration
	if conf.SyncInterval != "" {
		syncInterval, err = time.ParseDuration(conf.SyncInterval)
		if err != nil {
			return nil, err
		}
	} else {
		syncInterval = time.Second * 30
	}
	m.syncInterval = syncInterval

	// build binlog consumer config
	c := canal.NewDefaultConfig()
	c.Addr = fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	c.User = conf.Username
	c.Password = conf.Password
	c.Dump.DiscardErr = false
	c.Dump.ExecutionPath = conf.MySQLDumpPath
	c.Dump.SkipMasterData = false
	if len(conf.Databases) == 1 && len(conf.Tables) > 0 {
		c.Dump.TableDB = conf.Databases[0]
		c.Dump.Tables = conf.Tables
	} else {
		c.Dump.Databases = conf.Databases
	}

	// create binlog consumer client
	client, err := canal.NewCanal(c)
	if err != nil {
		return nil, fmt.Errorf("error creating mysql binlog client: %v", err)
	}
	client.SetEventHandler(&m)
	m.canal = client

	return &m, nil
}

//------------------------------------------------------------------------------

// loadPosition computes the consumer's starting position
func (m *MySQL) loadPosition() (*mysqlPosition, error) {
	var pos mysqlPosition

	// if mysqldump indicated, exit early
	if m.conf.MySQLDumpPath != "" {
		return nil, nil
	}

	// attempt to load position from cache
	state, err := m.cache.Get(m.key)
	if err != nil {
		m.log.Warnf("error retrieving last synchronized mysql position: %v", err)
	} else if err = json.Unmarshal(state, &pos); err != nil {
		m.log.Warnf("error unmarshalling last synchronized mysql position: %v", err)
	}

	if err != nil {
		if !m.conf.Latest {
			return nil, fmt.Errorf("error loading mysql position: %v", err)
		}
		p, err := m.canal.GetMasterPos()
		if err != nil {
			return nil, fmt.Errorf("error retrieving latest mysql position: %v", err)
		}
		pos.ConsumerID = m.conf.ConsumerID
		pos.Log = p.Name
		pos.Position = p.Pos
	}

	return &pos, nil
}

// marshalKeys computes a map of primary key columns to values
func (m *MySQL) marshalKeys(e *canal.RowsEvent, summary *MySQLRowSummary) (map[string]interface{}, error) {
	// grab a reference to appropriate source image
	var src []byte
	switch e.Action {
	case canal.InsertAction:
		src = summary.After
	case canal.UpdateAction:
		src = summary.After
	case canal.DeleteAction:
		src = summary.Before
	}
	image, err := gabs.ParseJSON(src)
	if err != nil {
		return nil, err
	}

	keys := make(map[string]interface{})
	for i := range e.Table.PKColumns {
		col := e.Table.GetPKColumn(i)
		keys[col.Name] = image.S(col.Name).Data()
	}
	return keys, nil
}

// marshalRowSummary converts a row image to json
func (m *MySQL) marshalRowSummary(table *schema.Table, row []interface{}) []byte {
	result := gabs.New()
	for i, c := range table.Columns {
		result.Set(m.parseValue(&c, row[i]), c.Name)
	}
	return result.Bytes()
}

// OnPosSynced handles a MySQL binlog position event
func (m *MySQL) OnPosSynced(pos mysql.Position, force bool) error {
	m.lastPosition = pos
	return nil
}

// OnRow handles a MySQL binlog row event
func (m *MySQL) OnRow(e *canal.RowsEvent) error {
	part, err := m.toPart(e, m.lastPosition.Name)
	if err != nil {
		m.log.Errorf("error parsing mysql row event: %v", err)
		close(m.internalMessages)
		return err
	}

	select {
	case m.internalMessages <- part:
	case <-m.interruptChan:
	}

	return nil
}

// parse converts a mysql row event into a MysqlMessage
func (m *MySQL) parse(e *canal.RowsEvent, log string) (*MysqlMessage, error) {
	msg := MysqlMessage{
		Row:       m.parseRowSummary(e),
		Schema:    e.Table.Schema,
		Table:     e.Table.Name,
		Timestamp: time.Unix(int64(e.Header.Timestamp), 0),
		Type:      e.Action,
	}

	keys, err := m.marshalKeys(e, &msg.Row)
	if err != nil {
		return nil, err
	}
	msg.Key = keys

	var id bytes.Buffer
	fmt.Fprintf(&id, "%s:%d:%s:%s:", log, e.Header.LogPos, msg.Schema, msg.Table)
	for _, v := range keys {
		fmt.Fprintf(&id, "%v:", v)
	}
	msg.ID = strings.TrimSuffix(id.String(), ":")

	return &msg, nil
}

// parseRowSummary parses the before and/or after row image
func (m *MySQL) parseRowSummary(e *canal.RowsEvent) MySQLRowSummary {
	var summary MySQLRowSummary
	switch e.Action {
	case canal.UpdateAction:
		summary.Before = m.marshalRowSummary(e.Table, e.Rows[0])
		summary.After = m.marshalRowSummary(e.Table, e.Rows[1])
	case canal.InsertAction:
		summary.After = m.marshalRowSummary(e.Table, e.Rows[0])
	case canal.DeleteAction:
		summary.Before = m.marshalRowSummary(e.Table, e.Rows[0])
	}
	return summary
}

// parseValue value using table column definition
// borrowed from https://github.com/siddontang/go-mysql-elasticsearch/blob/master/river/sync.go#L261
func (m *MySQL) parseValue(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				m.log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}
			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME:
		switch v := value.(type) {
		case string:
			vt, _ := time.ParseInLocation(mysql.TimeFormat, string(v), time.UTC)
			return vt.Format(time.RFC3339Nano)
		}
	}
	return value
}

// periodicSync maintains the current consumer position in memory, periodically
// synching the latest acked position with the cache
func (m *MySQL) periodicSync(consumerID uint32, syncInterval time.Duration) {
	var unsynced time.Time
	position := mysqlPosition{
		ConsumerID: consumerID,
	}
	defer func() {
		position.LastSyncedAt = time.Now()
		m.closed <- m.sync(position)
	}()

	sync := time.NewTicker(syncInterval)
	defer sync.Stop()

	for {
		select {
		// keep track of unsynced messages
		case p, ok := <-m.ack:
			if !ok {
				return
			}
			position.Log = p.Name
			position.Position = p.Pos
			position.LastSyncedAt = time.Now()

		// periodically acknowledge messages by persisting consumer offsets
		case <-sync.C:
			m.sync(position)
			position.LastSyncedAt = unsynced
		}
	}
}

// sync the consumer position with the cache
func (m *MySQL) sync(position mysqlPosition) error {
	// continue if there is nothing new to acknowledge
	if position.Log == "" || position.LastSyncedAt.IsZero() {
		return nil
	}

	// update sync time and marshal position
	position.LastSyncedAt = time.Now()
	p, err := json.Marshal(&position)
	if err != nil {
		m.log.Errorf("error marshalling consumer position: %v", err)
		return err
	}

	m.backoff.Reset()
	if err := backoff.Retry(func() error {
		return m.cache.Set(m.key, p)
	}, m.backoff); err != nil {
		m.log.Errorf("error persisting consumer position: %v", err)
		return err
	}

	m.log.Infof("synced consumer %d position: %s:%d\n", position.ConsumerID, position.Log, position.Position)
	return nil
}

// toPart parses a mysql row event and converts it into a benthos message part
func (m *MySQL) toPart(e *canal.RowsEvent, log string) (*message.Part, error) {
	record, err := m.parse(e, log)
	if err != nil {
		return nil, types.ErrBadMessageBytes
	}

	var part message.Part
	if err := part.SetJSON(record); err != nil {
		return nil, err
	}
	part.SetMetadata(metadata.New(map[string]string{
		metaMysqlLogFile:     log,
		metaMysqlLogPosition: strconv.FormatInt(int64(e.Header.LogPos), 10),
		metaMysqlServerID:    strconv.FormatInt(int64(m.conf.ConsumerID), 10),
	}))
	return &part, nil
}

//------------------------------------------------------------------------------

// Acknowledge attempts to synchronize the current reader state with the backend
func (m *MySQL) Acknowledge(err error) error {
	if err != nil {
		return nil
	}

	l := len(m.unacked)
	if l == 0 {
		return nil
	}

	// compute furthest read position using last unacked part metadata
	var pos mysql.Position
	last := m.unacked[l-1]
	meta := last.Metadata()
	pos.Name = meta.Get(metaMysqlLogFile)
	offset, _ := strconv.ParseInt(meta.Get("mysql_log_position"), 10, 64)
	pos.Pos = uint32(offset)

	// send position and clear unacked
	m.ack <- pos
	m.unacked = nil

	return nil
}

// CloseAsync shuts down the MySQL input and stops processing requests.
func (m *MySQL) CloseAsync() {
	go func() {
		close(m.interruptChan)
		m.canal.Close()
	}()
}

// Connect retrieves the starting binlog position and begins streaming
// change data capture events from mysql
func (m *MySQL) Connect() error {
	m.Lock()
	defer m.Unlock()

	// load starting position
	pos, err := m.loadPosition()
	if err != nil {
		return fmt.Errorf("unable to load mysql binlog position: %v", err)
	}

	// start the binlog consumer
	var start func(c *canal.Canal) error
	if pos == nil {
		start = func(c *canal.Canal) error {
			return c.Run()
		}
	} else {
		start = func(c *canal.Canal) error {
			err := c.RunFrom(mysql.Position{
				Name: pos.Log,
				Pos:  pos.Position,
			})
			if err != nil && strings.Contains(err.Error(), "ERROR 1236 ") && m.conf.Latest {
				return c.Run()
			}
			return err
		}
	}

	go func() {
		m.closed <- start(m.canal)
		close(m.ack)
		close(m.internalMessages)
	}()
	go m.periodicSync(m.conf.ConsumerID, m.syncInterval)

	return nil
}

// Read attempts to read a new message from MySQL.
func (m *MySQL) Read() (types.Message, error) {
	var n int
	msg := message.New(nil)

	// requeue any unackknowledged message parts
	if l := len(m.unacked); l > 0 {
		for i := 0; i < l && n < m.batchSize; i++ {
			msg.Append(m.unacked[i])
			n++
		}
	}

	// if message empty, block until at least one part has been read
	if n == 0 {
		select {
		case part, ok := <-m.internalMessages:
			if !ok {
				return nil, types.ErrTypeClosed
			}
			m.unacked = append(m.unacked, part)
			msg.Append(part)
			n++
		case <-m.interruptChan:
			return nil, types.ErrTypeClosed
		}
	}

	// if message is full, return it
	if n == m.batchSize {
		return msg, nil
	}

	// reset timer
	m.bufferTimer.Reset(m.bufferTimeout)
	defer m.bufferTimer.Stop()

	// continue to add parts from buffer until batch is full or buffer
	// timeout reached
	for n < m.batchSize {
		select {
		case part, ok := <-m.internalMessages:
			if !ok {
				return nil, types.ErrTypeClosed
			}
			m.unacked = append(m.unacked, part)
			msg.Append(part)
			n++
		case <-m.bufferTimer.C:
			break
		case <-m.interruptChan:
			return nil, types.ErrTypeClosed
		}
	}

	return msg, nil
}

// WaitForClose blocks until the MySQL input has closed down.
func (m *MySQL) WaitForClose(timeout time.Duration) error {
	var err error
	for i := 0; i < 2; i++ {
		err = <-m.closed
		if err != nil && err.Error() == context.Canceled.Error() {
			err = nil
		}
	}
	return err
}

//------------------------------------------------------------------------------

// MysqlMessage represents a single mysql binlog row event
type MysqlMessage struct {
	ID        string                 `json:"id"`
	Key       map[string]interface{} `json:"key"`
	Row       MySQLRowSummary        `json:"row"`
	Schema    string                 `json:"schema"`
	Table     string                 `json:"table"`
	Timestamp time.Time              `json:"timestamp"`
	Type      string                 `json:"type"`
}

// MySQLRowSummary contains the before and after row images of a single
// binlog row event
type MySQLRowSummary struct {
	After  json.RawMessage `json:"after"`
	Before json.RawMessage `json:"before"`
}

// mysqlPosition describes an individual reader's binlog position at a given point in time
type mysqlPosition struct {
	ConsumerID   uint32    `db:"consumer_id" dynamodbav:"consumer_id"`
	LastSyncedAt time.Time `db:"synced_at" dynamodbav:"sync_at"`
	Log          string    `db:"log" dynamodbav:"log"`
	Position     uint32    `db:"position" dynamodbav:"position"`
}
