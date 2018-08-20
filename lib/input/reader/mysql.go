package reader

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/mysql"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"

	"github.com/siddontang/go-mysql/canal"
)

//------------------------------------------------------------------------------

// MySQLConfig contains configuration fields for the MySQL input type.
type MySQLConfig struct {
	ConsumerID string   `json:"consumer_id" yaml:"consumer_id"`
	Databases  []string `json:"databases" yaml:"databases"`
	Host       string   `json:"host" yaml:"host"`
	Password   string   `json:"password" yaml:"password"`
	Port       uint32   `json:"port" yaml:"port"`
	Tables     []string `json:"tables" yaml:"tables"`
	Username   string   `json:"username" yaml:"username"`
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
	sync.Mutex
	canal.DummyEventHandler

	canal  *canal.Canal
	store  MySQLStore
	state  MySQLState
	synced bool

	unAckMsgs        []*canal.RowsEvent
	internalMessages chan *canal.RowsEvent
	interruptChan    chan struct{}

	conf  MySQLConfig
	stats metrics.Type
	log   log.Modular
}

// NewMySQL creates a new MySQL input type.
func NewMySQL(conf MySQLConfig, log log.Modular, stats metrics.Type) (*MySQL, error) {
	// create base reader
	m := MySQL{
		internalMessages: make(chan *canal.RowsEvent),
		interruptChan:    make(chan struct{}),
		conf:             conf,
		log:              log.NewModule(".input.mysql"),
		stats:            stats,
	}

	// build binlog consumer config
	c := canal.NewDefaultConfig()
	c.Addr = fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	c.User = conf.Username
	c.Password = conf.Password
	c.Dump.DiscardErr = false
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

// OnPosSynced handles a MySQL binlog position event
func (m *MySQL) OnPosSynced(pos mysql.Position, force bool) error {
	// update state
	m.Lock()
	m.state = MySQLState{
		ConsumerID: m.conf.ConsumerID,
		Log:        pos.Name,
		Position:   pos.Pos,
	}
	m.synced = false
	m.Unlock()

	// sync if force flag is true
	if force == true {
		return m.Acknowledge(nil)
	}

	return nil
}

// OnRow handles a MySQL binlog row event
func (m *MySQL) OnRow(e *canal.RowsEvent) error {
	select {
	case m.internalMessages <- e:
	case <-m.interruptChan:
	}
	return nil
}

//------------------------------------------------------------------------------

// Acknowledge attempts to synchronize the current reader state with the backend
func (m *MySQL) Acknowledge(err error) error {
	if err != nil {
		return nil
	}
	m.Lock()
	defer m.Unlock()
	if m.synced == true {
		return nil
	}
	if err := m.store.Sync(m.state); err != nil {
		return fmt.Errorf("error syncing mysql binlog state: %v", err)
	}
	m.synced = true
	return nil
}

// CloseAsync shuts down the NSQ input and stops processing requests.
func (m *MySQL) CloseAsync() {
	close(m.interruptChan)
}

// Connect retrieves the starting binlog position and establishes a connection
// with MySQL
func (m *MySQL) Connect() error {
	m.Lock()
	defer m.Unlock()
	return errors.New("not implemented")
}

// close attempts to gracefully close the MySQL reader
func (m *MySQL) close() error {
	return errors.New("not implemented")
}

// Read attempts to read a new message from MySQL.
func (m *MySQL) Read() (types.Message, error) {
	return nil, errors.New("not implemented")
}

// WaitForClose blocks until the NSQ input has closed down.
func (m *MySQL) WaitForClose(timeout time.Duration) error {
	return m.close()
}

//------------------------------------------------------------------------------

// MySQLState describes an individual reader's binlog position at a given point in time
type MySQLState struct {
	ConsumerID   string    `db:"consumer_id" dynamodbav:"consumer_id"`
	LastSyncedAt time.Time `db:"synced_at" dynamodbav:"sync_at"`
	Log          string    `db:"log" dynamodbav:"log"`
	Position     uint32    `db:"position" dynamodbav:"position"`
}

// MySQLStore provides access to a MySQL reader state store
type MySQLStore interface {
	// Connect establishes a connection with the store
	Connect() error
	// Get retrieves the last synchronized state
	Get() (MySQLState, error)
	// Sync synchronises the most recent state with the store
	Sync(MySQLState) error
}
