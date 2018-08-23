package reader

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/cache"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/manager"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/gabs"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
)

func TestMySQLIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	// start mysql container with binlog enabled
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "5.7",
		Cmd: []string{
			"--server-id=1234",
			"--log-bin=/var/lib/mysql/mysql-bin.log",
			"--binlog-do-db=test",
			"--binlog-format=ROW",
		},
		Env: []string{
			"MYSQL_DATABASE=test",
			"MYSQL_ROOT_PASSWORD=s3cr3t",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"3306": []docker.PortBinding{
				docker.PortBinding{
					HostPort: "3306",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %v", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	port, err := strconv.ParseInt(resource.GetPort("3306/tcp"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	// bootstrap mysql
	var db *sql.DB
	if err := pool.Retry(func() error {
		db, err = sql.Open("mysql", fmt.Sprintf("root:s3cr3t@tcp(localhost:%d)/test", port))
		if err == nil {
			// create test table
			_, err := db.Exec("CREATE TABLE `test`.`foo` (id integer AUTO_INCREMENT, created_at datetime(6), title varchar(255), enabled tinyint(1), PRIMARY KEY (id))")
			if err != nil {
				db.Close()
				return err
			}
		}
		return err
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}
	defer db.Close()

	config := NewMySQLConfig()
	config.Cache = "memory"
	config.ConsumerID = 1234
	config.Databases = []string{"test"}
	config.Latest = true
	config.Password = "s3cr3t"
	config.Port = uint32(port)
	config.Username = "root"

	t.Run("testMySQLConnect", func(t *testing.T) {
		testMySQLConnect(t, db, config)
	})
}

func testMySQLConnect(t *testing.T, db *sql.DB, config MySQLConfig) {
	met := metrics.DudType{}
	log := log.New(os.Stdout, log.Config{LogLevel: "DEBUG"})
	mgr, err := manager.New(manager.NewConfig(), nil, log, met)
	if err != nil {
		t.Fatal(err)
	}

	c, err := cache.NewMemory(cache.NewConfig(), mgr, log, metrics.DudType{})

	r, err := NewMySQL(config, c, log, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.CloseAsync()
		if err := r.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	// define mysql fixtures
	type fooRow struct {
		createdAt time.Time
		title     string
		enabled   int
	}
	rows := []fooRow{
		fooRow{time.Now(), "foo", 1},
		fooRow{time.Now(), "bar", 0},
		fooRow{time.Now(), "baz", 1},
	}
	stmt := "INSERT INTO `test`.`foo` (created_at, title, enabled) VALUES (?, ?, ?)"
	for _, row := range rows {
		_, err = db.Exec(stmt, row.createdAt, row.title, row.enabled)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i, row := range rows {
		msg, err := r.Read()
		if err != nil {
			t.Error(err)
		} else {
			if length := msg.Len(); length != 1 {
				t.Errorf("Expected message %d to have length of %d, got %d", i, 1, length)
			}
			part := msg.Get(0)
			data, err := part.JSON()
			if err != nil {
				t.Errorf("Unexpected error retrieving part JSON: %v", err)
			}
			record, _ := data.(*MysqlMessage)
			if schema := record.Schema; schema != "test" {
				t.Errorf("Expected message %d to have schema %s, got %s", i, "test", schema)
			}
			if table := record.Table; table != "foo" {
				t.Errorf("Expected message %d to have table %s, got %s", i, "foo", table)
			}
			if typ := record.Type; typ != "insert" {
				t.Errorf("Expected message %d to have type %s, got %s", i, "insert", typ)
			}
			if ts := record.Timestamp; time.Now().Sub(ts) > time.Second*5 {
				t.Errorf("Expected message %d to have timestamp within the last five seconds", i)
			}
			ids := strings.Split(record.ID, ":")
			if l := len(ids); l != 5 {
				t.Errorf("Expected message %d id to contain 5 elements, got %d", i, l)
			} else if ids[4] != fmt.Sprintf("%d", i+1) {
				t.Errorf("Expected message %d id to have primary key %d, got %s", i, i+1, ids[4])
			}
			if l := len(record.Row.Before); l != 0 {
				t.Errorf("Expected message %d to have empty before image", i)
			}
			image, _ := gabs.ParseJSON(part.Get())
			if id := image.S("row", "after", "id").String(); id != fmt.Sprintf("%d", i+1) {
				t.Errorf("Expected message %d to have id of %d, got %s", i, i+1, id)
			}
			createdAt, err := time.Parse(time.RFC3339Nano, image.S("row", "after", "created_at").String())
			if err != nil {
				t.Errorf("Failed to parse message %d created_at timestamp: %v", i, err)
			} else if !createdAt.Equal(row.createdAt) {
				t.Errorf("Expected message %d to have created_at of %s, got %s", i, row.createdAt, createdAt)
			}
			if title := image.S("row", "after", "title").String(); title != row.title {
				t.Errorf("Expected message %d to have title %s, got %s", i, row.title, title)
			}
			if enabled := image.S("row", "after", "title").String(); enabled != fmt.Sprintf("%d", row.enabled) {
				t.Errorf("Expected message %d to have enabled %d, got %s", i, row.enabled, enabled)
			}
		}
		if err := r.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	var pos mysqlPosition
	val, err := c.Get(r.key)
	if err != nil {
		t.Error(err)
	}
	if err := json.Unmarshal(val, &pos); err != nil {
		t.Error(err)
	}
	if pos.ConsumerID != config.ConsumerID {
		t.Errorf("Expected cache position to have id of %d, got %d", config.ConsumerID, pos.ConsumerID)
	}
	if len(pos.Log) == 0 {
		t.Error("Expected cache position to have log name")
	}
	if pos.Position == 0 {
		t.Error("Expected cache position to have non-zero log position")
	}
	if time.Now().Sub(pos.LastSyncedAt) > time.Second {
		t.Error("Expected cache position to have been synced within the last econd")
	}
}
