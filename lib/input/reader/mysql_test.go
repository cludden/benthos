package reader

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/docker"

	"github.com/ory/dockertest"
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
			"--binlog-do-b=test",
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

	// create test table
	var db *sql.DB
	if err := pool.Retry(func() error {
		db, err = sql.Open("mysql", fmt.Sprintf("root:password@tcp(localhost:%s)/test", resource.GetPort("3306")))
		if err == nil {
			_, err := db.Exec("CREATE TABLE `test`.`foo` (id integer AUTO_INCREMENT, created_at datetime(6), title varchar(255), enabled tinyint(1))")
			if err != nil {
				t.Logf("Could not create mysql table: %v", err)
				db.Close()
				return err
			}
		}
		return err
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}
	defer db.Close()

	t.Run("Test")
}
