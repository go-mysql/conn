package conn_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	myconn "github.com/go-mysql/conn"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-test/deep"
)

// default_connection.json in sandbox dir
type connInfo struct {
	Host   string `json:"host"`
	Port   string `json:"port"`
	Socket string `json:"socket"`
	User   string `json:"username"`
	Pass   string `json:"password"`
}

var (
	sandboxDir  string
	defaultDSN  string
	defaultPort string
)

// Runs a script in sandbox dir
func sandboxAction(t *testing.T, action string) {
	cmd := exec.Command(filepath.Join(sandboxDir, action))
	t.Logf("%s sandbox", action)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Log(out)
		t.Fatal(err)
	}
}

func setup(t *testing.T) {
	if sandboxDir == "" || defaultDSN == "" {
		sandboxDir = os.Getenv("MYSQL_SANDBOX_DIR")
		if sandboxDir == "" {
			t.Fatal("MYSQL_SANDBOX_DIR is not set")
		}
		t.Logf("sandbox dir: %s", sandboxDir)

		bytes, err := ioutil.ReadFile(filepath.Join(sandboxDir, "default_connection.json"))
		if err != nil {
			t.Fatalf("cannot read MYSQL_SANDBOX_DIR/default_connection.json: %s", err)
		}

		var c connInfo
		if err := json.Unmarshal(bytes, &c); err != nil {
			t.Fatalf("cannot unmarshal MYSQL_SANDBOX_DIR/default_connection.json: %s", err)
		}

		defaultPort = c.Port
		defaultDSN = fmt.Sprintf("msandbox:%s@tcp(%s:%s)/", c.Pass, c.Host, c.Port)
		t.Logf("dsn: %s", defaultDSN)
	}
	sandboxAction(t, "start")
}

func teardown(t *testing.T) {
}

func newDB(t *testing.T, dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("cannot connect to %s: %s", dsn, err)
	}
	return db
}

func TestNormalFlow(t *testing.T) {
	setup(t)
	defer teardown(t)

	db := newDB(t, defaultDSN)
	pool := myconn.NewPool(db)

	// First, let's make sure the stat start at zero
	now := time.Now().Unix()
	expectStats := myconn.Stats{
		// All values should be zero except:
		Open: 1, // from db.Ping() in newDB()
	}
	gotStats := pool.Stats()
	if gotStats.Ts < now {
		t.Errorf("got stats.Ts %d, expected >= %d", gotStats.Ts, now)
	}
	gotStats.Ts = 0
	if diff := deep.Equal(gotStats, expectStats); diff != nil {
		t.Error(diff)
	}

	// Normal workflow: open, close, check error
	ctx := context.TODO()
	conn, err := pool.Open(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	if conn == nil {
		t.Fatal("got nil conn, expected it to be set")
	}

	// Make sure the conn actually works
	var port string
	err = conn.QueryRowContext(ctx, "SELECT @@port").Scan(&port)
	if err != nil {
		t.Fatal(err)
	}
	if port != defaultPort {
		t.Errorf("got port %s, expected %s", port, defaultPort)
	}

	// Normally, the code block that calls Open() calls defer pool.Close(conn)
	pool.Close(conn)

	// Normally, the code block that uses the conn would return any error,
	// and the caller of that block would call pool.Error(err). Here, we simulate
	// no error.
	ok, err := pool.Error(nil)
	if ok == true {
		t.Errorf("got conn error true, expected false")
	}
	if err != nil {
		t.Errorf("got err %s, expected nil", err)
	}

	// Check the stats again
	now = time.Now().Unix()
	expectStats = myconn.Stats{
		Open:      1,
		OpenCalls: 1,
		Opened:    1,
		Closed:    1,
		// other values are zero
	}
	gotStats = pool.Stats()
	if gotStats.Ts < now {
		t.Errorf("got stats.Ts %d, expected >= %d", gotStats.Ts, now)
	}
	gotStats.Ts = 0
	if diff := deep.Equal(gotStats, expectStats); diff != nil {
		t.Error(diff)
	}
}

func TestLostConnection(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Create the normal pool but also...
	pool := myconn.NewPool(newDB(t, defaultDSN))

	// Create a pool for connection to kill conns in the normal pool
	kill := myconn.NewPool(newDB(t, defaultDSN))

	// The normal pool should have only 1 conn, get its ID so we
	// can kill it to simulate a lost connection
	ctx := context.TODO()
	conn, err := pool.Open(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	var id string
	err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close(conn)
	t.Logf("conn id: %s", id)

	// Kill the conn in the normal pool
	k, err := kill.Open(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	_, err = k.ExecContext(ctx, "KILL "+id)
	if err != nil {
		t.Fatal(err)
	}

	// Try to use the normal pool conn again and it should throw errors.
	// Because this is a sql.Conn, the driver is going to throw "packets.go:36:
	// unexpected EOF" first, then "connection.go:373: invalid connection" second,
	// then finally reconnect the conn. Don't ask me why; that's just the
	// behavior I observe from go-sql-driver/mysql v1.3. Ideally, it should
	// throw only one error when it first finds the conn is invalid, as sql.DB does.
	recovered := false
	for i := 0; i < 3; i++ {
		conn, err := pool.Open(ctx)
		if err != nil {
			t.Fatalf("got pool.Open error: %s, expected nil", err)
		}
		err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
		pool.Close(conn)
		if err != nil {
			// Error() should report this as a conn issue (lost=true) and the
			// error should be ErrConnLost which is a human-friendly wrapper
			// for various lower-level errors that mean the same.
			lost, err := pool.Error(err)
			t.Logf("try %d: %s", i, err)
			if !lost {
				t.Errorf("pool.Error lost=false, expected true (try %d)", i)
			}
			if err != myconn.ErrConnLost {
				t.Errorf("got error '%v', expected ErrConnLost", err)
			}
		} else {
			recovered = true
			break
		}
	}
	if !recovered {
		t.Error("pool conn did not recover after 3 retries")
	}

	// Check the stats reflect the failures correctly
	expectStats := myconn.Stats{
		Open:      1, // from db.Ping() in newDB()
		OpenCalls: 4,
		Opened:    4,
		Closed:    4,
		Lost:      2, // from the for look above
	}
	gotStats := pool.Stats()
	gotStats.Ts = 0
	if diff := deep.Equal(gotStats, expectStats); diff != nil {
		t.Error(diff)
	}
}

func TestMySQLDown(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Make one connection as usual.
	pool := myconn.NewPool(newDB(t, defaultDSN))
	ctx := context.TODO()
	conn, err := pool.Open(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	var id string
	err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close(conn)
	t.Logf("conn id: %s", id)

	// Stop MySQL sandbox
	sandboxAction(t, "stop")

	// As far as pool knows, its conn is still valid, but try to use it and
	// it'll throw two ErrConnLost before it tries to reconnect on pool.Open()
	// which will throw ErrErrConnCannotConnect.
	for i := 0; i < 2; i++ {
		conn, err = pool.Open(ctx)
		if err != nil {
			t.Fatalf("got pool.Open error: %s, expected nil", err)
		}
		err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
		pool.Close(conn)
		if err == nil {
			t.Error("got nil error, expected ErrConnLost")
		} else {
			lost, err := pool.Error(err)
			t.Logf("try %d: %s", i, err)
			if !lost {
				t.Errorf("pool.Error lost=false, expected true")
			}
			if err != myconn.ErrConnLost {
				t.Errorf("got error '%v', expected ErrConnLost", err)
			}
		}
	}

	// Now it'll start throwing ErrConnCannotConnect
	for i := 2; i < 4; i++ {
		conn, err = pool.Open(ctx)
		if err == nil {
			t.Error("got nil error, expected ErrConnCannotConnect")
		} else {
			lost, err := pool.Error(err)
			t.Logf("try %d: %s", i, err)
			if !lost {
				t.Errorf("pool.Error lost=false, expected true")
			}
			if err != myconn.ErrConnCannotConnect {
				t.Errorf("got error '%v', expected ErrConnCannotConnect", err)
			}
		}
	}

	// Check the stats reflect the failures correctly
	expectStats := myconn.Stats{
		Open:      0,
		OpenCalls: 5, // Opened+2 for the two calls in the second for loop
		Opened:    3, // the first Open() and the 2 in the first for loop
		Closed:    3,
		Lost:      2, // from the first for loop
		Down:      2, // from the second for loop
	}
	gotStats := pool.Stats()
	gotStats.Ts = 0
	if diff := deep.Equal(gotStats, expectStats); diff != nil {
		t.Error(diff)
	}
}

func TestServerShutdown(t *testing.T) {
	// MySQL is so polite that it'll tell you when it's shutting down.
	// And we're so polite we return ErrConnServerShutdown to let you know
	// the conn is about to be lost.
	setup(t)
	defer teardown(t)

	// Make one connection as usual.
	pool := myconn.NewPool(newDB(t, defaultDSN))
	ctx := context.TODO()
	conn, err := pool.Open(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}

	// Fake a long-running query so we can shutdown MySQL while it's running
	doneChan := make(chan error, 1)
	go func() {
		var n string
		err1 := conn.QueryRowContext(ctx, "SELECT SLEEP(5) FROM mysql.user").Scan(&n)
		pool.Close(conn)
		doneChan <- err1
	}()

	// Stop MySQL sandbox
	time.Sleep(500 * time.Millisecond) // yield to goroutine
	sandboxAction(t, "stop")

	// The goroutine shoud return immediately with "Error 1053: Server shutdown in progress",
	// which we treat as ErrConnLost because the conn is about to be lost.
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SELECT SLEEP(5) to return")
	case err = <-doneChan:
		t.Logf("recv err: %v", err)
		lost, err2 := pool.Error(err)
		if !lost {
			t.Errorf("pool.Error lost=false, expected true")
		}
		if err2 != myconn.ErrConnLost {
			t.Errorf("got error '%v', expected ErrConnLost", err2)
		}
	}

	// Check the stats reflect the failures correctly
	expectStats := myconn.Stats{
		Open:      1,
		OpenCalls: 1,
		Opened:    1,
		Closed:    1,
		Lost:      1,
	}
	gotStats := pool.Stats()
	gotStats.Ts = 0
	if diff := deep.Equal(gotStats, expectStats); diff != nil {
		t.Error(diff)
	}
}

func TestKillQuery(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Like TestLostConnection we'll need a normal and a kill pool
	pool := myconn.NewPool(newDB(t, defaultDSN))
	kill := myconn.NewPool(newDB(t, defaultDSN))

	// The normal pool should have only 1 conn, get its ID so we
	// can kill it to simulate a lost connection
	ctx := context.TODO()
	conn, err := pool.Open(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	var id string
	err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close(conn)
	t.Logf("conn id: %s", id)

	// Like TestServerShutdown simulate a long-running query we can kill
	doneChan := make(chan error, 1)
	go func() {
		conn, err = pool.Open(ctx)
		if err != nil {
			t.Fatalf("got pool.Open error: %s, expected nil", err)
		}
		var n string
		err1 := conn.QueryRowContext(ctx, "SELECT SLEEP(5) FROM mysql.user").Scan(&n)
		pool.Close(conn)
		doneChan <- err1
	}()
	time.Sleep(500 * time.Millisecond) // yield to goroutine

	// Kill the conn in the normal pool
	k, err := kill.Open(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	_, err = k.ExecContext(ctx, "KILL QUERY "+id)
	if err != nil {
		t.Fatal(err)
	}

	// The goroutine should return immediately because we killed the query, which
	// causes "Error 1317: Query execution was interrupted" aka ErrQueryKilled
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SELECT SLEEP(5) to return")
	case err = <-doneChan:
		t.Logf("recv err: %v", err)
		lost, err2 := pool.Error(err)
		if lost {
			// conn isn't lost, only killed the query
			t.Errorf("pool.Error lost=true, expected false")
		}
		if err2 != myconn.ErrQueryKilled {
			t.Errorf("got error '%v', expected ErrQueryKilled", err2)
		}
	}

	// Check the stats reflect the failures correctly
	expectStats := myconn.Stats{
		Open:      1,
		OpenCalls: 2,
		Opened:    2,
		Closed:    2,
		Lost:      0, // no conn lost
	}
	gotStats := pool.Stats()
	gotStats.Ts = 0
	if diff := deep.Equal(gotStats, expectStats); diff != nil {
		t.Error(diff)
	}
}

func TestReadOnly(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Make a connection as usual
	db := newDB(t, defaultDSN)
	pool := myconn.NewPool(db)
	ctx := context.TODO()
	conn, err := pool.Open(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	if conn == nil {
		t.Fatal("got nil conn, expected it to be set")
	}
	defer pool.Close(conn)

	// Make MySQL read-only
	_, err = conn.ExecContext(ctx, "SET GLOBAL read_only=1, super_read_only=1")
	if err != nil {
		t.Fatal(err)
	}

	// Make MySQL writeable again when done
	defer func() {
		_, err = conn.ExecContext(ctx, "SET GLOBAL read_only=0, super_read_only=0")
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Try to write something, which should cause "Error 1290: The MySQL server
	// is running with the --read-only option so it cannot execute this statement"
	_, err = conn.ExecContext(ctx, "CREATE TABLE test.t (id INT)")
	t.Logf("err: %v", err)
	lost, err := pool.Error(err)
	if lost == true {
		t.Error("got lost=true, expected false")
	}
	if err != myconn.ErrReadOnly {
		t.Errorf("got err '%v', expected ErrReadOnly", err)
	}
}

func TestDupeKey(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Make a connection as usual
	db := newDB(t, defaultDSN)
	pool := myconn.NewPool(db)
	ctx := context.TODO()
	conn, err := pool.Open(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	if conn == nil {
		t.Fatal("got nil conn, expected it to be set")
	}
	defer pool.Close(conn)

	// Make a test table and insert a value
	queries := []string{
		"DROP TABLE IF EXISTS test.t",
		"CREATE TABLE test.t (id INT, UNIQUE KEY (id))",
		"INSERT INTO test.t VALUES (1)",
	}
	for _, q := range queries {
		_, err := conn.ExecContext(ctx, q)
		if err != nil {
			t.Fatalf("%s: %s", q, err)
		}
	}
	defer func() {
		_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS test.t")
		if err != nil {
			t.Errorf("cannot drop table test.t: %s", err)
		}
	}()

	// Insert a duplicate value which causes "Error 1062: Duplicate entry '1' for key 'id'"
	_, err = conn.ExecContext(ctx, "INSERT INTO test.t VALUES (1)")
	t.Logf("err: %v", err)
	lost, err := pool.Error(err)
	if lost == true {
		t.Error("got lost=true, expected false")
	}
	if err != myconn.ErrDupeKey {
		t.Errorf("got err '%v', expected ErrDupeKey", err)
	}
}

func TestRandomError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Make a connection as usual
	db := newDB(t, defaultDSN)
	pool := myconn.NewPool(db)
	ctx := context.TODO()
	conn, err := pool.Open(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	if conn == nil {
		t.Fatal("got nil conn, expected it to be set")
	}
	defer pool.Close(conn)

	// It's easy to produce a random error we don't handle.
	// Example: syntax error: "Error 1064: You have an error in your SQL syntax; ..."
	_, err = conn.ExecContext(ctx, "INSERT bananas!")
	t.Logf("err: %v", err)
	lost, err := pool.Error(err)
	if lost == true {
		t.Error("got lost=true, expected false")
	}
	if err == nil {
		t.Errorf("got nil err, expected an error")
	}

	// While we're here, let's test MySQLErrorCode
	errCode := myconn.MySQLErrorCode(err)
	if errCode != 1064 {
		t.Errorf("got MySQL error code %d, expected 1064", errCode)
	}
}

func TestOpenTimeout(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Make a sql.DB that's limited to 1 connetion so it's easy to
	// make a 2nd conn block and timeout
	db := newDB(t, defaultDSN)
	db.SetMaxOpenConns(1)
	pool := myconn.NewPool(db)
	ctx := context.TODO()

	// Make one connection run for awhile
	doneChan := make(chan error, 1)
	go func() {
		conn, err := pool.Open(ctx)
		if err != nil {
			t.Fatalf("got pool.Open error: %s, expected nil", err)
		}
		defer pool.Close(conn)
		var n string
		doneChan <- conn.QueryRowContext(ctx, "SELECT SLEEP(2)").Scan(&n)
	}()
	time.Sleep(200 * time.Millisecond) // yield to goroutine

	// Try to open a 2nd conn with a timeout ctx
	timeout, cancel := context.WithTimeout(context.Background(), time.Duration(500*time.Millisecond))
	defer cancel()
	conn, err := pool.Open(timeout)
	if err == nil {
		t.Fatalf("got pool.Open error: %v, expected ErrTimeout", err)
	} else {
		lost, err2 := pool.Error(err)
		if lost == true {
			t.Error("got lost=true, expected false")
		}
		if err2 != myconn.ErrTimeout {
			t.Errorf("got err '%v', expected ErrTimeout", err2)
		}
	}
	if conn != nil {
		t.Errorf("got conn, expected nil")
	}

	// Make sure that goroutine returns
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SELECT SLEEP(5) to return")
	case <-doneChan:
	}
}
