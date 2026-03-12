package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"

	"github.com/hashicorp/go-version"
)

// mockConnector is a mock implementation of driver.Connector for testing
type mockConnector struct {
	connectCount int
	mu           sync.Mutex
	conn         *mockConn
	allConns     []*mockConn // Track all connections created
	connectErr   error
}

func (m *mockConnector) Connect(ctx context.Context) (driver.Conn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectCount++
	if m.connectErr != nil {
		return nil, m.connectErr
	}
	conn := &mockConn{
		execCount: 0,
	}
	m.conn = conn
	m.allConns = append(m.allConns, conn)
	return conn, nil
}

func (m *mockConnector) Driver() driver.Driver {
	return &mockDriver{}
}

func (m *mockConnector) getConnectCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connectCount
}

func (m *mockConnector) getAllConns() []*mockConn {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*mockConn, len(m.allConns))
	copy(result, m.allConns)
	return result
}

// mockConn is a mock implementation of driver.Conn for testing
type mockConn struct {
	execCount  int
	execQuery  string
	mu         sync.Mutex
	closed     bool
	prepCount  int
	beginCount int
}

func (m *mockConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.execCount++
	m.execQuery = query
	return &mockResult{}, nil
}

func (m *mockConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.execCount++
	m.execQuery = query
	return &mockResult{}, nil
}

func (m *mockConn) Prepare(query string) (driver.Stmt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prepCount++
	if m.closed {
		return nil, errors.New("connection closed")
	}
	return &mockStmt{}, nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConn) Begin() (driver.Tx, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.beginCount++
	if m.closed {
		return nil, errors.New("connection closed")
	}
	return &mockTx{}, nil
}

func (m *mockConn) getExecCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.execCount
}

func (m *mockConn) getExecQuery() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.execQuery
}

// mockResult, mockStmt, mockTx, mockDriver are minimal implementations
type mockResult struct{}

func (m *mockResult) LastInsertId() (int64, error) { return 0, nil }
func (m *mockResult) RowsAffected() (int64, error) { return 0, nil }

type mockStmt struct{}

func (m *mockStmt) Close() error                                    { return nil }
func (m *mockStmt) NumInput() int                                   { return 0 }
func (m *mockStmt) Exec(args []driver.Value) (driver.Result, error) { return &mockResult{}, nil }
func (m *mockStmt) Query(args []driver.Value) (driver.Rows, error)  { return nil, nil }

type mockTx struct{}

func (m *mockTx) Commit() error   { return nil }
func (m *mockTx) Rollback() error { return nil }

type mockDriver struct{}

func (m *mockDriver) Open(name string) (driver.Conn, error) { return &mockConn{}, nil }

// TestSessionInitializingConnector_SetsSessionOnEachConnection tests that
// the sessionInitializingConnector applies session settings to each new connection.
func TestSessionInitializingConnector_SetsSessionOnEachConnection(t *testing.T) {
	// Create a version for MySQL 5.7.x (should use NO_AUTO_CREATE_USER)
	v, _ := version.NewVersion("5.7.10")

	mckCnctr := &mockConnector{}

	connector := &sessionInitializingConnector{
		base: mckCnctr,
	}
	connector.setVersion(v)

	expectedQuery := `SET SESSION sql_mode='NO_AUTO_CREATE_USER'`

	// Create multiple connections and verify each gets session settings
	conn1, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}
	if conn1 == nil {
		t.Fatal("Connect() returned nil connection")
	}

	// Create a second connection
	conn2, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() for second connection failed: %v", err)
	}

	// Create a third connection
	conn3, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() for third connection failed: %v", err)
	}

	// Verify that Connect was called 3 times on the base connector
	if mckCnctr.getConnectCount() != 3 {
		t.Errorf("Expected 3 base connector calls, got %d", mckCnctr.getConnectCount())
	}

	// Get all connections and verify each one got session settings
	allConns := mckCnctr.getAllConns()
	if len(allConns) != 3 {
		t.Fatalf("Expected 3 connections to be tracked, got %d", len(allConns))
	}

	for i, conn := range allConns {
		if conn.getExecCount() != 1 {
			t.Errorf("Connection %d: expected 1 exec call, got %d", i, conn.getExecCount())
		}
		if conn.getExecQuery() != expectedQuery {
			t.Errorf("Connection %d: expected query %q, got %q", i, expectedQuery, conn.getExecQuery())
		}
	}

	// Verify connections are different objects
	if conn1 == conn2 || conn2 == conn3 || conn1 == conn3 {
		t.Error("Expected different connection objects")
	}
}

// TestSessionInitializingConnector_MySQL8EmptySQLMode tests that
// MySQL 8.x uses empty sql_mode.
func TestSessionInitializingConnector_MySQL8EmptySQLMode(t *testing.T) {
	v, _ := version.NewVersion("8.0.25")

	mckCnctr := &mockConnector{}

	connector := &sessionInitializingConnector{
		base: mckCnctr,
	}
	connector.setVersion(v)

	_, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}

	expectedQuery := `SET SESSION sql_mode=''`
	innerConn := mckCnctr.conn
	if innerConn.getExecQuery() != expectedQuery {
		t.Errorf("Expected query %q, got %q", expectedQuery, innerConn.getExecQuery())
	}
}

// TestSessionInitializingConnector_MySQL57EmptySession tests that
// MySQL 5.7.4 and earlier uses empty sql_mode (before NO_AUTO_CREATE_USER default).
func TestSessionInitializingConnector_MySQL57EmptySession(t *testing.T) {
	v, _ := version.NewVersion("5.7.4")

	mckCnctr := &mockConnector{}

	connector := &sessionInitializingConnector{
		base: mckCnctr,
	}
	connector.setVersion(v)

	_, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}

	expectedQuery := `SET SESSION sql_mode=''`
	innerConn := mckCnctr.conn
	if innerConn.getExecQuery() != expectedQuery {
		t.Errorf("Expected query %q, got %q", expectedQuery, innerConn.getExecQuery())
	}
}

// TestSessionInitializingConnector_NoVersion tests that
// when version is nil, no session query is executed.
func TestSessionInitializingConnector_NoVersion(t *testing.T) {
	mckCnctr := &mockConnector{}

	connector := &sessionInitializingConnector{
		base: mckCnctr,
	}
	// Don't set version

	conn, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}

	// Connection should still be created
	if conn == nil {
		t.Fatal("Expected non-nil connection")
	}

	// But no exec should have been called
	innerConn := mckCnctr.conn
	if innerConn.getExecCount() != 0 {
		t.Errorf("Expected 0 exec calls when version is nil, got %d", innerConn.getExecCount())
	}
}

// TestSessionInitializingConnector_BaseConnectError tests that
// errors from the base connector are propagated.
func TestSessionInitializingConnector_BaseConnectError(t *testing.T) {
	expectedErr := errors.New("connection failed")

	mckCnctr := &mockConnector{
		connectErr: expectedErr,
	}

	connector := &sessionInitializingConnector{
		base: mckCnctr,
	}
	v, _ := version.NewVersion("8.0.0")
	connector.setVersion(v)

	_, err := connector.Connect(t.Context())
	if err == nil {
		t.Fatal("Expected error from base connector")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

// TestSessionInitializingConnector_ExecErrorClosesConn tests that
// when session exec fails, the connection is closed.
func TestSessionInitializingConnector_ExecErrorClosesConn(t *testing.T) {
	// Create a mock connection that fails on exec
	mckCnctr := &mockConnectorWithFailingExec{}

	connector := &sessionInitializingConnector{
		base: mckCnctr,
	}
	v, _ := version.NewVersion("8.0.0")
	connector.setVersion(v)

	_, err := connector.Connect(t.Context())
	if err == nil {
		t.Fatal("Expected error from session exec")
	}

	// Verify the connection was closed
	if !mckCnctr.conn.closed {
		t.Error("Expected connection to be closed after exec failure")
	}
}

// mockConnectorWithFailingExec creates connections that fail on exec
type mockConnectorWithFailingExec struct {
	conn *mockConnFailingExec
}

func (m *mockConnectorWithFailingExec) Connect(ctx context.Context) (driver.Conn, error) {
	m.conn = &mockConnFailingExec{}
	return m.conn, nil
}

func (m *mockConnectorWithFailingExec) Driver() driver.Driver {
	return &mockDriver{}
}

// mockConnFailingExec fails on exec
type mockConnFailingExec struct {
	closed bool
}

func (m *mockConnFailingExec) Exec(query string, args []driver.Value) (driver.Result, error) {
	return nil, errors.New("exec failed")
}

func (m *mockConnFailingExec) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("exec failed")
}

func (m *mockConnFailingExec) Prepare(query string) (driver.Stmt, error) { return nil, nil }
func (m *mockConnFailingExec) Close() error {
	m.closed = true
	return nil
}
func (m *mockConnFailingExec) Begin() (driver.Tx, error) { return nil, nil }

// TestGetSQLModeForConnection tests the version-based SQL mode logic.
func TestGetSQLModeForConnection(t *testing.T) {
	testCases := []struct {
		name            string
		version         string
		expectedSQLMode string
	}{
		{
			name:            "MySQL 5.6.x - before NO_AUTO_CREATE_USER",
			version:         "5.6.45",
			expectedSQLMode: `SET SESSION sql_mode=''`,
		},
		{
			name:            "MySQL 5.7.4 - just before NO_AUTO_CREATE_USER",
			version:         "5.7.4",
			expectedSQLMode: `SET SESSION sql_mode=''`,
		},
		{
			name:            "MySQL 5.7.5 - first version with NO_AUTO_CREATE_USER",
			version:         "5.7.5",
			expectedSQLMode: `SET SESSION sql_mode='NO_AUTO_CREATE_USER'`,
		},
		{
			name:            "MySQL 5.7.30 - within NO_AUTO_CREATE_USER range",
			version:         "5.7.30",
			expectedSQLMode: `SET SESSION sql_mode='NO_AUTO_CREATE_USER'`,
		},
		{
			name:            "MySQL 5.7.999 - last version before 8.0",
			version:         "5.7.999",
			expectedSQLMode: `SET SESSION sql_mode='NO_AUTO_CREATE_USER'`,
		},
		{
			name:            "MySQL 8.0.0 - NO_AUTO_CREATE_USER removed",
			version:         "8.0.0",
			expectedSQLMode: `SET SESSION sql_mode=''`,
		},
		{
			name:            "MySQL 8.0.25 - current version",
			version:         "8.0.25",
			expectedSQLMode: `SET SESSION sql_mode=''`,
		},
		{
			name:            "MySQL 8.4.0 - latest",
			version:         "8.4.0",
			expectedSQLMode: `SET SESSION sql_mode=''`,
		},
		{
			name:            "MySQL 9.0.0 - future version",
			version:         "9.0.0",
			expectedSQLMode: `SET SESSION sql_mode=''`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := version.NewVersion(tc.version)
			if err != nil {
				t.Fatalf("Failed to parse version: %v", err)
			}

			result := getSQLModeForConnection(v)
			if result != tc.expectedSQLMode {
				t.Errorf("Expected SQL mode %q, got %q", tc.expectedSQLMode, result)
			}
		})
	}
}
