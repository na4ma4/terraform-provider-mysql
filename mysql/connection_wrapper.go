package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"

	"github.com/hashicorp/go-version"
)

// sessionInitializingConnector wraps a driver.Connector to initialize session settings
// on each new connection that is created from the pool.
type sessionInitializingConnector struct {
	base         driver.Connector
	sqlModeQuery string
	version      *version.Version
	versionMu    sync.RWMutex
}

// Connect returns a connection to the database.
// Connect may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes. A default timeout should still be used
// when dialing as a connection pool may call Connect
// asynchronously to any query.
//
// The returned connection is only used by one goroutine at a
// time.
func (c *sessionInitializingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}

	// Get the version once
	c.versionMu.RLock()
	ver := c.version
	c.versionMu.RUnlock()

	if ver != nil && c.sqlModeQuery != "" {
		// Apply session settings to the connection
		if err := c.applySessionModeToConnection(ctx, conn); err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to set session sql_mode: %v", err)
		}
	}

	return conn, nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c *sessionInitializingConnector) Driver() driver.Driver {
	return c.base.Driver()
}

func (c *sessionInitializingConnector) setVersion(v *version.Version) {
	c.versionMu.Lock()
	defer c.versionMu.Unlock()
	c.version = v

	c.sqlModeQuery = getSQLModeForConnection(v)
}

// applySessionModeToConnection executes the SQL mode session statement on the given connection.
func (c *sessionInitializingConnector) applySessionModeToConnection(ctx context.Context, conn driver.Conn) error {
	if execer, ok := conn.(driver.ExecerContext); ok {
		_, err := execer.ExecContext(ctx, c.sqlModeQuery, nil)
		return err
	} else if execerOld, ok := conn.(driver.Execer); ok {
		_, err := execerOld.Exec(c.sqlModeQuery, nil)
		return err
	} else {
		return errors.New("connection does not support Exec or ExecContext for setting session sql_mode")
	}
}

// getSQLModeForConnection returns the appropriate SQL mode session statement based on the MySQL version.
func getSQLModeForConnection(v *version.Version) string {
	// Return the SQL mode statement based on version
	versionMinInclusive, _ := version.NewVersion("5.7.5")
	versionMaxExclusive, _ := version.NewVersion("8.0.0")
	if v.GreaterThanOrEqual(versionMinInclusive) && v.LessThan(versionMaxExclusive) {
		// We set NO_AUTO_CREATE_USER to prevent provider from creating user when creating grants. Newer MySQL has it automatically.
		// We don't want any other modes, esp. not ANSI_QUOTES.
		return `SET SESSION sql_mode='NO_AUTO_CREATE_USER'`
	}

	// We don't want any modes, esp. not ANSI_QUOTES.
	return `SET SESSION sql_mode=''`
}
