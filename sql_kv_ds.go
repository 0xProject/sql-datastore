package sqlds

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" //postgres driver
)

// Options are the postgres datastore options, reexported here for convenience.
type Options struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	Table    string
}

type queries struct {
	tableName string
}

func newQueries(tableName string) *queries {
	return &queries{tableName}
}

func (q queries) Delete() string {
	return `DELETE FROM ` + q.tableName + ` WHERE key = $1`
}

func (q queries) Exists() string {
	return `SELECT exists(SELECT 1 FROM ` + q.tableName + ` WHERE key=$1)`
}

func (q queries) Get() string {
	return `SELECT data FROM ` + q.tableName + ` WHERE key = $1`
}

func (q queries) Put() string {
	return `INSERT INTO ` + q.tableName + ` (key, data) SELECT $1, $2 WHERE NOT EXISTS ( SELECT key FROM ` + q.tableName + ` WHERE key = $1)`
}

func (q queries) Query() string {
	return `SELECT key, data FROM ` + q.tableName
}

func (q queries) Prefix() string {
	return ` WHERE key LIKE '%s%%' ORDER BY key`
}

func (q queries) Limit() string {
	return ` LIMIT %d`
}

func (q queries) Offset() string {
	return ` OFFSET %d`
}

func (q queries) GetSize() string {
	return `SELECT octet_length(data) FROM ` + q.tableName + ` WHERE key = $1`
}

// Create returns a datastore connected to postgres initialized with a table
func (opts *Options) CreatePostgres() (*Datastore, error) {
	opts.setDefaults()
	fmtstr := "postgresql:///%s?host=%s&port=%s&user=%s&password=%s&sslmode=disable"
	constr := fmt.Sprintf(fmtstr, opts.Database, opts.Host, opts.Port, opts.User, opts.Password)
	db, err := sql.Open("postgres", constr)
	if err != nil {
		return nil, err
	}

	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key TEXT NOT NULL UNIQUE, data BYTEA NOT NULL)", opts.Table)
	_, err = db.Exec(createTable)

	if err != nil {
		return nil, err
	}

	return NewDatastore(db, newQueries(opts.Table)), nil
}

func (opts *Options) setDefaults() {
	if opts.Table == "" {
		opts.Table = "kv"
	}
	if opts.Host == "" {
		opts.Host = "postgres"
	}

	if opts.Port == "" {
		opts.Port = "5432"
	}

	if opts.User == "" {
		opts.User = "postgres"
	}

	if opts.Database == "" {
		opts.Database = "datastore"
	}
}
