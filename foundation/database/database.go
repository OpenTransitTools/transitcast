// Package database provides support for access the database.
package database

import (
	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	"net/url"
)

// Config is the required properties to use the database.
type Config struct {
	User       string
	Password   string
	Host       string
	Name       string
	DisableTLS bool
}

// Open knows how to open a database connection based on the configuration.
func Open(cfg Config) (*sqlx.DB, error) {
	sslMode := "require"
	if cfg.DisableTLS {
		sslMode = "disable"
	}

	q := make(url.Values)
	q.Set("sslmode", sslMode)
	q.Set("timezone", "utc")

	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     cfg.Host,
		Path:     cfg.Name,
		RawQuery: q.Encode(),
	}
	return sqlx.Connect("pgx", u.String())
}
