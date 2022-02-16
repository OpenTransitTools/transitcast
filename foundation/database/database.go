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

// PrepareNamedQueryFromMap wraps boilerplate sqlx to prepare named query from map of ddl parameters
// returns rebound query string and arguments slice
func PrepareNamedQueryFromMap(
	statementString string,
	db *sqlx.DB,
	sqlArgMap map[string]interface{}) (string, []interface{}, error) {

	query, args, err := sqlx.Named(statementString, sqlArgMap)
	if err != nil {
		return query, nil, err
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return query, nil, err
	}
	query = db.Rebind(query)
	return query, args, nil
}

// PrepareNamedQueryRowsFromMap wraps boilerplate sqlx to prepare named query from map of ddl parameters
// returns sqlx.Rows after executing query with db.Queryx
func PrepareNamedQueryRowsFromMap(
	statementString string,
	db *sqlx.DB,
	sqlArgMap map[string]interface{}) (*sqlx.Rows, error) {

	query, args, err := PrepareNamedQueryFromMap(statementString, db, sqlArgMap)
	if err != nil {
		return nil, err
	}
	rows, err := db.Queryx(query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
