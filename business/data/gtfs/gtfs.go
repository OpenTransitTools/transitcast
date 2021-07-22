// Package gtfs provides gtfs related CRUD functionality
package gtfs

import (
	"fmt"
	"github.com/jmoiron/sqlx"

	"time"
)

// DataSetOperation contains required data for operating on gtfs records owned by a DataSet
type DataSetOperation struct {
	DS *DataSet
	Db *sqlx.DB
}

// DataSetTransaction contains required data for recording new gtfs records owned by a DataSet
type DataSetTransaction struct {
	DS DataSet
	Tx *sqlx.Tx
}

// DataSet encompasses a gtfs schedule available from a source at a point in time.
//The same source will be loaded over time.
// Each record from a gtfs file shares the DataSet.Id value as part of the primary key.
type DataSet struct {
	Id  int64
	URL string
	// ETag is the ETag header if available from the source web site for the gtfs file. Is empty if not available
	ETag string `db:"e_tag"`
	// LastModifiedTimestamp is the unix epoch seconds the source web site provided for the last time the gtfs file was modified
	// is 0 if not available
	LastModifiedTimestamp int64      `db:"last_modified_timestamp"`
	DownloadedAt          time.Time  `db:"downloaded_at"`
	SavedAt               *time.Time `db:"saved_at"`
	ReplacedAt            *time.Time `db:"replaced_at"`
}

func (d DataSet) String() string {
	lastModified := ""
	if d.LastModifiedTimestamp != 0 {
		lastModTime := time.Unix(d.LastModifiedTimestamp, 0)
		lastModified = formatTime(&lastModTime)
	}
	return fmt.Sprintf("DataSet id:%d, url:%s, ETag:%s, lastModified:%s savedAt:%s replacedAt:%s",
		d.Id, d.URL, d.ETag, lastModified, formatTime(d.SavedAt), formatTime(d.ReplacedAt))
}

func formatTime(time *time.Time) string {
	if time == nil {
		return ""
	}
	return time.Format("2006-01-02T15:04:05")
}

// SaveAndTerminateReplacedDataSet updates all DataSet where now is between DataSet.SavedAt and DataSet.ReplacedAt and
//sets DataSet.ReplacedAt to one microsecond before now.
//ds is then saved with now as DataSet.SavedAt and the default DataSet.ReplacedAt date of 9999-12-31
func SaveAndTerminateReplacedDataSet(tx *sqlx.Tx, ds *DataSet, now time.Time) error {
	endDate, err := time.Parse("2006-01-02", "9999-12-31")
	if err != nil {
		return err
	}
	millisecondAgo := now.Add(-time.Microsecond)
	statementString := "update data_set set replaced_at = :millisecondAgo" +
		" where :now between saved_at and replaced_at"
	//statementString = tx.Rebind(statementString)
	_, err = tx.NamedExec(statementString, map[string]interface{}{"now": now, "millisecondAgo": millisecondAgo})
	if err != nil {
		return err
	}
	ds.SavedAt = &now
	ds.ReplacedAt = &endDate
	return SaveDataSet(tx, ds)
}

/*
SaveDataSet saves new or updates existing DataSets.

*/
func SaveDataSet(tx *sqlx.Tx, ds *DataSet) error {
	statementString := "insert into data_set ( " +
		"url, " +
		"e_tag, " +
		"last_modified_timestamp, " +
		"downloaded_at, " +
		"saved_at, " +
		"replaced_at) " +
		"values (" +
		":url, " +
		":e_tag, " +
		":last_modified_timestamp, " +
		":downloaded_at, " +
		":saved_at, " +
		":replaced_at)"
	if ds.Id != 0 {
		statementString = "update data_set set " +
			"url = :url, " +
			"e_tag = :e_tag, " +
			"last_modified_timestamp = :last_modified_timestamp, " +
			"downloaded_at = :downloaded_at, " +
			"saved_at = :saved_at, " +
			"replaced_at = :replaced_at " +
			"where id = :id"
	}

	statementString = tx.Rebind(statementString)
	_, err := tx.NamedExec(statementString, ds)
	if err != nil {
		return err
	}
	// retrieve new id if zero
	if ds.Id == 0 {
		statementString = tx.Rebind("SELECT id FROM data_set " +
			"where e_tag = ? " +
			"and last_modified_timestamp = ? " +
			"and downloaded_at = ? limit 1")
		err = tx.Get(&ds.Id, statementString, ds.ETag, ds.LastModifiedTimestamp, ds.DownloadedAt)
		if err != nil {
			return err
		}
	}

	return err
}

// GetDataSet retrieves DataSet with dataSetId
func GetDataSet(db *sqlx.DB, dataSetId int64) (*DataSet, error) {
	query := "select * from data_set where id = $1"
	ds := DataSet{}
	err := db.Get(&ds, db.Rebind(query), dataSetId)
	return &ds, err
}

// GetLatestDataSet retrieves the latest DataSet that is active
func GetLatestDataSet(db *sqlx.DB) (*DataSet, error) {
	return GetDataSetAt(db, time.Now())
}

// GetDataSetAt retrieves the DataSet that was active at a time
func GetDataSetAt(db *sqlx.DB, at time.Time) (*DataSet, error) {
	query := "select * from data_set " +
		"where $1 between saved_at and replaced_at order by saved_at desc limit 1"
	ds := DataSet{}
	err := db.Get(&ds, db.Rebind(query), at)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve DataSet at %v, error: %w", at, err)
	}
	return &ds, nil
}

// GetAllDataSets retrieves all DataSets currently loaded
func GetAllDataSets(db *sqlx.DB) ([]DataSet, error) {
	query := "select * from data_set"
	var results []DataSet
	err := db.Select(&results, query)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve all DataSets. error: %w", err)
	}
	return results, nil
}

// prepareNamedQueryRowsFromMap wraps boilerplate sqlx to prepare named query from map of sql parameters
func prepareNamedQueryRowsFromMap(statementString string, db *sqlx.DB, sqlArgMap map[string]interface{}) (*sqlx.Rows, error) {
	query, args, err := sqlx.Named(statementString, sqlArgMap)
	if err != nil {
		return nil, err
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return nil, err
	}
	query = db.Rebind(query)
	rows, err := db.Queryx(query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
