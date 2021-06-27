// Package gtfs provides gtfs related CRUD functionality
package gtfs

import (
	"fmt"
	"github.com/jmoiron/sqlx"

	"time"
)

//test removal of db:id simple fields
// add terminatedAt to DataSet

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
}

func (d DataSet) String() string {
	lastModified := ""
	if d.LastModifiedTimestamp != 0 {
		lastModTime := time.Unix(d.LastModifiedTimestamp, 0)
		lastModified = formatTime(&lastModTime)
	}
	return fmt.Sprintf("DataSet Id:%d, url:%s, ETag:%s, lastModified:%s downloaded:%s savedAt:%s",
		d.Id, d.URL, d.ETag, lastModified, formatTime(&d.DownloadedAt), formatTime(d.SavedAt))
}

func formatTime(time *time.Time) string {
	if time == nil {
		return ""
	}
	return time.Format("2006-01-02T15:04:05")
}

/*
SaveDataSet saves new or updates existing DataSets. Existing records are determined by a non-zero DataSet.ID

*/
func SaveDataSet(tx *sqlx.Tx, ds *DataSet) error {
	statementString := "insert into data_set ( " +
		"url, " +
		"e_tag, " +
		"last_modified_timestamp, " +
		"downloaded_at, " +
		"saved_at) " +
		"values (" +
		":url, " +
		":e_tag, " +
		":last_modified_timestamp, " +
		":downloaded_at, " +
		":saved_at)"
	if ds.Id != 0 {
		statementString = "update data_set set " +
			"url = :url, " +
			"e_tag = :e_tag, " +
			"last_modified_timestamp = :last_modified_timestamp, " +
			"downloaded_at = :downloaded_at, " +
			"saved_at = :saved_at " +
			" where id = :id"
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

// GetLatestSavedDataSet retrieves the latest DataSet with a saved_at date
func GetLatestSavedDataSet(db *sqlx.DB) (*DataSet, error) {
	query := "select * from data_set where saved_at is not null order by saved_at desc, downloaded_at desc limit 1"
	ds := DataSet{}
	err := db.Get(&ds, query)
	return &ds, err
}

// GetAllDataSets retrieves all DataSets currently loaded
func GetAllDataSets(db *sqlx.DB) ([]DataSet, error) {
	query := "select * from data_set"
	var results []DataSet
	err := db.Select(&results, query)
	return results, err
}
