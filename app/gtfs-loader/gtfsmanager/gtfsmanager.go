// Package gtfsmanager provides support for retrieving, reading, parsing, deleting and saving gtfs schedules to a database
package gtfsmanager

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/OpenTransitTools/transitcast/foundation/httpclient"
	"github.com/jmoiron/sqlx"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"
)

// DeleteGTFSSchedule deletes all gtfs records associated with gtfs.DataSet with dataSetId
func DeleteGTFSSchedule(log *log.Logger,
	db *sqlx.DB,
	dataSetId int64) error {

	dataSet, err := gtfs.GetDataSet(db, dataSetId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("no DataSet found with id %d", dataSetId)
		}
		return err
	}
	err = transact(log, db, func(tx *sqlx.Tx) error {
		log.Printf("Removing dataSet %v", dataSet)
		deleteStatements := []struct {
			query string
			name  string
		}{
			{
				name:  "stop_time",
				query: "delete from stop_time where data_set_id = ?",
			},
			{
				name:  "trip",
				query: "delete from trip where data_set_id = ?",
			},
			{
				name:  "shape",
				query: "delete from shape where data_set_id = ?",
			},
			{
				name:  "calendar",
				query: "delete from calendar where data_set_id = ?",
			},
			{
				name:  "calendar_date",
				query: "delete from calendar_date where data_set_id = ?",
			},
			{
				name:  "data_set",
				query: "delete from data_set where id = ?",
			},
		}
		for _, deleteStatement := range deleteStatements {
			stmt, innerErr := tx.Prepare(tx.Rebind(deleteStatement.query))
			if innerErr != nil {
				return fmt.Errorf("error running '%s' error:%w", deleteStatement.query, innerErr)
			}
			result, innerErr := stmt.Exec(dataSet.Id)
			if innerErr != nil {
				return fmt.Errorf("error running '%s' error:%w", deleteStatement.query, innerErr)
			}
			rows, innerErr := result.RowsAffected()
			if innerErr != nil {
				return fmt.Errorf("error retrieving rows affected after '%s' error:%w", deleteStatement.query, innerErr)
			}
			log.Printf("Deleted %d lines from %s\n", rows, deleteStatement.name)
		}
		return nil
	})
	if err != nil {
		return err
	}
	log.Printf("Deleted DataSet %v", dataSet)
	return nil
}

// UpdateGTFSSchedule checks for updated gtfs schedule on remote server
// if new version is detected attempts to load gtfs file in zip format to localDownloadDirectory from url to database
// forceDownload flag will bypass remote check
func UpdateGTFSSchedule(log *log.Logger,
	db *sqlx.DB,
	localDownloadDirectory string,
	url string,
	forceDownload bool) error {
	if forceDownload {
		log.Printf("Not checking remote gtfs file for new information, forcing load of gtfs file")
	} else if !shouldUpdateGTFSSchedule(log, db, url) {
		return nil
	}

	err := makeDirectoryIfNotPresent(localDownloadDirectory)
	if err != nil {
		return err
	}
	start := time.Now()
	localGtfsZipFile := filepath.Join(localDownloadDirectory, "gtfs.zip")
	log.Printf("Downloading file from %s to %s\n", url, localGtfsZipFile)
	downloadedFile, err := httpclient.DownloadRemoteFile(localGtfsZipFile, url)

	//remove downloaded file after we are done
	defer func() {
		if _, err := os.Stat(localGtfsZipFile); err == nil {
			err = os.Remove(localGtfsZipFile)
			if err != nil {
				log.Printf("Unable to remove downloaded file. error:%v", err)
			}
		}
	}()
	if err != nil {
		return err
	}

	log.Printf("Downloaded %v bytes in %v seconds\n",
		downloadedFile.Size, downloadedFile.DownloadedAt.Unix()-start.Unix())

	_, err = loadGTFSScheduleFromFile(log, db, *downloadedFile)

	return err

}

// shouldUpdateGTFSSchedule checks currently loaded gtfs.DataSet and compares it to what's available on the remote
// server. If it see's a differance returns true.
// On error logs and returns false.
// if the gtfs.DataSet.ETag or gtfs.DataSet.LastModifiedTimestamp match the remote file information returns false.
func shouldUpdateGTFSSchedule(log *log.Logger, db *sqlx.DB, url string) bool {
	remoteFileInfo, err := httpclient.GetRemoteFileInfo(url)
	if err != nil {
		log.Printf("Unable to retrieve remote file information from '%s' error: %v", url, err)
		return false
	}

	existingDataSet, err := gtfs.GetLatestDataSet(db)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("No DataSet loaded, should perform initial load")
			return true
		}
		log.Printf("Received error checking DataSet from database. error: %v", err)
		return false
	}
	// use eTag if not empty
	if len(remoteFileInfo.ETag) > 0 {
		if remoteFileInfo.ETag != existingDataSet.ETag {
			log.Printf("Remote file ETag indicates new file available")
			return true
		}
		log.Printf("Remote file ETag indicates the loaded DataSet is current: %v", *existingDataSet)
		return false

	}
	//if last modified timestamp is zero, do load
	if remoteFileInfo.LastModifiedTimestamp == 0 {
		log.Printf("Unable to determine remote file timestamp or eTag, can't not determine if dataset should be reloaded")
		return false
	}
	if remoteFileInfo.LastModifiedTimestamp != existingDataSet.LastModifiedTimestamp {
		log.Printf("Remote file last timestamp indicates new file available")
		return true
	}
	log.Printf("Remote file last_timestamp indicates the loaded DataSet is current: %v", *existingDataSet)
	return false
}

// ListGTFSSchedules displays a list of all DataSets to logger
func ListGTFSSchedules(db *sqlx.DB) error {
	fmt.Println("Loaded DataSets:")
	dataSets, err := gtfs.GetAllDataSets(db)
	if err != nil {
		return err
	}
	for _, ds := range dataSets {
		fmt.Println(&ds)
	}
	return nil
}

// loadGTFSScheduleFromFile loads gtfs file described in httpclient.DownloadedFile and saves it to new DataSet
// wrapped inside single transaction
func loadGTFSScheduleFromFile(log *log.Logger,
	db *sqlx.DB,
	downloadedFile httpclient.DownloadedFile) (*gtfs.DataSet, error) {
	// Create and data set to save other data under
	ds := gtfs.DataSet{
		URL:                   downloadedFile.RemoteFileInfo.Path,
		ETag:                  downloadedFile.RemoteFileInfo.ETag,
		LastModifiedTimestamp: downloadedFile.RemoteFileInfo.LastModifiedTimestamp,
		DownloadedAt:          downloadedFile.DownloadedAt,
	}
	err := transact(log, db, func(tx *sqlx.Tx) error {
		err := gtfs.SaveDataSet(tx, &ds)
		if err != nil {
			return err
		}

		// create DataSetTransaction for recording gtfs records
		dsTx := gtfs.DataSetTransaction{
			DS: ds,
			Tx: tx,
		}

		err = loadGtfsZipFile(log, &dsTx, downloadedFile.LocalFilePath)
		if err != nil {
			return err
		}
		now := time.Now()
		err = gtfs.SaveAndTerminateReplacedDataSet(tx, &ds, now)
		if err != nil {
			return err
		}
		return nil
	})

	return &ds, err
}

//ExportTripToJson attempts to load tripId effective "at" a point in time and writes to destinationFile in Json format
func ExportTripToJson(log *log.Logger,
	db *sqlx.DB,
	at time.Time,
	tripId string,
	destinationFile string) error {

	const tripSearchRangeSeconds = 60 * 60 * 8
	start := at.Add(time.Duration(-tripSearchRangeSeconds) * time.Second)
	end := at.Add(time.Duration(tripSearchRangeSeconds) * time.Second)

	results, err := gtfs.GetTripInstances(db, at, start, end, []string{tripId})
	if err != nil {
		var missingTripInstancesError *gtfs.MissingTripInstances
		if errors.As(err, &missingTripInstancesError) {
			log.Printf("%s\n", err)
		}
		return err
	}
	trip, present := results[tripId]
	if !present {
		return fmt.Errorf("unable to find trip %s", tripId)
	}
	file, err := json.MarshalIndent(trip, "", " ")
	if err != nil {
		return err
	}
	log.Printf("saving trip to %s", destinationFile)
	return ioutil.WriteFile(destinationFile, file, 0644)
}

func makeDirectoryIfNotPresent(directory string) error {
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		err = os.Mkdir(directory, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
transact starts a Transaction on sqlx.DB, calls txFunc and commits or rolls back the transaction depending on the
return code of the txFunc result
*/
func transact(log *log.Logger, db *sqlx.DB, txFunc func(*sqlx.Tx) error) (err error) {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			rollbackErr := tx.Rollback() // err is non-nil; don't change it
			if rollbackErr != nil {
				log.Printf("Received error while attempting to rollback transaction. error:%v", rollbackErr)
			}
			return
		}
		err = tx.Commit() // err is nil; if Commit returns error update err
	}()
	err = txFunc(tx)
	return err
}
