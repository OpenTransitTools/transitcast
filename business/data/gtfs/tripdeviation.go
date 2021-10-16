package gtfs

import (
	"github.com/jmoiron/sqlx"
	"time"
)

type TripDeviation struct {
	Id                 int64
	CreatedAt          time.Time `db:"created_at" json:"created_at"`
	DeviationTimestamp time.Time `db:"deviation_timestamp" json:"deviation_timestamp"`
	//TripProgress is the distance of the trip that has been traversed.
	//a negative number indicates a position on a prior trip to this one
	TripProgress float64 `db:"trip_progress" json:"trip_progress"`
	//DataSetId identifies the DataSet used when this TripDeviation was calculated
	DataSetId int64  `db:"data_set_id" json:"data_set_id"`
	TripId    string `db:"trip_id" json:"trip_id"`
	VehicleId string `db:"vehicle_id" json:"vehicle_id"`
	AtStop    bool   `db:"at_stop" json:"at_stop"`
	Delay     int    `db:"delay"`
}

// RecordTripDeviation saves slice of TripDeviations into database in batch
func RecordTripDeviation(tripDeviations []*TripDeviation, db *sqlx.DB) error {
	if len(tripDeviations) == 0 {
		return nil
	}
	now := time.Now()
	for _, tripDeviation := range tripDeviations {
		tripDeviation.CreatedAt = now
	}
	statementString := "insert into trip_deviation (created_at, deviation_timestamp, " +
		"trip_progress, " +
		"data_set_id, " +
		"trip_id, " +
		"vehicle_id, " +
		"at_stop, " +
		"delay) values " +
		"(:created_at, :deviation_timestamp, " +
		":trip_progress, " +
		":data_set_id, " +
		":trip_id, " +
		":vehicle_id, " +
		":at_stop, " +
		":delay)"
	statementString = db.Rebind(statementString)
	_, err := db.NamedExec(statementString, tripDeviations)
	return err
}
