package gtfs

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/foundation/database"
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
	RouteId   string `db:"-" json:"route_id"`
}

// SchedulePosition returns the schedule position (where the vehicle is according to its schedule) of the vehicle
// derived from Delay and DeviationTimestamp
func (t *TripDeviation) SchedulePosition() time.Time {
	return t.DeviationTimestamp.Add(time.Duration(-t.Delay) * time.Second)
}

// RecordTripDeviation saves slice of TripDeviations into database in batch
func RecordTripDeviation(tripDeviations []*TripDeviation, db *sqlx.DB) error {
	if len(tripDeviations) == 0 {
		return nil
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

// GetTripDeviations returns list of TripDeviations between start and end for vehicleId
func GetTripDeviations(db *sqlx.DB,
	start time.Time,
	end time.Time,
	vehicleId string) ([]*TripDeviation, error) {
	statementString := "select * from trip_deviation where created_at between :start and :end " +
		" and vehicle_id = :vehicle_id " +
		"order by created_at"
	rows, err := database.PrepareNamedQueryRowsFromMap(statementString, db, map[string]interface{}{
		"start":      start,
		"end":        end,
		"vehicle_id": vehicleId,
	})

	defer func() {
		if rows != nil {
			_ = rows.Close()
		}
	}()

	if err != nil {
		return nil, fmt.Errorf("unable to retrieve trip_deviation rows, error: %w", err)
	}

	tripDeviations := make([]*TripDeviation, 0)
	for rows.Next() {
		tripDeviation := TripDeviation{}
		err = rows.StructScan(&tripDeviation)
		tripDeviations = append(tripDeviations, &tripDeviation)
	}
	return tripDeviations, err
}
