package gtfs

import (
	"github.com/jmoiron/sqlx"
	"time"
)

//ObservedStopTime contains details when a vehicle is observed to have transitioned between two stops, or
//assumed to have passed a two stops based on the subsequent vehicle positions indicating it passed two or more
//stops on a trip, in which case the travel time is interpolated
// primary key consists of ObservedTime, StopId, NextStopId, VehicleId
type ObservedStopTime struct {
	//ObservedTime is the time the vehicle movement was seen
	ObservedTime time.Time `db:"observed_time"`
	//StopId is the stopId the vehicle moved from
	StopId string `db:"stop_id"`
	//NextStopId is the stopId the vehicle moved to
	NextStopId string `db:"next_stop_id"`
	VehicleId  string `db:"vehicle_id"`

	RouteId string `db:"route_id"`
	//ObservedAtStop is true when a gtfs-rt vehicle record indicated the vehicle was located at the stop the vehicle moved from
	ObservedAtStop bool `db:"observed_at_stop"`

	//ObservedAtNextStop is true when a gtfs-rt vehicle record indicated the vehicle was located at the stop the vehicle moved to
	ObservedAtNextStop bool `db:"observed_at_next_stop"`

	//TravelSeconds is the number of seconds the vehicle is assumed to have taken to move between the stops
	TravelSeconds    int  `db:"travel_seconds"`
	ScheduledSeconds *int `db:"scheduled_seconds"`
	//DataSetId identifies the DataSet used during this ObservedStopTime
	DataSetId int64     `db:"data_set_id" json:"data_set_id"`
	TripId    string    `db:"trip_id"`
	CreatedAt time.Time `db:"created_at"`
}

//AssumedDepartTime returns the time the vehicle is assumed to have departed the from stopId, this is calculated
//based on the last time the vehicle was observed at or before the from stopId
func (ost *ObservedStopTime) AssumedDepartTime() int {
	return int(ost.ObservedTime.Unix() - int64(ost.TravelSeconds))
}

// RecordObservedStopTime saves slice of ObservedStopTime into database in batch
func RecordObservedStopTime(observation *ObservedStopTime, db *sqlx.DB) error {

	observation.CreatedAt = time.Now()

	statementString := "insert into observed_stop_time " +
		"(observed_time, " +
		"stop_id, " +
		"next_stop_id, " +
		"vehicle_id, " +
		"route_id, " +
		"observed_at_stop, " +
		"observed_at_next_stop, " +
		"travel_seconds, " +
		"scheduled_seconds, " +
		"trip_id, " +
		"created_at) " +
		"values " +
		"(:observed_time, " +
		":stop_id, " +
		":next_stop_id, " +
		":vehicle_id, " +
		":route_id, " +
		":observed_at_stop, " +
		":observed_at_next_stop, " +
		":travel_seconds, " +
		":scheduled_seconds, " +
		":trip_id, " +
		":created_at)"
	statementString = db.Rebind(statementString)
	_, err := db.NamedExec(statementString, observation)
	return err
}
