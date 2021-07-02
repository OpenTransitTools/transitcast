package gtfs

import "time"

//ObservedStopTime contains details when a vehicle is observed to have transitioned between two stops, or
//assumed to have passed a two stops based on the subsequent vehicle positions indicating it passed two or more
//stops on a trip, in which case the travel time is interpolated
type ObservedStopTime struct {
	Id      int64
	RouteId string `db:"route_id"`
	//StopId is the stopId the vehicle moved from
	StopId string `db:"stop_id"`
	//ObservedAtStop is true when a gtfs-rt vehicle record indicated the vehicle was located at the stop the vehicle moved from
	ObservedAtStop bool `db:"observed_at_stop"`
	//NextStopId is the stopId the vehicle moved to
	NextStopId string `db:"next_stop_id"`
	//ObservedAtNextStop is true when a gtfs-rt vehicle record indicated the vehicle was located at the stop the vehicle moved to
	ObservedAtNextStop bool `db:"observed_at_next_stop"`
	//ObservedTime is the time the vehicle movement was seen
	ObservedTime time.Time `db:"observed_time"`
	//TravelSeconds is the number of seconds the vehicle is assumed to have taken to move between the stops
	TravelSeconds int64     `db:"travel_seconds"`
	VehicleId     string    `db:"vehicle_id"`
	TripId        string    `db:"trip_id"`
	CreatedAt     time.Time `db:"create_at"`
}

//AssumedDepartTime returns the time the vehicle is assumed to have departed the from stopId, this is calculated
//based on the last time the vehicle was observed at or before the from stopId
func (ost *ObservedStopTime) AssumedDepartTime() int64 {
	return ost.ObservedTime.Unix() - ost.TravelSeconds
}
