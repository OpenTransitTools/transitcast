package gtfs

// StopTime contains a record from a gtfs stop_times.txt file
// represents a scheduled arrival and departure at a stop.
type StopTime struct {
	DataSetId         int64    `db:"data_set_id"`
	TripId            string   `db:"trip_id"`
	StopSequence      int      `db:"stop_sequence"`
	StopId            string   `db:"stop_id"`
	ArrivalTime       int      `db:"arrival_time"`
	DepartureTime     int      `db:"departure_time"`
	ShapeDistTraveled *float64 `db:"shape_dist_traveled"`
}

func RecordStopTime(stopTimes []*StopTime, dsTx *DataSetTransaction) error {
	for _, stopTime := range stopTimes {
		stopTime.DataSetId = dsTx.DS.Id
	}

	statementString := "insert into stop_time ( " +
		"data_set_id, " +
		"trip_id, " +
		"stop_sequence, " +
		"stop_id, " +
		"arrival_time, " +
		"departure_time, " +
		"shape_dist_traveled) " +
		"values (" +
		":data_set_id, " +
		":trip_id, " +
		":stop_sequence, " +
		":stop_id, " +
		":arrival_time, " +
		":departure_time," +
		":shape_dist_traveled)"
	statementString = dsTx.Tx.Rebind(statementString)
	_, err := dsTx.Tx.NamedExec(statementString, stopTimes)
	return err
}
