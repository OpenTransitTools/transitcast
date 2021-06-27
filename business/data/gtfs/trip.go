package gtfs

// Trip contains data from a gtfs trip definition in a trips.txt file
type Trip struct {
	DataSetId     int64   `db:"data_set_id"`
	TripId        string  `db:"trip_id"`
	RouteId       string  `db:"route_id"`
	ServiceId     string  `db:"service_id"`
	TripHeadsign  *string `db:"trip_headsign"`
	TripShortName *string `db:"trip_short_name"`
	BlockId       *string `db:"block_id"`
}

func RecordTrips(trips []*Trip, dsTx *DataSetTransaction) error {
	for _, trip := range trips {
		trip.DataSetId = dsTx.DS.Id
	}
	statementString := "insert into trip ( " +
		"data_set_id, " +
		"trip_id, " +
		"route_id, " +
		"service_id, " +
		"trip_headsign, " +
		"trip_short_name, " +
		"block_id) " +
		"values (" +
		":data_set_id, " +
		":trip_id, " +
		":route_id, " +
		":service_id, " +
		":trip_headsign, " +
		":trip_short_name," +
		":block_id)"
	statementString = dsTx.Tx.Rebind(statementString)
	_, err := dsTx.Tx.NamedExec(statementString, trips)
	return err

}
