package gtfs

import (
	"github.com/jmoiron/sqlx"
	"time"
)

// Trip contains data from a gtfs trip definition in a trips.txt file
type Trip struct {
	DataSetId     int64   `db:"data_set_id" json:"data_set_id"`
	TripId        string  `db:"trip_id" json:"trip_id"`
	RouteId       string  `db:"route_id" json:"route_id"`
	ServiceId     string  `db:"service_id" json:"service_id"`
	TripHeadsign  *string `db:"trip_headsign" json:"trip_headsign"`
	TripShortName *string `db:"trip_short_name" json:"trip_short_name"`
	BlockId       *string `db:"block_id" json:"block_id"`
	ShapeId       *string `db:"shape_id" json:"shape_id"`
}

// RecordTrips saves trips to database in batch
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
		"block_id, " +
		"shape_id) " +
		"values (" +
		":data_set_id, " +
		":trip_id, " +
		":route_id, " +
		":service_id, " +
		":trip_headsign, " +
		":trip_short_name, " +
		":block_id, " +
		":shape_id)"
	statementString = dsTx.Tx.Rebind(statementString)
	_, err := dsTx.Tx.NamedExec(statementString, trips)
	return err

}

// TripInstanceBatchQueryResults provides results from batch querying trips
// tripIds that were not found (MissingTripIds) or where the schedule time was outside of date range (ScheduleSliceOutOfRange) can be logged
type TripInstanceBatchQueryResults struct {
	TripInstancesByTripId   map[string]*TripInstance
	MissingTripIds          []string
	ScheduleSliceOutOfRange []string
	MissingShapeIds         []string
}

func makeTripInstanceBatchQueryResults() *TripInstanceBatchQueryResults {
	return &TripInstanceBatchQueryResults{
		TripInstancesByTripId: make(map[string]*TripInstance),
	}
}

type TripInstance struct {
	Trip
	StopTimeInstances []*StopTimeInstance `json:"stop_time_instances"`
	Shapes            []*Shape            `json:"shapes"`
}

func GetTripInstances(db *sqlx.DB,
	at time.Time,
	relevantFrom time.Time,
	relevantTo time.Time,
	tripIds []string) (*TripInstanceBatchQueryResults, error) {

	dataSet, err := GetDataSetAt(db, at)
	if err != nil {
		return nil, err
	}

	scheduleSlices := GetScheduleSlices(relevantFrom, relevantTo)

	stopTimeMap, missingTripIds, scheduleSliceOutOfRange, err := GetStopTimeInstances(db, scheduleSlices, dataSet.Id, tripIds)

	if err != nil {
		return nil, err
	}

	results := makeTripInstanceBatchQueryResults()
	results.MissingTripIds = missingTripIds
	results.ScheduleSliceOutOfRange = scheduleSliceOutOfRange

	statementString := "select * from trip where data_set_id = :data_set_id and trip_id in (:trip_ids)"
	rows, err := prepareNamedQueryRowsFromMap(statementString, db, map[string]interface{}{
		"data_set_id": dataSet.Id,
		"trip_ids":    tripIds,
	})
	if err != nil {
		return nil, err
	}

	shapeIds := make([]string, 0)

	// iterate over each row
	for rows.Next() {
		tripInstance := TripInstance{}
		err = rows.StructScan(&tripInstance)
		if err != nil {
			return nil, err
		}
		//collect shapeIds we need
		if tripInstance.ShapeId != nil {
			shapeIds = append(shapeIds, *tripInstance.ShapeId)
		}
		if stopTimes, present := stopTimeMap[tripInstance.TripId]; present {
			tripInstance.StopTimeInstances = stopTimes
			results.TripInstancesByTripId[tripInstance.TripId] = &tripInstance
		}
	}

	// check the error from rows
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	//load shapes
	mappedShapes, missingShapeIds, err := GetShapes(db, dataSet.Id, shapeIds)
	if err != nil {
		return nil, err
	}

	//load any shape lists available into trips
	for _, tripInstance := range results.TripInstancesByTripId {
		if tripInstance.ShapeId != nil {
			if shapes, present := mappedShapes[*tripInstance.ShapeId]; present {
				tripInstance.Shapes = shapes
			}
		}
	}
	results.MissingShapeIds = missingShapeIds

	return results, err

}
