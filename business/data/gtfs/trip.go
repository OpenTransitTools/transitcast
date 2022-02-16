package gtfs

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/foundation/database"
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
	BlockId       string  `db:"block_id" json:"block_id"`
	ShapeId       string  `db:"shape_id" json:"shape_id"`
	StartTime     int     `db:"start_time" json:"start_time"`
	EndTime       int     `db:"end_time" json:"end_time"`
	TripDistance  float64 `db:"trip_distance" json:"trip_distance"`
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
		"shape_id," +
		"start_time, " +
		"end_time, " +
		"trip_distance) " +
		"values (" +
		":data_set_id, " +
		":trip_id, " +
		":route_id, " +
		":service_id, " +
		":trip_headsign, " +
		":trip_short_name, " +
		":block_id, " +
		":shape_id," +
		":start_time, " +
		":end_time, " +
		":trip_distance)"
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

// ShapesBetweenDistances returns slice of Shapes where Shape.ShapeDistTraveled is between start and end
func (t *TripInstance) ShapesBetweenDistances(start float64, end float64) []*Shape {
	results := make([]*Shape, 0)
	for _, shape := range t.Shapes {
		if shape.ShapeDistTraveled != nil {
			if *shape.ShapeDistTraveled >= start {
				if *shape.ShapeDistTraveled <= end {
					results = append(results, shape)
				} else {
					//moved past the end, no need to continue
					return results
				}
			}
		}
	}
	return results
}

//GetScheduledTripIds returns all map of trip_ids that are scheduled between relevantFrom and relevantTo
// at is used to retrieve the active dataSet
func GetScheduledTripIds(db *sqlx.DB,
	at time.Time,
	relevantFrom time.Time,
	relevantTo time.Time) (map[string]bool, error) {
	scheduleSlices := GetScheduleSlices(relevantFrom, relevantTo)

	dataSet, err := GetDataSetAt(db, at)
	if err != nil {
		return nil, err
	}
	tripIdMap := make(map[string]bool)

	for _, slice := range scheduleSlices {
		serviceIds, err := GetActiveServiceIds(db, dataSet, slice.ServiceDate)
		if err != nil {
			return nil, err
		}
		if len(serviceIds) > 0 {
			tripIds, err := getScheduledTripIdsForSlice(db, dataSet, serviceIds, slice)
			if err != nil {
				return nil, err
			}
			for _, tripId := range tripIds {
				tripIdMap[tripId] = true
			}
		}
	}
	return tripIdMap, nil
}

//getScheduledTripIdsForSlice retrieves the tripIds for dataSet for serviceIds where trip start and trip end
//fall within the range of ScheduleSlice.StartSeconds and ScheduleSlice.EndSeconds
func getScheduledTripIdsForSlice(
	db *sqlx.DB,
	dataSet *DataSet,
	serviceIds []string,
	slice ScheduleSlice) ([]string, error) {
	if len(serviceIds) < 1 {
		return nil, nil
	}
	query := "select trip_id from trip where data_set_id = :data_set_id and service_id in (:service_ids) " +
		"and ((start_time between :start_seconds and :end_seconds " +
		"or end_time between :start_seconds and :end_seconds) " +
		"or (trip.start_time < :start_seconds and trip.end_time > :end_seconds))"

	query, args, err := database.PrepareNamedQueryFromMap(query, db, map[string]interface{}{
		"data_set_id":   dataSet.Id,
		"service_ids":   serviceIds,
		"start_seconds": slice.StartSeconds,
		"end_seconds":   slice.EndSeconds,
	})

	var tripIds []string
	err = db.Select(&tripIds, query, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve trip_ids from trip table. query:%s error: %w", query, err)
	}
	return tripIds, nil
}

// GetTripInstances loads trip instances with tripIds.
// Appropriate scheduleDates are selected where trip start and end times are within range of relevantFrom and relevantTo
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
	rows, err := database.PrepareNamedQueryRowsFromMap(statementString, db, map[string]interface{}{
		"data_set_id": dataSet.Id,
		"trip_ids":    tripIds,
	})
	if err != nil {
		return nil, err
	}

	shapeIdMap := make(map[string]bool)
	shapeIds := make([]string, 0)

	// iterate over each row
	for rows.Next() {
		tripInstance := TripInstance{}
		err = rows.StructScan(&tripInstance)
		if err != nil {
			return nil, err
		}
		//collect shapeIds we need

		if _, present := shapeIdMap[tripInstance.ShapeId]; !present {
			shapeIdMap[tripInstance.ShapeId] = true
			shapeIds = append(shapeIds, tripInstance.ShapeId)
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
		if shapes, present := mappedShapes[tripInstance.ShapeId]; present {
			tripInstance.Shapes = shapes
		}

	}
	results.MissingShapeIds = missingShapeIds

	return results, err

}
