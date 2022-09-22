package gtfs

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/foundation/database"
	"github.com/jmoiron/sqlx"
	"strings"
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

func (t *TripInstance) FirstStopTimeInstance() *StopTimeInstance {
	if len(t.StopTimeInstances) == 0 {
		return nil
	}
	return t.StopTimeInstances[0]
}

func (t *TripInstance) LastStopTimeInstance() *StopTimeInstance {
	lastIndex := len(t.StopTimeInstances) - 1
	if lastIndex < 0 {
		return nil
	}
	return t.StopTimeInstances[lastIndex]
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

type MissingTripInstances struct {
	DataSetId               int64
	MissingTripIds          []string
	ScheduleSliceOutOfRange []string
	MissingShapeIds         []string
}

func (m *MissingTripInstances) Error() string {

	return fmt.Sprintf("tripids not found or out of range for dataSetId:%d missingTrips:[%s], "+
		"missingScheduleSlice:[%s], missingShapeIds:[%s]",
		m.DataSetId,
		strings.Join(m.MissingTripIds, ","),
		strings.Join(m.ScheduleSliceOutOfRange, ","),
		strings.Join(m.MissingShapeIds, ","))

}

// GetTripInstances loads trip instances with tripIds.
// Appropriate scheduleDates are selected where trip start and end times are within range of relevantFrom and relevantTo
// if any tripIds could not be loaded error will be of MissingTripInstances, in which case its safe to continue if those
// trips are not needed, but the error should be logged
func GetTripInstances(db *sqlx.DB,
	at time.Time,
	relevantFrom time.Time,
	relevantTo time.Time,
	tripIds []string) (map[string]*TripInstance, error) {

	//find dataSet that's relevant
	dataSet, err := GetDataSetAt(db, at)
	if err != nil {
		return nil, err
	}

	//find relevant schedule slices
	scheduleSlices := GetScheduleSlices(relevantFrom, relevantTo)

	//load all stopTimes for requested tripIds
	stopTimeMap, missingTripIds, tripIdsScheduleSliceOutOfRange, err :=
		getStopTimeInstances(db, scheduleSlices, dataSet.Id, tripIds)

	if err != nil {
		return nil, err
	}

	//if some tripIds couldn't be found remove them from the requested tripIds
	if len(missingTripIds) > 0 || len(tripIdsScheduleSliceOutOfRange) > 0 {
		//remove missing tripIds from request
		tripIds = removeStringsFromSlice(tripIds, missingTripIds)
		tripIds = removeStringsFromSlice(tripIds, tripIdsScheduleSliceOutOfRange)
		//no more tripIds to load, just return error, as there are no results
		if len(tripIds) == 0 {
			return nil, &MissingTripInstances{
				DataSetId:               dataSet.Id,
				MissingTripIds:          missingTripIds,
				ScheduleSliceOutOfRange: tripIdsScheduleSliceOutOfRange,
				MissingShapeIds:         nil,
			}
		}
	}

	//load tripInstances with stopTimeMap
	var tripInstanceByTripId map[string]*TripInstance
	tripInstanceByTripId, err = getTripInstances(db, tripIds, dataSet, stopTimeMap)

	if err != nil {
		return nil, err
	}

	//load any shape list available into trips
	var missingShapeIds []string
	missingShapeIds, err = loadShapesIntoTrips(tripInstanceByTripId, db, dataSet)

	if err != nil {
		return nil, err
	}

	//only return missingTripInstancesError if its non-null
	if len(missingTripIds) > 0 || len(tripIdsScheduleSliceOutOfRange) > 0 || len(missingShapeIds) > 0 {
		return tripInstanceByTripId, &MissingTripInstances{
			DataSetId:               dataSet.Id,
			MissingTripIds:          missingTripIds,
			ScheduleSliceOutOfRange: tripIdsScheduleSliceOutOfRange,
			MissingShapeIds:         missingShapeIds,
		}
	}

	return tripInstanceByTripId, nil

}

func getTripInstances(db *sqlx.DB,
	tripIds []string,
	dataSet *DataSet,
	stopTimeMap map[string][]*StopTimeInstance) (map[string]*TripInstance, error) {

	results := make(map[string]*TripInstance)

	statementString := "select * from trip where data_set_id = :data_set_id and trip_id in (:trip_ids)"
	rows, err := database.PrepareNamedQueryRowsFromMap(statementString, db, map[string]interface{}{
		"data_set_id": dataSet.Id,
		"trip_ids":    tripIds,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	// iterate over each row
	for rows.Next() {
		tripInstance, err := loadTripInstanceRows(rows, stopTimeMap)
		if err != nil {
			return nil, err
		}

		results[tripInstance.TripId] = tripInstance
	}

	// check the error from rows
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return results, nil
}

func loadShapesIntoTrips(tripsByTripId map[string]*TripInstance,
	db *sqlx.DB,
	dataSet *DataSet) ([]string, error) {

	//find shapeIds needed
	shapeIdMap := make(map[string]bool)
	shapeIds := make([]string, 0)
	for _, tripInstance := range tripsByTripId {
		if _, present := shapeIdMap[tripInstance.ShapeId]; !present {
			shapeIdMap[tripInstance.ShapeId] = true
			shapeIds = append(shapeIds, tripInstance.ShapeId)
		}
	}

	//load shapes
	mappedShapes, missingShapeIds, err := GetShapes(db, dataSet.Id, shapeIds)
	if err != nil {
		return missingShapeIds, err
	}

	for _, tripInstance := range tripsByTripId {
		if shapes, present := mappedShapes[tripInstance.ShapeId]; present {
			tripInstance.Shapes = shapes
		}

	}
	return missingShapeIds, nil
}

func removeStringsFromSlice(target []string, toRemove []string) []string {
	removeMap := make(map[string]bool)
	for _, s := range toRemove {
		removeMap[s] = true
	}
	var newSlice []string
	for _, s := range target {
		if _, exists := removeMap[s]; !exists {
			newSlice = append(newSlice, s)
		}
	}
	return newSlice
}

func GetTripInstance(db *sqlx.DB,
	dataSetId int64,
	tripId string,
	at time.Time,
	tripSearchRangeSeconds int) (*TripInstance, error) {
	scheduleSlices := GetScheduleSlicesForSearchRange(at, tripSearchRangeSeconds)

	stopTimeMap, _, _, err := getStopTimeInstances(db, scheduleSlices, dataSetId, []string{tripId})

	if err != nil {
		return nil, err
	}

	statementString := "select * from trip where data_set_id = :data_set_id and trip_id = :trip_id"
	rows, err := database.PrepareNamedQueryRowsFromMap(statementString, db, map[string]interface{}{
		"data_set_id": dataSetId,
		"trip_id":     tripId,
	})
	defer func() {
		if rows != nil {
			_ = rows.Close()
		}
	}()
	if err != nil {
		return nil, err
	}

	var tripInstance *TripInstance
	if rows.Next() {
		tripInstance, err = loadTripInstanceRows(rows, stopTimeMap)

		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("unable to find trip for dataSet id: %d, tripId: %s at %v", dataSetId, tripId, at)
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}
	// check the error from rows
	err = rows.Err()

	return tripInstance, err
}

func loadTripInstanceRows(rows *sqlx.Rows,
	stopTimeMap map[string][]*StopTimeInstance) (*TripInstance, error) {
	tripInstance := TripInstance{}
	err := rows.StructScan(&tripInstance)
	if err != nil {
		return nil, err
	}
	//collect stopTimes we needed
	stopTimes, present := stopTimeMap[tripInstance.TripId]
	if present {
		tripInstance.StopTimeInstances = stopTimes
	} else {
		return nil, fmt.Errorf("found no scheduled stops in dataSet id: %d, tripId: %s",
			tripInstance.DataSetId, tripInstance.TripId)
	}
	return &tripInstance, nil

}
