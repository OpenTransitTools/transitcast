package gtfs

import (
	"github.com/OpenTransitTools/transitcast/foundation/database"
	"github.com/jmoiron/sqlx"
	"time"
)

// StopTime contains a record from a gtfs stop_times.txt file
// represents a scheduled arrival and departure at a stop.
type StopTime struct {
	DataSetId         int64   `db:"data_set_id" json:"data_set_id"`
	TripId            string  `db:"trip_id" json:"trip_id"`
	StopSequence      uint32  `db:"stop_sequence" json:"stop_sequence"`
	StopId            string  `db:"stop_id" json:"stop_id"`
	ArrivalTime       int     `db:"arrival_time" json:"arrival_time"`
	DepartureTime     int     `db:"departure_time" json:"departure_time"`
	ShapeDistTraveled float64 `db:"shape_dist_traveled" json:"shape_dist_traveled"`
	Timepoint         int     `db:"timepoint" json:"timepoint"`
}

type StopTimeInstance struct {
	StopTime
	FirstStop         bool `json:"first_stop"`
	ArrivalDateTime   time.Time
	DepartureDateTime time.Time
}

func (sti *StopTimeInstance) IsTimepoint() bool {
	return sti != nil && sti.Timepoint == 1
}

// RecordStopTimes saves stopTimes to database in batch
func RecordStopTimes(stopTimes []*StopTime, dsTx *DataSetTransaction) error {
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
		"shape_dist_traveled," +
		"timepoint) " +
		"values (" +
		":data_set_id, " +
		":trip_id, " +
		":stop_sequence, " +
		":stop_id, " +
		":arrival_time, " +
		":departure_time," +
		":shape_dist_traveled," +
		":timepoint)"
	statementString = dsTx.Tx.Rebind(statementString)
	_, err := dsTx.Tx.NamedExec(statementString, stopTimes)
	return err
}

// GetStopTimeInstances collects StopTimeInstances and returns in order by tripID inside a map
// ArrivalDateTime and DepartureDateTime are populated from the best ScheduleSlice match from the trips first arrival time.
//If a ScheduleSlice match can't be found the StopTimeInstances are not included in the map result
// returns:
//		map with results keyed by tripId,
//		slice of missing trip ids (where no StopTimeInstances could be found)
//		slice of trip ids where no matching ScheduleSlice could be found for the trip
func GetStopTimeInstances(db *sqlx.DB,
	scheduleSlices []ScheduleSlice,
	dataSetId int64,
	tripIds []string) (map[string][]*StopTimeInstance, []string, []string, error) {

	results := make(map[string][]*StopTimeInstance)
	seenTripIds := make(map[string]bool, 0)
	missingTripIds := make([]string, 0)
	invalidTimeSliceTripIds := make([]string, 0)

	statementString := "select * from stop_time where data_set_id = :data_set_id and trip_id in (:trip_ids) " +
		"order by trip_id, stop_sequence"
	rows, err := database.PrepareNamedQueryRowsFromMap(statementString, db, map[string]interface{}{
		"data_set_id": dataSetId,
		"trip_ids":    tripIds,
	})
	if err != nil {
		return nil, missingTripIds, invalidTimeSliceTripIds, err
	}

	currentTripId := ""
	var currentScheduleSlice *ScheduleSlice
	currentStopTimes := make([]*StopTimeInstance, 0)
	for rows.Next() {
		sti := StopTimeInstance{}
		err = rows.StructScan(&sti)
		if err != nil {
			return nil, missingTripIds, invalidTimeSliceTripIds, err
		}

		// check if the current row is the start of a new trip
		if currentTripId != sti.TripId {
			//mark this stop as the first stop on trip
			sti.FirstStop = true

			//if there are items in currentStopTimes add them to the results map for the tripId
			if len(currentStopTimes) > 0 {
				results[currentTripId] = currentStopTimes
				// create new currentStopTimes
				currentStopTimes = make([]*StopTimeInstance, 0)

			}

			//set the new tripId being iterated over
			currentTripId = sti.TripId

			//add to list of tripIds that have been seen
			seenTripIds[currentTripId] = true

			//look for a schedule slice
			currentScheduleSlice = findScheduleSlice(scheduleSlices, sti.ArrivalTime)
			if currentScheduleSlice == nil {
				invalidTimeSliceTripIds = append(invalidTimeSliceTripIds, sti.TripId)
			}
		} else {
			//mark this as not the first stop
			sti.FirstStop = false
		}
		//only if we found a valid ScheduleSlice for the current trip do we calculate stop times and
		//append the StopTimeInstance to our current list
		if currentScheduleSlice != nil {
			sti.ArrivalDateTime = MakeScheduleTime(currentScheduleSlice.ServiceDate, sti.ArrivalTime)
			sti.DepartureDateTime = MakeScheduleTime(currentScheduleSlice.ServiceDate, sti.DepartureTime)
			currentStopTimes = append(currentStopTimes, &sti)
		}

	}
	//take care of last list of stop times
	if len(currentStopTimes) > 0 {
		//put the currentStopTimes times into the results
		results[currentTripId] = currentStopTimes
	}

	//find tripIds that were not found
	for _, tripId := range tripIds {
		//check in map of seen trip ids
		if _, tripIdPresent := seenTripIds[tripId]; !tripIdPresent {
			missingTripIds = append(missingTripIds, tripId)
		}
	}

	return results, missingTripIds, invalidTimeSliceTripIds, err
}
