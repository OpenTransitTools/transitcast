package monitor

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/jmoiron/sqlx"
	"log"
	"sort"
	"time"
)

//tripStopPosition is used by vehicleMonitor to keep track of vehicle movement between updated positions
type tripStopPosition struct {
	dataSetId int64

	vehicleId string

	//atPreviousStop is true when vehicle position was set to StoppedAt for previousSTI
	atPreviousStop bool

	//witnessedPreviousStop indicates that we have seen the vehicle at or prior to previousSTI
	witnessedPreviousStop bool

	//tripInstance is always populated from the vehiclePosition's tripId
	tripInstance *gtfs.TripInstance

	//previousSTI is the stop this trip that we are at or just passed
	previousSTI *gtfs.StopTimeInstance

	//nextSTI is the stop this trip that we are headed towards (or at in the case where we are at the last stop of the trip)
	nextSTI *gtfs.StopTimeInstance

	//lastTimestamp the timestamp of the vehiclePosition this tripStopPosition was created from
	lastTimestamp int64

	//latitude optionally included if present in vehiclePosition
	latitude *float32

	//longitude optionally included if present in vehiclePosition
	longitude *float32

	//how delayed the vehicle is. Positive is late. Negative is early
	delay int

	//tripDistancePosition is present if vehicle's distance on the trip was could be found
	tripDistancePosition *float64

	//scheduledSecondsFromLastStop is number of seconds vehicle was found beyond the previousSTI based on tripDistancePosition
	//if tripDistancePosition was unavailable will have default value of zero
	scheduledSecondsFromLastStop int

	//observedSecondsToTravelToPosition is number of seconds is assumed to have taken to move to scheduledSecondsFromLastStop
	//if tripDistancePosition was unavailable will have default value of zero
	observedSecondsToTravelToPosition int
}

//logFormat simple format for logging a tripStopPosition
func (t *tripStopPosition) logFormat() string {
	var lat float32
	if t.latitude != nil {
		lat = *t.latitude
	}
	var lon float32
	if t.longitude != nil {
		lon = *t.longitude
	}

	return fmt.Sprintf("tripStopPosition{ tripId:%s, previousStop:(seq:%d id:%s secs:%d), nextStop:(seq:%d id:%s secs:%d), atPrevious:%t, latlng:%f,%f }",
		t.tripInstance.TripId, t.previousSTI.StopSequence, t.previousSTI.StopId, t.previousSTI.ArrivalTime,
		t.nextSTI.StopSequence, t.nextSTI.StopId, t.nextSTI.ArrivalTime,
		t.atPreviousStop, lat, lon)
}

//recordNewTripDeviations create and record gtfs.TripDeviations from new newPositionsByBlock
func recordNewTripDeviations(log *log.Logger,
	db *sqlx.DB,
	loadedTripInstancesByTripId map[string]*gtfs.TripInstance,
	newPositionsByBlock map[string]*tripStopPosition) {

	newTripDeviations := collectTripDeviations(loadedTripInstancesByTripId, newPositionsByBlock)
	err := gtfs.RecordTripDeviation(newTripDeviations, db)
	if err != nil {
		log.Printf("failed to record %d trip deviations, error:%v", len(newTripDeviations), err)
		return
	}

	log.Printf("Recorded %d trip deviation records for %d new positions",
		len(newTripDeviations), len(newPositionsByBlock))

}

//collectTripDeviations takes positions found in newPositionsByBlock and creates gtfs.TripDeviation for each trip
//the block is currently on or scheduled in the future as found in loadedTripInstancesByTripId
func collectTripDeviations(
	loadedTripInstancesByTripId map[string]*gtfs.TripInstance,
	newPositionsByBlock map[string]*tripStopPosition) []*gtfs.TripDeviation {

	//destination for trips by blockId
	tripListsByBlock := make(map[string][]*gtfs.TripInstance)

	//for each loaded gtfs.TripInstance see if a new position is present for the blockId and add them to tripListsByBlock
	for _, tripInstance := range loadedTripInstancesByTripId {
		position := newPositionsByBlock[tripInstance.BlockId]
		// only store trips ahead of the one the vehicle is performing
		if position != nil && position.tripInstance.StartTime < tripInstance.StartTime {
			blockTrips := tripListsByBlock[tripInstance.BlockId]
			tripListsByBlock[tripInstance.BlockId] = append(blockTrips, tripInstance)
		}
	}
	results := make([]*gtfs.TripDeviation, 0)

	// create TripDeviations for every block, adding untraveled distance of previous trips to next TripDeviation
	for blockId, position := range newPositionsByBlock {
		if position.tripDistancePosition == nil {
			continue
		}
		results = append(results, makeTripDeviation(position, *position.tripDistancePosition, position.tripInstance))
		futureTrips := tripListsByBlock[blockId]
		//sort them
		sort.Slice(futureTrips, func(i, j int) bool {
			return futureTrips[i].StartTime < futureTrips[j].StartTime
		})
		distanceToNextTrip := position.tripInstance.TripDistance - *position.tripDistancePosition
		for _, futureTrip := range futureTrips {
			results = append(results, makeTripDeviation(position, -distanceToNextTrip, futureTrip))
			distanceToNextTrip += position.tripInstance.TripDistance - *position.tripDistancePosition
		}
	}
	return results
}

//makeTripDeviation creates new gtfs.TripDeviation for trip
func makeTripDeviation(
	position *tripStopPosition,
	tripProgress float64,
	trip *gtfs.TripInstance) *gtfs.TripDeviation {
	return &gtfs.TripDeviation{
		DeviationTimestamp: time.Unix(position.lastTimestamp, 0),
		TripProgress:       tripProgress,
		DataSetId:          position.dataSetId,
		TripId:             trip.TripId,
		VehicleId:          position.vehicleId,
		AtStop:             position.atPreviousStop,
		Delay:              position.delay,
	}
}
