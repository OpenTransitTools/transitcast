package monitor

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"log"
	"math"
	"time"
)

//vehicleMonitorCollection simple wrapper for retrieving, constructing, and expiring old vehicleMonitors
type vehicleMonitorCollection struct {
	vehicles              map[string]*vehicleMonitor
	earlyTolerance        float64
	expirePositionSeconds int64 //int64 so no need to convert it when comparing int64 timestamps
}

func newVehicleMonitorCollection(earlyTolerance float64, expirePositionSeconds int) vehicleMonitorCollection {
	return vehicleMonitorCollection{
		vehicles:              make(map[string]*vehicleMonitor),
		earlyTolerance:        earlyTolerance,
		expirePositionSeconds: int64(expirePositionSeconds),
	}
}

func (vc *vehicleMonitorCollection) getOrMakeVehicle(vehicleId string) *vehicleMonitor {
	if monitor, present := vc.vehicles[vehicleId]; present {
		return monitor
	}
	vehicleMonitor := makeVehicleMonitor(vehicleId, vc.earlyTolerance, vc.expirePositionSeconds)
	vc.vehicles[vehicleId] = &vehicleMonitor
	return &vehicleMonitor
}

//tripStopPosition is used by vehicleMonitor to keep track of vehicle movement between updated positions
type tripStopPosition struct {
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

	//tripDistancePosition is present if vehicle's distance on the trip was able to be found
	tripDistancePosition *float64
	//scheduledSecondsFromLastStop is number of seconds vehicle was found beyond the previousSTI based on tripDistancePosition
	//if tripDistancePosition was unavailable will have default value of zero
	scheduledSecondsFromLastStop int
	//observedSecondsToTravelToPosition is number of seconds is assumed to have taken to move to scheduledSecondsFromLastStop
	//if tripDistancePosition was unavailable will have default value of zero
	observedSecondsToTravelToPosition int
}

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

//vehicleMonitor generates gtfs.ObservedStopTime records by watching subsequent vehiclePosition records from gtfs
type vehicleMonitor struct {
	Id                    string
	lastTripStopPosition  *tripStopPosition
	lastPositionTimestamp int64
	//earlyTolerance a percentage (should be between 0.0 and 1.0) of how early the vehicle can be observed to have traveled between two stops
	//before and gtfs.ObservedStopTime is assumed to be invalid and shouldn't be returned.
	//for example if a vehicle is observed to travel between two stops in 10 seconds, but the scheduled to take 100 seconds
	//an earlyTolerance of 0.1 or lower would allow that observation to generate an gtfs.ObservedStopTime since the vehicle
	//appears to have only taken 10 percent of the time its scheduled to travel between the stops
	//an earlyTolerance of 0.1 or higher would cause that observation to be discarded as invalid or unlikely
	earlyTolerance float64
	//expirePositionSeconds is how old a previous vehicle position is in seconds before it will not be used
	//to generate gtfs.ObservedStopTime
	expirePositionSeconds int64 //int64 so no need to convert it when comparing int64 timestamps
}

func makeVehicleMonitor(Id string, earlyTolerance float64, expirePositionSeconds int64) vehicleMonitor {
	return vehicleMonitor{Id: Id,
		earlyTolerance:        earlyTolerance,
		expirePositionSeconds: expirePositionSeconds}
}

//newPosition takes a vehiclePosition and optionally a gtfs.TripInstance and generates arrivalDelayResult and gtfs.ObservedStopTime records
//based on previous positions
//if trip is nil the vehicles trip is assumed to be unavailable from the gtfs schedule and its position is invalidated
//this method is the currently the only intended entry point to use a vehicleMonitor
func (vm *vehicleMonitor) newPosition(log *log.Logger, position *vehiclePosition, trip *gtfs.TripInstance) []gtfs.ObservedStopTime {
	var results []gtfs.ObservedStopTime
	if position.Timestamp <= vm.lastPositionTimestamp {
		return results
	}
	if position.TripId == nil || position.StopId == nil || position.StopSequence == nil || position.VehicleStopStatus.IsUnknown() {
		//non trip monitoring not implemented yet
		vm.removeStopPosition(position.Timestamp)
		return results
	}

	if trip == nil {
		log.Printf("missing tripId %s\n", *position.TripId)
		//non trip monitoring not implemented yet
		return results
	}

	newTripStopPosition, err := getTripStopPosition(trip, vm.lastTripStopPosition, position)
	if err != nil {
		log.Printf("Unable to create TripStopPosition. error: %v\n", err)
		vm.removeStopPosition(position.Timestamp)
		return results
	}

	lastTripStopPosition := vm.lastTripStopPosition
	lastPositionTimestamp := vm.lastPositionTimestamp

	if !vm.newTripStopPosition(newTripStopPosition, position.Timestamp) {
		return results
	}

	stopTimePairs, err := getStopPairsBetweenPositions(lastTripStopPosition, newTripStopPosition)
	if err != nil {
		log.Printf("error finding stop positions. error:%v\n", err)
		return results
	}
	validMovement, totalScheduleTime, took := isMovementBelievable(stopTimePairs, lastPositionTimestamp,
		position.Timestamp, vm.earlyTolerance)
	if !validMovement {

		log.Printf("Discarding trip movement as it doesn't appear valid. vehicle:%s totalScheduleTime:%d took:%d "+
			"last %s next %s",
			vm.Id, totalScheduleTime, took, lastTripStopPosition.logFormat(), newTripStopPosition.logFormat())
		vm.removeStopPosition(position.Timestamp)
		return results
	}

	results = makeObservedStopTimes(vm.Id, lastPositionTimestamp, position.Timestamp,
		lastTripStopPosition, newTripStopPosition, stopTimePairs)

	return results
}

//witnessedPreviousStop returns true if the previous tripStopPosition is before or at the stop on tripId at previousStopSequence
//indicating that the vehicle was seen at ore previous to the last stop
func witnessedPreviousStop(tripId string, stopSequence uint32, previousTripStopPosition *tripStopPosition) bool {
	if previousTripStopPosition == nil {
		return false
	}
	if previousTripStopPosition.tripInstance.TripId != tripId {
		return true
	}
	if previousTripStopPosition.previousSTI.StopSequence < stopSequence {
		return true
	}
	if previousTripStopPosition.previousSTI.StopSequence == stopSequence && previousTripStopPosition.atPreviousStop {
		return true
	}
	return false
}

//getTripStopPosition builds a tripStopPosition
func getTripStopPosition(trip *gtfs.TripInstance, previousTripStopPosition *tripStopPosition, position *vehiclePosition) (*tripStopPosition, error) {

	witnessedPrevious := witnessedPreviousStop(trip.TripId, *position.StopSequence, previousTripStopPosition)
	var previousIndex int
	var previousSST *gtfs.StopTimeInstance
	for index, sst := range trip.StopTimeInstances {
		if sst.StopSequence == *position.StopSequence {
			//move backwards one if its still in transit to the stop
			if position.VehicleStopStatus == InTransitTo && previousSST != nil {
				index = previousIndex
				sst = previousSST
			}

			//if the current stop sequence is the final stop the next stop is the same stop
			nextSTI := sst
			//otherwise get the next one
			if index+1 < len(trip.StopTimeInstances) {
				nextSTI = trip.StopTimeInstances[index+1]
			}
			result := tripStopPosition{
				atPreviousStop:        position.VehicleStopStatus == StoppedAt,
				witnessedPreviousStop: witnessedPrevious || position.VehicleStopStatus == StoppedAt,
				tripInstance:          trip,
				previousSTI:           sst,
				nextSTI:               nextSTI,
				lastTimestamp:         position.Timestamp,
				latitude:              position.Latitude,
				longitude:             position.Longitude,
			}
			return &result, nil
		}
		previousIndex = index
		previousSST = sst
	}
	return nil, fmt.Errorf("missing stop at tripId:%s previousStopSequence:%d", *position.TripId, *position.StopSequence)
}

//calculateTravelBetweenStops calculates the time a vehicle may have took to travel from previousTripStopPosition
//to its new location between position.previousSTI and position.nextSTI
//returns:
//the amount of schedule seconds the vehicle was given to travel to its position between stops
//observedSecondsToTravelToPosition - the amount of time the vehicle may have spent traveling to this position given
// how much time it spent traveling from its previous tripStopPosition
func calculateTravelBetweenStops(previousTripStopPosition *tripStopPosition, position *tripStopPosition) (int, int) {
	//don't perform calculation if previousTripStopPosition is nil
	//or position.tripDistancePosition is nil,
	//or either StopTimeInstance.ShapeDistTraveled are null
	if previousTripStopPosition == nil ||
		position.tripDistancePosition == nil ||
		position.previousSTI.ShapeDistTraveled == nil ||
		position.nextSTI.ShapeDistTraveled == nil {
		return 0, 0
	}
	firstScheduleSeconds := previousTripStopPosition.previousSTI.ArrivalTime + previousTripStopPosition.scheduledSecondsFromLastStop
	lastScheduleSeconds := position.previousSTI.ArrivalTime
	totalScheduledLengthTraveled := lastScheduleSeconds - firstScheduleSeconds

	totalTimeOfTravel := int(position.lastTimestamp - previousTripStopPosition.lastTimestamp)

	distanceFromPreviousStop := *position.tripDistancePosition - *position.previousSTI.ShapeDistTraveled
	distanceBetweenStops := *position.nextSTI.ShapeDistTraveled - *position.previousSTI.ShapeDistTraveled
	//don't proceed if the data doesn't make sense
	if distanceBetweenStops <= 0 {
		return 0, 0
	}
	//if distance traveled on the trip is greater than the distance between stops, revert to distance between the stops
	if distanceFromPreviousStop > distanceBetweenStops {
		distanceFromPreviousStop = distanceBetweenStops
	}
	percentBetweenStops := distanceFromPreviousStop / distanceBetweenStops
	//scheduleTimeBetweenStops = nextStop.scheduledArrivalTime - previousStop.scheduledDepartureTime
	scheduleTimeBetweenStops := position.nextSTI.ArrivalTime - position.previousSTI.DepartureTime

	scheduledSecondsFromLastStop := int(math.Round(float64(scheduleTimeBetweenStops) * percentBetweenStops))

	//add how far the vehicle moved past the stop to the total scheduled length traveled
	totalScheduledLengthTraveled += scheduledSecondsFromLastStop

	if totalScheduledLengthTraveled <= 0 {
		return 0, 0
	}
	percentSpentOnTravelPastStop := float64(scheduledSecondsFromLastStop) / float64(totalScheduledLengthTraveled)
	return scheduledSecondsFromLastStop, int(math.Round(float64(totalTimeOfTravel) * percentSpentOnTravelPastStop))

}

//shouldUseToMoveForward  returns true if the newPosition indicates movement from previousTripStopPosition
func shouldUseToMoveForward(previousTripStopPosition *tripStopPosition, newPosition *tripStopPosition) bool {
	if previousTripStopPosition.tripInstance.TripId != newPosition.tripInstance.TripId {
		return true
	}
	if newPosition.previousSTI.StopSequence > previousTripStopPosition.previousSTI.StopSequence {
		return true
	}
	if previousTripStopPosition.previousSTI.StopSequence == newPosition.previousSTI.StopSequence {
		if !previousTripStopPosition.atPreviousStop && newPosition.atPreviousStop {
			return true
		}
	}

	return false
}

//updateStoppedAtPosition checks if two tripStopPositions are at the same stop
//and returns true if the new position should cause an update to the monitored vehicle position
//Currently new positions at the first stop of the trip is considered new and usable, others are not
func updateStoppedAtPosition(previousTripStopPosition *tripStopPosition, newPosition *tripStopPosition) bool {
	if previousTripStopPosition.previousSTI.StopSequence == newPosition.previousSTI.StopSequence {
		if newPosition.atPreviousStop {
			return newPosition.previousSTI.FirstStop
		}
	}
	return false
}

//isCurrentPositionExpired returns true if the current position is expired at currentTimestamp
func (vm *vehicleMonitor) isCurrentPositionExpired(currentTimestamp int64) bool {
	diff := currentTimestamp - vm.lastTripStopPosition.lastTimestamp
	return diff > vm.expirePositionSeconds
}

//getObservedAtPositions convenience function returns the tripStopPosition arguments that have had their atPreviousStop flag set
func getObservedAtPositions(position1 *tripStopPosition, position2 *tripStopPosition) []tripStopPosition {
	result := make([]tripStopPosition, 0)
	if position1.atPreviousStop {
		result = append(result, *position1)
	}
	if position2.atPreviousStop {
		result = append(result, *position2)
	}
	return result
}

//newTripStopPosition updates trip position if needed
//returns true if the vehicle has moved forward from its previous position
//or false if the current position has just been updated
func (vm *vehicleMonitor) newTripStopPosition(
	newPosition *tripStopPosition,
	positionTimestamp int64) bool {

	//if last position is expired or not set then set it
	if vm.lastTripStopPosition == nil || vm.isCurrentPositionExpired(positionTimestamp) {
		vm.updateTripStopPosition(newPosition, positionTimestamp)
		return false
	}

	movedForward := shouldUseToMoveForward(vm.lastTripStopPosition, newPosition)
	if movedForward || updateStoppedAtPosition(vm.lastTripStopPosition, newPosition) {
		vm.updateTripStopPosition(newPosition, positionTimestamp)
	}
	return movedForward
}

//updateTripStopPosition sets vehicleMonitors current position to newTripStopPosition at positionTimestamp
func (vm *vehicleMonitor) updateTripStopPosition(
	newTripStopPosition *tripStopPosition,
	positionTimestamp int64) {
	//perform gps based calculations on new position
	newTripStopPosition.tripDistancePosition = findTripDistanceOfVehicleFromPosition(newTripStopPosition)
	//next populate between stop attributes of newTripStopPosition if possible
	newTripStopPosition.scheduledSecondsFromLastStop, newTripStopPosition.observedSecondsToTravelToPosition =
		calculateTravelBetweenStops(vm.lastTripStopPosition, newTripStopPosition)

	vm.lastTripStopPosition = newTripStopPosition
	vm.lastPositionTimestamp = positionTimestamp

}

//removeStopPosition removes lastTripStopPosition and sets lastPositionTimestamp to the timestamp
func (vm *vehicleMonitor) removeStopPosition(timestamp int64) {
	vm.lastTripStopPosition = nil
	vm.lastPositionTimestamp = timestamp
}

//makeObservedStopTimes build list of gtfs.ObservedStopTime for StopTimePair array
//startTimestamp should be the previous position prior to StopTimePair being observed
//endTimestamp is the time the observation was made
//observedAtTripStopPositions contains list of tripStopPositions where the vehicle was seen at a stop
func makeObservedStopTimes(
	vehicleId string,
	startTimestamp int64,
	endTimestamp int64,
	lastTripStopPosition *tripStopPosition,
	newTripStopPosition *tripStopPosition,
	stopPairs []StopTimePair) []gtfs.ObservedStopTime {

	results := make([]gtfs.ObservedStopTime, 0)
	lastStopTimePairIndex := len(stopPairs) - 1
	if lastStopTimePairIndex < 0 {
		return results
	}

	observedAtTripStopPositions := getObservedAtPositions(lastTripStopPosition, newTripStopPosition)
	firstScheduleSeconds := stopPairs[0].from.ArrivalTime
	lastScheduleSeconds := stopPairs[lastStopTimePairIndex].to.ArrivalTime
	totalScheduledLength := lastScheduleSeconds - firstScheduleSeconds

	observedTime := endTimestamp

	//don't include the seconds vehicle spent traveling between the next two stops
	observedTime -= int64(newTripStopPosition.observedSecondsToTravelToPosition)

	for i := lastStopTimePairIndex; i >= 0; i-- {
		pair := stopPairs[i]
		stopTimeInstance1 := pair.from
		stopTimeInstance2 := pair.to

		totalTimeOfTravel := int(observedTime - startTimestamp)

		segmentScheduleLength := stopTimeInstance2.ArrivalTime - stopTimeInstance1.ArrivalTime
		travelSeconds := getSegmentTravelPortion(totalTimeOfTravel, totalScheduledLength, segmentScheduleLength)
		if i == 0 { //only needed for first stop pair since lastTripStopPosition will contain any travel time recorded from previous positions
			travelSeconds += earlierTravelSecondsForStop(&stopTimeInstance1, lastTripStopPosition)
		}

		//if calculating from the first stop
		if containsFirstStopOfTrip(stopTimeInstance1.TripId, stopPairs) {
			late := int(observedTime - stopTimeInstance2.ArrivalDateTime.Unix())
			if travelSeconds > segmentScheduleLength && late >= 0 {
				//Convert travel seconds observed seconds to be no more late than the vehicle is arriving at the stop
				//due vehicles laying over at their first stop and not sending an update
				travelSeconds = segmentScheduleLength + late

			}
		}

		observedStopTime := gtfs.ObservedStopTime{
			RouteId:            pair.trip.RouteId,
			StopId:             stopTimeInstance1.StopId,
			ObservedAtStop:     stopTimeInstancePresent(stopTimeInstance1, observedAtTripStopPositions),
			NextStopId:         stopTimeInstance2.StopId,
			ObservedAtNextStop: stopTimeInstancePresent(stopTimeInstance2, observedAtTripStopPositions),
			ObservedTime:       time.Unix(observedTime, 0),
			TravelSeconds:      travelSeconds,
			ScheduledSeconds:   &segmentScheduleLength,
			VehicleId:          vehicleId,
			TripId:             stopTimeInstance1.TripId,
		}
		//prepend since we are moving backwards
		results = append([]gtfs.ObservedStopTime{observedStopTime}, results...)
		observedTime -= int64(travelSeconds)

	}
	return results
}

//earlierTravelSecondsForStop returns number of seconds vehicle was previously observed traveling from stopInstance
func earlierTravelSecondsForStop(stopInstance *gtfs.StopTimeInstance, lastTripStopPosition *tripStopPosition) int {
	if stopInstance.TripId == lastTripStopPosition.previousSTI.TripId &&
		stopInstance.StopSequence == lastTripStopPosition.previousSTI.StopSequence {
		return lastTripStopPosition.scheduledSecondsFromLastStop
	}
	return 0
}

//containsFirstStopOfTrip returns trip if any StopTimePair is the first stop of tripId
func containsFirstStopOfTrip(tripId string, stopPairs []StopTimePair) bool {
	for _, pair := range stopPairs {
		if pair.from.FirstStop && pair.from.TripId == tripId {
			return true
		}
	}
	return false
}

//stopTimeInstancePresent returns true if stopTimeInstance is present in positions
func stopTimeInstancePresent(stopTimeInstance gtfs.StopTimeInstance, positions []tripStopPosition) bool {
	for _, position := range positions {
		if stopTimeInstance.TripId == position.tripInstance.TripId &&
			stopTimeInstance.StopSequence == position.previousSTI.StopSequence {
			return true
		}
	}
	return false
}

//getSegmentTravelPortion returns the portion of totalTravelSeconds
//that segmentScheduleLength represents in totalScheduleLength
func getSegmentTravelPortion(totalTravelSeconds int,
	totalScheduledLength int,
	segmentScheduleLength int) int {
	if segmentScheduleLength <= 0 {
		return 0
	}
	percent := float32(segmentScheduleLength) / float32(totalScheduledLength)
	return int(percent * float32(totalTravelSeconds))
}

//getStopPairsBetweenPositions get list of StopTimePairs between lastPosition and currentPosition
func getStopPairsBetweenPositions(lastPosition *tripStopPosition,
	currentPosition *tripStopPosition) ([]StopTimePair, error) {

	currentTrip := currentPosition.tripInstance
	fromSequence := lastPosition.previousSTI.StopSequence
	toSequence := currentPosition.previousSTI.StopSequence

	//ignore the previous stop if we do not have information about the vehicle's position from that stop
	if !lastPosition.witnessedPreviousStop {
		fromSequence++
	}

	//check if we are on the same trip
	if lastPosition.tripInstance.TripId == currentPosition.tripInstance.TripId {
		return getStopPairsBetweenSequences(currentTrip, fromSequence, toSequence), nil
	}

	lastTripChangedStops := getStopPairsBetweenSequences(lastPosition.tripInstance, fromSequence, getLastStopTimeSequenceOnTrip(currentTrip))
	currentChangedStops := getStopPairsBetweenSequences(currentTrip, 0, toSequence)
	combined := append(lastTripChangedStops, currentChangedStops...)

	return combined, nil
}

//getLastStopTimeSequenceOnTrip returns the final previousStopSequence on trip
func getLastStopTimeSequenceOnTrip(trip *gtfs.TripInstance) uint32 {
	if trip == nil {
		return 0
	}
	size := len(trip.StopTimeInstances)
	if size < 1 {
		return 0
	}
	return trip.StopTimeInstances[size-1].StopSequence
}

//StopTimePair contains the to and from gtfs.StopTimeInstance for a stop transition on a gtfs.TripInstance
type StopTimePair struct {
	from gtfs.StopTimeInstance
	to   gtfs.StopTimeInstance
	trip *gtfs.TripInstance
}

//getStopPairsBetweenSequences returns StopTimePair on trip that have stop sequences
//between fromStopSequence and toStopSequence inclusively
func getStopPairsBetweenSequences(trip *gtfs.TripInstance,
	fromStopSequence uint32,
	toStopSequence uint32) []StopTimePair {
	changedStops := make([]StopTimePair, 0)
	if toStopSequence <= fromStopSequence {
		return changedStops
	}
	stopTimeInstances := trip.StopTimeInstances
	numberStopTimeInstances := len(stopTimeInstances)
	for i := 0; i+1 < numberStopTimeInstances; i++ {
		stopTimeInstance1 := stopTimeInstances[i]
		stopTimeInstance2 := stopTimeInstances[i+1]
		if stopTimeInstance1.StopSequence >= fromStopSequence && stopTimeInstance2.StopSequence <= toStopSequence {
			changedStops = append(changedStops, StopTimePair{*stopTimeInstance1, *stopTimeInstance2, trip})
		}
		if stopTimeInstance2.StopSequence >= toStopSequence {
			return changedStops
		}
	}
	return changedStops
}

//isMovementBelievable for a given StopTimePair list, is it believable that these stops where traversed in the time
//between fromTimestamp and toTimestamp
func isMovementBelievable(stopTimePairs []StopTimePair,
	fromTimestamp int64,
	toTimestamp int64,
	earlyTolerance float64) (isValid bool, totalScheduleTime int64, took int64) {
	took = toTimestamp - fromTimestamp
	size := len(stopTimePairs)
	if size < 1 {
		return true, 0, took
	}
	totalScheduleTime = int64(0)
	furthestTime := int64(0)
	for _, pair := range stopTimePairs {
		//never move backwards while observing stops
		if furthestTime > pair.from.ArrivalDateTime.Unix() {
			return false, 0, took
		} else {
			furthestTime = pair.from.ArrivalDateTime.Unix()
		}
		totalScheduleTime += pair.to.ArrivalDateTime.Unix() - pair.from.ArrivalDateTime.Unix()

	}
	if totalScheduleTime < 0 {
		return false, totalScheduleTime, took
	}

	if totalScheduleTime == 0.0 && earlyTolerance > 0.0 {
		return false, totalScheduleTime, took
	}
	early := float64(took) / float64(totalScheduleTime)
	return early >= earlyTolerance, totalScheduleTime, took
}
