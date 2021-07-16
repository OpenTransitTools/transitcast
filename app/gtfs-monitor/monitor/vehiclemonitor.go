package monitor

import (
	"fmt"
	"gitlab.trimet.org/transittracker/transitmon/business/data/gtfs"
	"log"

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
	stopId                string
	//seenAtStop is true when we have seen vehicle be StoppedAt at stopSequence
	seenAtStop            bool
	//witnessedPreviousStop indicates that we have seen the vehicle at or prior to stopSequence
	witnessedPreviousStop bool
	tripInstance          *gtfs.TripInstance
	//stopSequence is the stop sequence on this trip that we are at or before
	stopSequence          uint32
	nextStopSequence      uint32
	isFirstStop           bool
	lastTimestamp         int64
}

//isSamePosition returns true if other tripStopPosition is equivalent to the t tripStopPosition receiver
func (t *tripStopPosition) isSamePosition(other *tripStopPosition) bool {
	return t.stopId == other.stopId &&
		t.seenAtStop == other.seenAtStop &&
		t.witnessedPreviousStop == other.witnessedPreviousStop &&
		t.tripInstance.TripId == other.tripInstance.TripId &&
		t.stopSequence == other.stopSequence &&
		t.nextStopSequence == other.nextStopSequence
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

		log.Printf("Discarding trip movement as it doesn't appear valid. totalScheduleTime:%d took:%d position:%v lastTripStopPosition:%+v",
			totalScheduleTime, took, position, vm.lastTripStopPosition)
		vm.removeStopPosition(position.Timestamp)
		return results
	}

	results = makeObservedStopTimes(vm.Id, lastPositionTimestamp, position.Timestamp,
		getObservedAtPositions(lastTripStopPosition, newTripStopPosition), stopTimePairs)

	return results
}

//witnessedPreviousStop returns true if the previous tripStopPosition is before or at the stop on tripId at stopSequence
//indicating that the vehicle was seen at ore previous to the last stop
func witnessedPreviousStop(tripId string, stopSequence uint32, previousTripStopPosition *tripStopPosition) bool {
	if previousTripStopPosition == nil {
		return false
	}
	if previousTripStopPosition.tripInstance.TripId != tripId {
		return true
	}
	if previousTripStopPosition.stopSequence < stopSequence {
		return true
	}
	if previousTripStopPosition.stopSequence == stopSequence && previousTripStopPosition.seenAtStop {
		return true
	}
	return false
}

//getTripStopPosition builds a tripStopPosition
func getTripStopPosition(trip *gtfs.TripInstance, previousTripStopPosition *tripStopPosition, position *vehiclePosition) (*tripStopPosition, error) {

	witnessedPrevious := witnessedPreviousStop(trip.TripId, *position.StopSequence, previousTripStopPosition)
	for index, sst := range trip.StopTimeInstances {
		if sst.StopSequence == *position.StopSequence {
			nextStopSequence := sst.StopSequence
			if index+1 < len(trip.StopTimeInstances) {
				nextStopSequence = trip.StopTimeInstances[index+1].StopSequence
			}
			return &tripStopPosition{
				stopId:                sst.StopId,
				seenAtStop:            position.VehicleStopStatus == StoppedAt,
				witnessedPreviousStop: witnessedPrevious || position.VehicleStopStatus == StoppedAt,
				tripInstance:          trip,
				stopSequence:          sst.StopSequence,
				nextStopSequence:      nextStopSequence,
				isFirstStop:           index == 0,
				lastTimestamp:         position.Timestamp,
			}, nil
		}
	}
	return nil, fmt.Errorf("missing stop at tripId:%s stopSequence:%d", *position.TripId, *position.StopSequence)
}

//shouldUseToMoveForward  returns true if the newPosition indicates movement from previousTripStopPosition
func shouldUseToMoveForward(previousTripStopPosition *tripStopPosition, newPosition *tripStopPosition) bool {
	if previousTripStopPosition.tripInstance.TripId != newPosition.tripInstance.TripId {
		return true
	}
	if newPosition.stopSequence > previousTripStopPosition.stopSequence {
		if newPosition.stopSequence == previousTripStopPosition.nextStopSequence { //its the next stop
			if previousTripStopPosition.seenAtStop && !newPosition.seenAtStop {
				//its only incoming to the next stop and isn't there yet
				return false
			}

		}
		return true
	}
	if previousTripStopPosition.stopSequence == newPosition.stopSequence {
		if !previousTripStopPosition.seenAtStop && newPosition.seenAtStop {
			return true
		}
	}

	return false
}

//updateStoppedAtPosition checks if two tripStopPositions are at the same stop
//and returns true if the new position should cause an update to the monitored vehicle position
//Currently new positions at the first stop of the trip is considered new and usable, others are not
func updateStoppedAtPosition(previousTripStopPosition *tripStopPosition, newPosition *tripStopPosition) bool {
	if previousTripStopPosition.stopSequence == newPosition.stopSequence {
		if newPosition.seenAtStop {
			return newPosition.isFirstStop
		}
	}
	return false
}

//isCurrentPositionExpired returns true if the current position is expired at currentTimestamp
func (vm *vehicleMonitor) isCurrentPositionExpired(currentTimestamp int64) bool {
	diff := currentTimestamp - vm.lastTripStopPosition.lastTimestamp
	return diff > vm.expirePositionSeconds
}

//getObservedAtPositions convenience function returns the tripStopPosition arguments that have had their seenAtStop flag set
func getObservedAtPositions(position1 *tripStopPosition, position2 *tripStopPosition) []tripStopPosition {
	result := make([]tripStopPosition, 0)
	if position1.seenAtStop {
		result = append(result, *position1)
	}
	if position2.seenAtStop {
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
	} else {
		vm.updateExpirationTimestamps(positionTimestamp)
	}
	return movedForward
}

//updateTripStopPosition sets vehicleMonitors current position to newTripStopPosition at positionTimestamp
func (vm *vehicleMonitor) updateTripStopPosition(
	newTripStopPosition *tripStopPosition,
	positionTimestamp int64) {
	vm.lastTripStopPosition = newTripStopPosition
	vm.lastPositionTimestamp = positionTimestamp

}

//updateExpirationTimestamps updates vm.lastTripStopPosition.lastTimestamp with new expiration
func (vm *vehicleMonitor) updateExpirationTimestamps(currentTimestamp int64) {
	vm.lastTripStopPosition.lastTimestamp = currentTimestamp + vm.expirePositionSeconds

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
	observedAtTripStopPositions []tripStopPosition,
	stopPairs []StopTimePair) []gtfs.ObservedStopTime {

	results := make([]gtfs.ObservedStopTime, 0)
	lastStopTimePairIndex := len(stopPairs) - 1
	if lastStopTimePairIndex < 0 {
		return results
	}

	firstScheduleSeconds := stopPairs[0].from.ArrivalTime
	lastScheduleSeconds := stopPairs[lastStopTimePairIndex].to.ArrivalTime
	totalScheduledLength := int64(lastScheduleSeconds - firstScheduleSeconds)
	observedTime := endTimestamp

	for i := lastStopTimePairIndex; i >= 0; i-- {
		pair := stopPairs[i]
		stopTimeInstance1 := pair.from
		stopTimeInstance2 := pair.to

		totalTimeOfTravel := observedTime - startTimestamp

		segmentScheduleLength := int64(stopTimeInstance2.ArrivalTime - stopTimeInstance1.ArrivalTime)
		travelSeconds := getSegmentTravelPortion(totalTimeOfTravel, totalScheduledLength, segmentScheduleLength)

		//if calculating from the first stop
		if containsFirstStopOfTrip(stopTimeInstance1.TripId, stopPairs) {
			late := observedTime - stopTimeInstance2.ArrivalDateTime.Unix()
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
		observedTime -= travelSeconds

	}
	return results
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
			stopTimeInstance.StopSequence == position.stopSequence {
			return true
		}
	}
	return false
}

//getSegmentTravelPortion returns the portion of totalTravelSeconds
//that segmentScheduleLength represents in totalScheduleLength
func getSegmentTravelPortion(totalTravelSeconds int64,
	totalScheduledLength int64,
	segmentScheduleLength int64) int64 {
	if segmentScheduleLength <= 0 {
		return 0
	}
	percent := float32(segmentScheduleLength) / float32(totalScheduledLength)
	return int64(percent * float32(totalTravelSeconds))
}

//getStopPairsBetweenPositions get list of StopTimePairs between lastPosition and currentPosition
func getStopPairsBetweenPositions(lastPosition *tripStopPosition,
	currentPosition *tripStopPosition) ([]StopTimePair, error) {

	currentTrip := currentPosition.tripInstance
	fromSequence := lastPosition.stopSequence
	/*if !lastPosition.witnessedPreviousStop {
		fromSequence++
	}*/
	toSequence := currentPosition.stopSequence
	//if we are at the current stop don't include it
	if !currentPosition.seenAtStop {
		toSequence--
	}

	//check if we are on the same trip
	if lastPosition.tripInstance.TripId == currentPosition.tripInstance.TripId {
		//if we saw the vehicle pass a previous stop, we should get at least one stop sequence
		if currentPosition.witnessedPreviousStop && fromSequence >= toSequence {
			fromSequence--
		}
		return getStopPairsBetweenSequences(currentTrip, fromSequence, toSequence), nil
	}

	lastTripChangedStops := getStopPairsBetweenSequences(lastPosition.tripInstance, fromSequence, getLastStopTimeSequenceOnTrip(currentTrip))
	currentChangedStops := getStopPairsBetweenSequences(currentTrip, 0, toSequence)
	combined := append(lastTripChangedStops, currentChangedStops...)

	return combined, nil
}

//getLastStopTimeSequenceOnTrip returns the final stopSequence on trip
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
