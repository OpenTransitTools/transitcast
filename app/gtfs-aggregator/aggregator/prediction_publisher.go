package aggregator

import (
	"encoding/json"
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/nats-io/nats.go"
	logger "log"
	"math"
	"time"
)

// predictionPublicationDestination is where predictions should be sent after completion.
type predictionPublicationDestination interface {
	Publish(update *gtfs.TripUpdate) error
}

// natsPredictionPublicationDestination sends predictions over nats
type natsPredictionPublicationDestination struct {
	natsConn          *nats.Conn
	predictionSubject string
}

func (n *natsPredictionPublicationDestination) Publish(tripUpdate *gtfs.TripUpdate) error {
	jsonData, err := json.Marshal(tripUpdate)
	if err != nil {
		return fmt.Errorf("error marshaling tripUpdate to json: error:%v\n", err)
	}
	return n.natsConn.Publish(n.predictionSubject, jsonData)
}

// predictionPublisher takes completed predictions and publishes them on NATS connection as TripUpdates
type predictionPublisher struct {
	log                              *logger.Logger
	predictionPublicationDestination predictionPublicationDestination
	limitEarlyDepartureSeconds       int
}

// makePredictionPublisher builds predictionPublisher
func makePredictionPublisher(log *logger.Logger,
	predictionPublicationDestination predictionPublicationDestination,
	limitEarlyDepartureSeconds int) *predictionPublisher {
	return &predictionPublisher{
		log:                              log,
		predictionPublicationDestination: predictionPublicationDestination,
		limitEarlyDepartureSeconds:       limitEarlyDepartureSeconds,
	}
}

// publishPredictionBatch for each trip predictions in predictionBatch, build gtfs.TripUpdate
// and publish them over NATS
func (p *predictionPublisher) publishPredictionBatch(batch *predictionBatch) {
	orderedTripPredictions := batch.orderedTripPredictions()
	tripUpdates := makeTripUpdates(p.log, orderedTripPredictions, p.limitEarlyDepartureSeconds)
	for _, tripUpdate := range tripUpdates {
		err := p.predictionPublicationDestination.Publish(tripUpdate)
		if err != nil {
			p.log.Printf("Error publishing tripUpdate: error:%v\n", err)
			return
		}
	}
}

// makeTripUpdates builds series of gtfs.TripUpdates from tripPredictions
func makeTripUpdates(log *logger.Logger,
	orderedPredictions []*tripPrediction,
	limitEarlyDepartureSeconds int) []*gtfs.TripUpdate {

	tripUpdates := make([]*gtfs.TripUpdate, 0)
	var predictedPositionInTime time.Time
	for _, prediction := range orderedPredictions {
		if len(tripUpdates) == 0 {
			predictedPositionInTime = prediction.tripDeviation.DeviationTimestamp
		}
		tripUpdate := buildTripUpdate(log, predictedPositionInTime, prediction, limitEarlyDepartureSeconds)
		if tripUpdate != nil {
			newSchedulePosition := tripUpdate.LastSchedulePosition()
			if newSchedulePosition != nil {
				predictedPositionInTime = *newSchedulePosition
			}
			tripUpdates = append(tripUpdates, tripUpdate)
		}

	}

	return tripUpdates
}

// buildTripUpdate builds a gtfs.TripUpdate a tripPrediction
// previousSchedulePositionTime should be the last position the vehicle was reported as departing from
// allowing this trip update to start late if the vehicle is running late after its previous trip
func buildTripUpdate(log *logger.Logger,
	predictedPositionInTime time.Time,
	prediction *tripPrediction,
	limitEarlyDepartureSeconds int) *gtfs.TripUpdate {
	trip := prediction.tripInstance
	if len(trip.StopTimeInstances) < 1 {
		log.Printf("trip %s had no StopTimeInstances", trip.TripId)
		return nil
	}
	tripDeviation := prediction.tripDeviation
	deviationTimestamp := tripDeviation.DeviationTimestamp

	tripUpdate := gtfs.TripUpdate{
		TripId:               trip.TripId,
		RouteId:              trip.RouteId,
		ScheduleRelationship: "SCHEDULED",
		Timestamp:            uint64(deviationTimestamp.Unix()),
		VehicleId:            tripDeviation.VehicleId,
	}

	var lastPastStop *gtfs.StopTimeInstance
	var predictionsForStopUpdates []*stopPrediction

	//find the last stop that was past, and collect stopPredictions that need to be made after it
	for _, sp := range prediction.stopPredictions {
		if sp.stopUpdateDisposition == PastStop {
			lastPastStop = sp.toStop
		} else {
			predictionsForStopUpdates = append(predictionsForStopUpdates, sp)
		}
	}

	delay := deviationTimestamp.Sub(tripDeviation.SchedulePosition())
	firstStopTimeInstance := trip.StopTimeInstances[0]
	stopUpdate := buildStopUpdateForFirstStop(predictedPositionInTime, tripDeviation.SchedulePosition(),
		deviationTimestamp, delay, firstStopTimeInstance)
	tripUpdate.StopTimeUpdates = []gtfs.StopTimeUpdate{stopUpdate}
	predictedPositionInTime = predictedPositionInTimeAfterFirstStop(predictedPositionInTime,
		stopUpdate.PredictedArrivalTime, firstStopTimeInstance, tripDeviation.TripProgress)

	if lastPastStop != nil {
		lastPastStopUpdate := buildStopUpdateForPassedStop(deviationTimestamp, lastPastStop, delay)
		tripUpdate.StopTimeUpdates = append(tripUpdate.StopTimeUpdates, lastPastStopUpdate)
	}

	var predictionRemainder = 0.0

	for _, sp := range predictionsForStopUpdates {
		var newStopUpdate gtfs.StopTimeUpdate
		if sp.stopUpdateDisposition == AtStop {
			newStopUpdate = buildStopUpdateForAtStop(deviationTimestamp, sp.toStop, limitEarlyDepartureSeconds)
		} else {
			newStopUpdate, predictionRemainder = buildStopUpdate(log, predictedPositionInTime,
				tripDeviation.TripProgress, predictionRemainder, sp, limitEarlyDepartureSeconds)
		}

		predictedPositionInTime = newStopUpdate.LatestPredictedTime()
		tripUpdate.StopTimeUpdates = append(tripUpdate.StopTimeUpdates, newStopUpdate)
	}
	return &tripUpdate
}

// predictedPositionInTimeAfterFirstStop returns how much predictedPositionInTime should be used after the first stop of the trip
func predictedPositionInTimeAfterFirstStop(predictedPositionInTime time.Time,
	predictedDepartTime time.Time,
	firstStopInstance *gtfs.StopTimeInstance,
	tripProgress float64) time.Time {
	if tripProgress >= firstStopInstance.ShapeDistTraveled {
		return predictedPositionInTime
	}
	departTime := laterOfDates(predictedDepartTime, firstStopInstance.DepartureDateTime)
	if predictedPositionInTime.After(departTime) {
		return predictedPositionInTime
	}
	return predictedDepartTime
}

// buildStopUpdate creates gtfs.StopTimeUpdate from stopPrediction. predictedPositionInTime is the last stop time the vehicle was
// located at, (a previous StopUpdate or the vehicle schedule position if its between the previous stop and this one)
// tripDistanceTraveled is how far along the vehicle is on this trip, should not be further than stopPrediction.toStop
// previousPredictionRemainder is the previous predictions remainder after rounding the predictions to seconds
func buildStopUpdate(log *logger.Logger,
	predictedPositionInTime time.Time,
	tripDistanceTraveled float64,
	previousPredictionRemainder float64,
	stopPrediction *stopPrediction,
	limitEarlyDepartureSeconds int) (stopTimeUpdate gtfs.StopTimeUpdate, predictionRemainder float64) {
	toStop := stopPrediction.toStop
	traversalSeconds := stopPrediction.predictedTime + previousPredictionRemainder
	//if the vehicle is further than the previous stop it's between the last stop and this one
	if tripDistanceTraveled > stopPrediction.fromStop.ShapeDistTraveled {
		//shorten the amount of distance the vehicle has to travel to stopPrediction.ToStop
		traversalSeconds = adjustTraversalSeconds(log, tripDistanceTraveled, stopPrediction)
	}
	//only whole seconds
	traversalInt64, traversalRemainder := roundSecondsAndRemainder(traversalSeconds)
	predictedArrivalTime := predictedPositionInTime.Add(time.Duration(traversalInt64) * time.Second)
	arrivalDelay := int(predictedArrivalTime.Sub(toStop.ArrivalDateTime).Seconds())
	//check for early departure from last stop
	if stopPrediction.fromStop.IsTimepoint() &&
		tripDistanceTraveled <= stopPrediction.fromStop.ShapeDistTraveled &&
		arrivalDelay < -limitEarlyDepartureSeconds {
		arrivalDelay = -limitEarlyDepartureSeconds
		predictedArrivalTime = toStop.ArrivalDateTime.Add(time.Duration(-limitEarlyDepartureSeconds) * time.Second)
	}

	return gtfs.StopTimeUpdate{
		StopSequence:         toStop.StopSequence,
		StopId:               toStop.StopId,
		ScheduledArrivalTime: toStop.ArrivalDateTime,
		ArrivalDelay:         arrivalDelay,
		PredictedArrivalTime: predictedArrivalTime,
		PredictionSource:     stopPrediction.predictionSource,
	}, traversalRemainder
}

// adjustTraversalSeconds returns the distance measured in schedule seconds left to travel between stops in
// stopPrediction based on tripDistanceTraveled (the vehicle's progress on its trip
func adjustTraversalSeconds(log *logger.Logger, tripDistanceTraveled float64, segmentPrediction *stopPrediction) float64 {
	distanceBetweenStops := segmentPrediction.toStop.ShapeDistTraveled - segmentPrediction.fromStop.ShapeDistTraveled
	if distanceBetweenStops <= 0 {
		log.Printf("Distance between stop segments is zero or less: from: %+v to: %+v ",
			segmentPrediction.fromStop, segmentPrediction.toStop)
		return segmentPrediction.predictedTime
	}
	distanceTraveledBetweenStops := tripDistanceTraveled - segmentPrediction.fromStop.ShapeDistTraveled
	remainingDistance := distanceBetweenStops - distanceTraveledBetweenStops
	if remainingDistance <= 0 {
		return 0
	}
	percentBetweenStops := remainingDistance / distanceBetweenStops
	return segmentPrediction.predictedTime * percentBetweenStops
}

// roundSecondsAndRemainder returns truncated traversalSeconds fractional seconds and remainder
func roundSecondsAndRemainder(traversalSeconds float64) (int64, float64) {
	seconds := int64(traversalSeconds)
	return seconds, traversalSeconds - float64(seconds)
}

// buildStopUpdateForFirstStop creates gtfs.StopTimeUpdate for first stop of trip
func buildStopUpdateForFirstStop(
	predictedPositionInTime time.Time,
	positionInSchedule time.Time,
	positionTimestamp time.Time,
	delay time.Duration,
	stopTime *gtfs.StopTimeInstance) gtfs.StopTimeUpdate {

	stopUpdate := gtfs.StopTimeUpdate{
		StopSequence:         stopTime.StopSequence,
		StopId:               stopTime.StopId,
		ScheduledArrivalTime: stopTime.ArrivalDateTime,
		PredictionSource:     gtfs.SchedulePrediction,
	}

	//If this is true we have already passed the first stop, stopUpdate should indicate it's been past
	if positionInSchedule.After(stopTime.DepartureDateTime) {
		//Always use delay, to match old system to make result comparison easy
		stopUpdate.PredictedArrivalTime = stopTime.ArrivalDateTime.Add(delay)
		stopUpdate.ArrivalDelay = int(stopUpdate.PredictedArrivalTime.Sub(stopUpdate.ScheduledArrivalTime).Seconds())
		return stopUpdate
	}
	departTime := laterOfDates(positionTimestamp, predictedPositionInTime)

	//position will be before depart time, assume on time departure
	if departTime.Unix() <= stopTime.DepartureDateTime.Unix() {
		stopUpdate.PredictedArrivalTime = stopTime.ArrivalDateTime
		stopUpdate.ArrivalDelay = 0
		return stopUpdate

	}
	//late starting trip

	//before depart time, position is before stop, predictedPositionInTime is after stop
	stopUpdate.PredictedArrivalTime = predictedPositionInTime
	stopUpdate.ArrivalDelay = int(stopUpdate.PredictedArrivalTime.Sub(stopUpdate.ScheduledArrivalTime).Seconds())

	earliestPosition := earlierOfDates(positionTimestamp, predictedPositionInTime)

	if earliestPosition.Unix() <= stopTime.DepartureDateTime.Unix() {
		stopUpdate.ScheduledDepartureTime = &stopTime.DepartureDateTime
		stopUpdate.PredictedDepartureTime = &departTime
		departureDelay := int(stopUpdate.PredictedDepartureTime.Sub(stopTime.DepartureDateTime).Seconds())
		stopUpdate.DepartureDelay = &departureDelay
	}

	return stopUpdate
}

// buildStopUpdateForAtStop creates gtfs.StopTimeUpdate when a vehicle is located at a stop.
func buildStopUpdateForAtStop(at time.Time,
	stopTime *gtfs.StopTimeInstance,
	limitEarlyDepartureSeconds int) gtfs.StopTimeUpdate {

	arrivalTime := at

	delay := int(arrivalTime.Sub(stopTime.ArrivalDateTime).Seconds())

	if stopTime.IsTimepoint() && delay < -limitEarlyDepartureSeconds {
		delay = -limitEarlyDepartureSeconds
		arrivalTime = stopTime.ArrivalDateTime.Add(time.Duration(delay) * time.Second)
	}

	return gtfs.StopTimeUpdate{
		StopSequence:         stopTime.StopSequence,
		StopId:               stopTime.StopId,
		ArrivalDelay:         delay,
		ScheduledArrivalTime: stopTime.ArrivalDateTime,
		PredictedArrivalTime: arrivalTime,
		PredictionSource:     gtfs.SchedulePrediction,
	}
}

// buildStopUpdateForPassedStop creates gtfs.StopTimeUpdate stopTime that the vehicle has already past
func buildStopUpdateForPassedStop(at time.Time,
	stopTime *gtfs.StopTimeInstance,
	delay time.Duration) gtfs.StopTimeUpdate {

	arrivalTime := stopTime.ArrivalDateTime.Add(delay)
	// use a time early enough to indicate the bus has moved beyond this stop
	arrivalTime = earlierOfDates(at.Add(-time.Minute), arrivalTime)

	return gtfs.StopTimeUpdate{
		StopSequence:         stopTime.StopSequence,
		StopId:               stopTime.StopId,
		ArrivalDelay:         int(arrivalTime.Sub(stopTime.ArrivalDateTime).Seconds()),
		ScheduledArrivalTime: stopTime.ArrivalDateTime,
		PredictedArrivalTime: arrivalTime,
		PredictionSource:     gtfs.SchedulePrediction,
	}
}

// consideredAtStop returns true if stopDistance is close enough to tripProgress to be considered at the stop
func consideredAtStop(tripProgress float64, stopDistance float64) bool {
	return math.Abs(tripProgress-stopDistance) < 2.0
}

// laterOfDates return the latter of two dates
func laterOfDates(first time.Time, second time.Time) time.Time {
	if first.After(second) {
		return first
	}
	return second
}

// laterOfDates return the earlier of two dates
func earlierOfDates(first time.Time, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
}
