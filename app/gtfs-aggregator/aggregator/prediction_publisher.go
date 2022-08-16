package aggregator

import (
	"encoding/json"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/nats-io/nats.go"
	logger "log"
	"time"
)

//predictionPublisher takes completed predictions and publishes them on NATS connection as TripUpdates
type predictionPublisher struct {
	log               *logger.Logger
	natsConn          *nats.Conn
	predictionSubject string
}

//makePredictionPublisher builds predictionPublisher
func makePredictionPublisher(log *logger.Logger,
	natsConn *nats.Conn,
	predictionSubject string) *predictionPublisher {
	return &predictionPublisher{
		log:               log,
		natsConn:          natsConn,
		predictionSubject: predictionSubject,
	}
}

//publishPredictionBatch for each trip predictions in predictionBatch, build gtfs.TripUpdate
// and publish them over NATS
func (p *predictionPublisher) publishPredictionBatch(batch *predictionBatch) {
	orderedTripPredictions := batch.orderedTripPredictions()
	tripUpdates := makeTripUpdates(p.log, orderedTripPredictions)
	for _, tripUpdate := range tripUpdates {
		jsonData, err := json.Marshal(tripUpdate)
		if err != nil {
			p.log.Printf("Error marshaling tripUpdate to json: error:%v\n", err)
			return
		}
		err = p.natsConn.Publish(p.predictionSubject, jsonData)
		if err != nil {
			p.log.Printf("Error sending tripUpdate to nats: error:%v\n", err)
			return
		}
	}
}

//makeTripUpdates builds series of gtfs.TripUpdates from tripPredictions
func makeTripUpdates(log *logger.Logger,
	orderedPredictions []*tripPrediction) []*gtfs.TripUpdate {

	tripUpdates := make([]*gtfs.TripUpdate, 0)
	schedulePosition := getInitialSchedulePosition(orderedPredictions)

	for _, prediction := range orderedPredictions {
		tripUpdate := buildTripUpdate(log, schedulePosition, prediction)
		newSchedulePosition := tripUpdate.LastSchedulePosition()
		if newSchedulePosition != nil {
			schedulePosition = *newSchedulePosition
			tripUpdates = append(tripUpdates, tripUpdate)
		}
	}

	return tripUpdates
}

//getInitialSchedulePosition return the schedule position (where the vehicle is according to its schedule) of the
//vehicle on its series of tripPredictions
func getInitialSchedulePosition(orderedPredictions []*tripPrediction) time.Time {
	if len(orderedPredictions) < 1 {
		return time.Now()
	}
	firstDeviation := orderedPredictions[0].tripDeviation
	return firstDeviation.DeviationTimestamp.Add(time.Duration(firstDeviation.Delay) * time.Second)

}

//buildTripUpdate builds a gtfs.TripUpdate a tripPrediction
// previousSchedulePositionTime should be the last position the vehicle was reported as departing from
// allowing this trip update to start late if the vehicle is running late after its previous trip
func buildTripUpdate(log *logger.Logger,
	previousSchedulePositionTime time.Time,
	prediction *tripPrediction) *gtfs.TripUpdate {
	trip := prediction.tripInstance
	tripUpdate := gtfs.TripUpdate{
		TripId:               trip.TripId,
		RouteId:              trip.RouteId,
		ScheduleRelationship: "SCHEDULED",
		Timestamp:            uint64(prediction.tripDeviation.DeviationTimestamp.Unix()),
		VehicleId:            prediction.tripDeviation.VehicleId,
		StopTimeUpdates:      buildPastStopUpdates(previousSchedulePositionTime, prediction),
	}

	previousSchedulePositionTime = getLatestTimeAfterInitialStop(previousSchedulePositionTime, tripUpdate.StopTimeUpdates)

	var predictionRemainder = 0.0
	tripDistanceTraveled := prediction.tripDeviation.TripProgress
	for _, sp := range prediction.stopPredictions {
		var newStopUpdate gtfs.StopTimeUpdate
		newStopUpdate, predictionRemainder = buildStopUpdate(log, previousSchedulePositionTime, tripDistanceTraveled, predictionRemainder,
			sp)
		previousSchedulePositionTime = newStopUpdate.PredictedArrivalTime
		tripUpdate.StopTimeUpdates = append(tripUpdate.StopTimeUpdates, newStopUpdate)
	}

	return &tripUpdate
}

//getLatestTimeAfterInitialStop returns previousTime, or the arrival time of the first stop, whichever is latest
func getLatestTimeAfterInitialStop(previousTime time.Time, stopTimeUpdates []gtfs.StopTimeUpdate) time.Time {
	size := len(stopTimeUpdates)
	if size == 0 {
		return previousTime
	}
	//just looking for the first stop
	return laterOfDates(previousTime, stopTimeUpdates[0].ScheduledArrivalTime)
}

//buildPastStopUpdates creates gtfs.StopTimeUpdates that should be included in a gtfs.TripUpdate that the vehicle
//has already past
func buildPastStopUpdates(previousTime time.Time,
	prediction *tripPrediction) []gtfs.StopTimeUpdate {
	updates := make([]gtfs.StopTimeUpdate, 0)
	if len(prediction.stopPredictions) == 0 {
		return updates
	}
	firstStopPrediction := prediction.stopPredictions[0]
	for _, stopTime := range prediction.tripInstance.StopTimeInstances {
		if len(updates) == 0 {
			//if the trip has moved past the stop, use buildStopUpdateForPastStop
			if prediction.tripDeviation.TripProgress > stopTime.ShapeDistTraveled {
				updates = append(updates, buildStopUpdateForPastStop(previousTime, stopTime))
			} else {
				updates = append(updates, buildStopUpdateForFirstStop(previousTime, stopTime))
			}

			if stopTime.StopSequence == firstStopPrediction.fromStop.StopSequence {
				return updates
			}
		}
		if stopTime.StopSequence == firstStopPrediction.fromStop.StopSequence {
			//found the first stop sequence without a prediction
			return append(updates, buildStopUpdateForPastStop(previousTime, stopTime))

		}
	}
	return updates
}

//buildStopUpdate creates gtfs.StopTimeUpdate from stopPrediction. previousTime is the last stop time the vehicle was
//located at, (a previous StopUpdate or the vehicle schedule position if its between the previous stop and this one)
//tripDistanceTraveled is how far along the vehicle is on this trip, should not be further than stopPrediction.toStop
//previousPredictionRemainder is the previous predictions remainder after rounding the predictions to seconds
func buildStopUpdate(log *logger.Logger,
	previousTime time.Time,
	tripDistanceTraveled float64,
	previousPredictionRemainder float64,
	stopPrediction *stopPrediction) (stopTimeUpdate gtfs.StopTimeUpdate, predictionRemainder float64) {
	toStop := stopPrediction.toStop
	traversalSeconds := stopPrediction.predictedTime + previousPredictionRemainder
	//if the vehicle is further than the previous stop it's between the last stop and this one
	if tripDistanceTraveled > stopPrediction.fromStop.ShapeDistTraveled {
		//shorten the amount of distance the vehicle has to travel to stopPrediction.ToStop
		traversalSeconds = adjustTraversalSeconds(log, tripDistanceTraveled, stopPrediction)
	}
	//only whole seconds
	traversalInt64, traversalRemainder := roundSecondsAndRemainder(traversalSeconds)
	predictedArrivalTime := previousTime.Add(time.Duration(traversalInt64) * time.Second)

	return gtfs.StopTimeUpdate{
		StopSequence:         toStop.StopSequence,
		StopId:               toStop.StopId,
		ScheduledArrivalTime: toStop.ArrivalDateTime,
		ArrivalDelay:         int(predictedArrivalTime.Sub(toStop.ArrivalDateTime).Seconds()),
		PredictedArrivalTime: predictedArrivalTime,
		PredictionSource:     stopPrediction.predictionSource,
	}, traversalRemainder
}

//adjustTraversalSeconds returns the distance measured in schedule seconds left to travel between stops in
//stopPrediction based on tripDistanceTraveled (the vehicle's progress on its trip
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

//roundSecondsAndRemainder returns truncated traversalSeconds fractional seconds and remainder
func roundSecondsAndRemainder(traversalSeconds float64) (int64, float64) {
	seconds := int64(traversalSeconds)
	return seconds, traversalSeconds - float64(seconds)
}

//buildStopUpdateForFirstStop creates gtfs.StopTimeUpdate for first stop of trip
func buildStopUpdateForFirstStop(at time.Time, stopTime *gtfs.StopTimeInstance) gtfs.StopTimeUpdate {

	late := at.Sub(stopTime.DepartureDateTime).Seconds()
	if late < 0 { //don't report early departure
		late = 0
	}

	return gtfs.StopTimeUpdate{
		StopSequence:         stopTime.StopSequence,
		StopId:               stopTime.StopId,
		ArrivalDelay:         int(late),
		ScheduledArrivalTime: stopTime.ArrivalDateTime,
		PredictedArrivalTime: stopTime.ArrivalDateTime.Add(time.Duration(late) * time.Second),
		PredictionSource:     gtfs.SchedulePrediction,
	}
}

//buildStopUpdateForPastStop creates gtfs.StopTimeUpdate stopTime that the vehicle has already past
//to indicate to consumers the vehicle has past this stop already
func buildStopUpdateForPastStop(at time.Time, stopTime *gtfs.StopTimeInstance) gtfs.StopTimeUpdate {
	arrivalTime := earlierOfDates(at.Add(-time.Minute), stopTime.ArrivalDateTime)

	return gtfs.StopTimeUpdate{
		StopSequence:         stopTime.StopSequence,
		StopId:               stopTime.StopId,
		ArrivalDelay:         int(arrivalTime.Sub(stopTime.ArrivalDateTime).Seconds()),
		ScheduledArrivalTime: stopTime.ArrivalDateTime,
		PredictedArrivalTime: arrivalTime,
		PredictionSource:     gtfs.SchedulePrediction,
	}
}

//laterOfDates return the latter of two dates
func laterOfDates(first time.Time, second time.Time) time.Time {
	if first.After(second) {
		return first
	}
	return second
}

//laterOfDates return the earlier of two dates
func earlierOfDates(first time.Time, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
}
