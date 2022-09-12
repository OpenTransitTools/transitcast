package aggregator

import (
	"encoding/json"
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/nats-io/nats.go"
	logger "log"
	"os"
	"sync"
	"time"
)

//startTripUpdateListener listens on NATS for vehicle-monitor-results (expecting gtfs.VehicleMonitorResults)
// these are used to generate predictions for the vehicles trips
// uses the NATS queue "prediction-generator", so more than one gtfs-aggregator process can generate predictions
func startTripUpdateListener(
	log *logger.Logger,
	wg *sync.WaitGroup,
	osts *observedStopTransitions,
	natsConn *nats.Conn,
	shutdownSignal chan bool,
	tripPredictorsCollection *tripPredictorsCollection,
	pendingPredictions *pendingPredictionsCollection,
	predictionPublisher *predictionPublisher,
	includedRoutes []string,
	inferenceBuckets int) {
	wg.Add(1)
	defer wg.Done()

	processor := makeTripUpdateProcessor(log,
		natsConn,
		predictionPublisher,
		osts,
		tripPredictorsCollection,
		pendingPredictions,
		inferenceBuckets,
		includedRoutes)

	ch := make(chan *nats.Msg, 64)
	log.Printf("Subscribing to vehicle-monitor-results in queue group prediction-generator on nats: %v\n",
		natsConn.Servers())
	sub, err := natsConn.ChanQueueSubscribe("vehicle-monitor-results", "prediction-generator", ch)
	if err != nil {
		log.Printf("Unable to establish subscription to nats server: %v\n", err)
		os.Exit(1)
	}

	predictionWG := sync.WaitGroup{}

	for {
		select {
		case msg := <-ch:
			go processor.initializePredictionFromMsg(msg, &predictionWG)
			break
		case <-shutdownSignal:
			log.Printf("ending TripUpdate listener on shutdown signal\n")
			unsubscribe(log, sub, "TripUpdate: vehicle-monitor-results")
			log.Printf("waiting for prediction subroutines to complete\n")
			predictionWG.Wait()
			log.Printf("exiting TripUpdate listener on shutdown signal\n")
			return
		}
	}

}

//unsubscribe convenience function for unsubscribing from a NATS subscription, and logging the results.
func unsubscribe(log *logger.Logger, sub *nats.Subscription, subName string) {
	if !sub.IsValid() {
		return
	}
	log.Printf("Unsubscribing to %s in queue group prediction-generator\n", subName)
	err := sub.Unsubscribe()

	if err != nil {
		log.Printf("error when attempting to unsubscribe to %s: %v\n", subName, err)
	}

}

//tripUpdateProcessor the creation of trip predictions from gtfs.VehicleMonitorResults
type tripUpdateProcessor struct {
	log                      *logger.Logger
	natsConn                 *nats.Conn
	predictionPublisher      *predictionPublisher
	osts                     *observedStopTransitions
	tripPredictorsCollection *tripPredictorsCollection
	pendingPredictions       *pendingPredictionsCollection
	inferenceBuckets         int
	includedRoutes           []string
}

//makeTripUpdateProcessor builds tripUpdateProcessor
func makeTripUpdateProcessor(log *logger.Logger,
	natsConn *nats.Conn,
	predictionPublisher *predictionPublisher,
	osts *observedStopTransitions,
	tripPredictorsCollection *tripPredictorsCollection,
	pendingPredictions *pendingPredictionsCollection,
	inferenceBuckets int,
	includedRoutes []string) *tripUpdateProcessor {
	return &tripUpdateProcessor{
		log:                      log,
		natsConn:                 natsConn,
		predictionPublisher:      predictionPublisher,
		osts:                     osts,
		tripPredictorsCollection: tripPredictorsCollection,
		pendingPredictions:       pendingPredictions,
		inferenceBuckets:         inferenceBuckets,
		includedRoutes:           includedRoutes,
	}
}

//initializePredictionFromMsg unmarshal gtfs.VehicleMonitorResults and create predictions from gtfs.TripDeviation
func (t *tripUpdateProcessor) initializePredictionFromMsg(msg *nats.Msg, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	var vehicleMonitorResults gtfs.VehicleMonitorResults
	err := json.Unmarshal(msg.Data, &vehicleMonitorResults)
	if err != nil {
		t.log.Printf("error parsing VehicleMonitorResults: %v, payload:%s", err, string(msg.Data))
		return
	}

	batch := t.predictionsForVehicleMonitorResults(&vehicleMonitorResults)
	if batch == nil {
		return
	}
	t.handlePredictionBatch(batch)

}

//predictionsForVehicleMonitorResults creates prediction requests from gtfs.VehicleMonitorResults and returns
//predictionBatch if successful
func (t *tripUpdateProcessor) predictionsForVehicleMonitorResults(
	vehicleMonitorResults *gtfs.VehicleMonitorResults) *predictionBatch {

	//first assign the OSTs to vehicleMonitorResults
	for _, ost := range vehicleMonitorResults.ObservedStopTimes {
		t.osts.newOST(ost)
	}
	batch := makePredictionBatch(time.Now(), vehicleMonitorResults.VehicleId)
	for _, deviation := range vehicleMonitorResults.TripDeviations {
		if !t.shouldPredictTripDeviation(deviation) {
			continue
		}
		tp, inferenceRequests, err := t.startPredictionForTripDeviation(deviation)
		if err != nil {
			t.log.Printf("Error generating pendingTripPrediction tripId %s, error:%v", deviation.TripId, err)
			return nil
		}
		batch.addPendingTripPrediction(tp, inferenceRequests)
	}
	return batch

}

//shouldPredictTripDeviation returns true if deviation should be used to generate a prediction based on filtered RouteIds
func (t *tripUpdateProcessor) shouldPredictTripDeviation(deviation *gtfs.TripDeviation) bool {
	//include the trip deviation if includedRoutes is empty
	if len(t.includedRoutes) == 0 {
		return true
	}
	for _, value := range t.includedRoutes {
		if value == deviation.RouteId {
			return true
		}
	}
	return false
}

//startPredictionForTripDeviation creates tripPrediction returning it and any InferenceRequests to be made to complete
//the tripPrediction
func (t *tripUpdateProcessor) startPredictionForTripDeviation(
	deviation *gtfs.TripDeviation) (*tripPrediction, []*InferenceRequest, error) {

	predictor, err := t.tripPredictorsCollection.retrieveTripPredictor(deviation)
	if err != nil {
		return nil, nil, err
	}
	tp, inferenceRequests := predictor.predict(deviation)
	return tp, inferenceRequests, nil
}

//handlePredictionBatch takes a predictionBatch, if complete uses predictionPublisher to publish the results,
//if not complete (there are inference requests that need to be made) adds predictionBatch to pendingPredictions
//and sends all InferenceRequests from the predictionBatch
func (t *tripUpdateProcessor) handlePredictionBatch(batch *predictionBatch) {
	if batch.predictionsRemaining() == 0 {
		t.predictionPublisher.publishPredictionBatch(batch)
		return
	}
	t.pendingPredictions.addPendingPredictionBatch(time.Now(), batch)
	t.sendInferenceRequests(batch.allInferenceRequests())
}

//sendInferenceRequests sends InferenceRequests via NATS to 'inference-request' subject
func (t *tripUpdateProcessor) sendInferenceRequests(requests []*InferenceRequest) {
	timestamp := time.Now().Unix()
	for _, request := range requests {
		jsonData, err := request.jsonRequest(timestamp)
		if err != nil {
			t.log.Printf("Error marshalling inferenceRequest: %v, error:%v", request, err)
			return
		}
		bucket := request.MLModelId % int64(t.inferenceBuckets)
		subject := fmt.Sprintf("inference-request.%d", bucket)
		err = t.natsConn.Publish(subject, jsonData)
		if err != nil {
			t.log.Printf("Error sending inferenceRequest: %v, error:%v", request, err)
			return
		}
	}
}
