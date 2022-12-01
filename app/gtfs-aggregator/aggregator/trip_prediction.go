package aggregator

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"sync"
)

// stopPrediction contains results of a prediction for vehicle movement from one stop to the next stop on a trip
type stopPrediction struct {
	fromStop              *gtfs.StopTimeInstance
	toStop                *gtfs.StopTimeInstance
	predictedTime         float64
	predictionSource      gtfs.PredictionSource
	stopUpdateDisposition stopUpdateDisposition
	predictionComplete    bool
}

// stopUpdateDisposition indicates how stopUpdate relates to a stopPrediction,
// if it's in the past, at the stop or it's a future stop
type stopUpdateDisposition int32

const (
	Undefined stopUpdateDisposition = iota
	AtStop
	PastStop
	FutureStop
)

func makeStopUpdateDisposition(tripProgress float64, stopDistance float64) stopUpdateDisposition {
	if consideredAtStop(tripProgress, stopDistance) {
		return AtStop
	}
	if stopDistance > tripProgress {
		return FutureStop
	}
	return PastStop
}

// tripPrediction contains results of predicting a trip. Can also replace initial stats based stopPredictions
// with newer inference based predictions once inference has been completed.
type tripPrediction struct {
	tripDeviation      *gtfs.TripDeviation
	mu                 sync.Mutex
	stopPredictions    []*stopPrediction
	tripInstance       *gtfs.TripInstance
	pendingPredictions int
}

// makeTripPrediction builds tripPrediction
func makeTripPrediction(tripDeviation *gtfs.TripDeviation,
	trip *gtfs.TripInstance,
	stopPredictions []*stopPrediction) *tripPrediction {

	//count predictions still pending
	pendingPredictions := 0
	for _, prediction := range stopPredictions {
		if !prediction.predictionComplete {
			pendingPredictions++
		}
	}
	return &tripPrediction{
		tripDeviation:      tripDeviation,
		mu:                 sync.Mutex{},
		stopPredictions:    stopPredictions,
		tripInstance:       trip,
		pendingPredictions: pendingPredictions,
	}
}

// addInferencePrediction finds and replaces stopPrediction with inference based prediction
// this method is intended to be called by applyInferenceResponse
func (tp *tripPrediction) addInferencePrediction(prediction *stopPrediction) error {
	for i, sp := range tp.stopPredictions {
		if sp.fromStop.StopSequence == prediction.fromStop.StopSequence &&
			sp.toStop.StopSequence == prediction.toStop.StopSequence {
			tp.stopPredictions[i] = prediction
			tp.pendingPredictions--
			return nil
		}
	}
	return fmt.Errorf("unable to find stop sequence to apply prediction for %v", prediction)
}

// applyInferenceResponse applies inferenceResponse against segmentPredictor and replaces the generated stopPredictions
// in this tripPrediction
func (tp *tripPrediction) applyInferenceResponse(predictor *segmentPredictor, inferenceResponse float64) error {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	predictions := predictor.applyInferenceResponse(inferenceResponse, tp.tripDeviation.TripProgress)
	for _, prediction := range predictions {
		err := tp.addInferencePrediction(prediction)
		if err != nil {
			return err
		}
	}
	return nil
}

// predictionsRemaining returns the number of stopPredictions awaiting inference responses in this tripPrediction
// if this returns 0 this tripPrediction is finished and can be published
func (tp *tripPrediction) predictionsRemaining() int {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	return tp.pendingPredictions
}
