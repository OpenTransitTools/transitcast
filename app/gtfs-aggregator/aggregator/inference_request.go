package aggregator

import (
	"encoding/json"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"time"
)

//InferenceRequest holds the parameters and features for the model runner to service
type InferenceRequest struct {
	RequestId        string `json:"request_id"`
	MLModelId        int64  `json:"ml_model_id"`
	Version          int    `json:"version"`
	segmentPredictor *segmentPredictor
	Features         inferenceFeatures
}

//jsonRequest marshals InferenceRequest into expected json bytes for sending to model runner
func (i *InferenceRequest) jsonRequest(timestamp int64) ([]byte, error) {
	m := map[string]interface{}{
		"request_id":  i.RequestId,
		"ml_model_id": i.MLModelId,
		"version":     i.Version,
		"features":    i.Features.featureArray(),
		"timestamp":   timestamp,
	}
	return json.Marshal(m)
}

//inferenceFeatures holds all elements used by the model to make an inference
type inferenceFeatures struct {
	month              int
	weekDay            int
	hour               int
	minute             int
	second             int
	holiday            bool
	scheduledSeconds   int
	scheduledTime      int
	delay              int
	distanceToStop     float64
	transitionFeatures []transitionFeature
}

//featureArray produces slice of floats for InferenceRequests
func (i *inferenceFeatures) featureArray() []float64 {
	holiday := 0.0
	if i.holiday {
		holiday = 1.0
	}
	features := []float64{
		float64(i.month),
		float64(i.weekDay),
		float64(i.hour),
		float64(i.minute),
		float64(i.second),
		holiday,
		float64(i.scheduledSeconds),
		float64(i.scheduledTime),
		float64(i.delay),
		i.distanceToStop,
	}

	for _, transition := range i.transitionFeatures {
		features = append(features, float64(transition.TransitionSeconds))
		features = append(features, float64(transition.TransitionAge))
	}
	return features
}

//transitionFeature holds all features representing stop to stop transitions
type transitionFeature struct {
	Description       string
	TransitionSeconds int
	TransitionAge     int
}

//buildTransitionFeature factory for transitionFeature
func buildTransitionFeature(stop1 *gtfs.StopTimeInstance,
	stop2 *gtfs.StopTimeInstance,
	osts *observedStopTransitions,
	at time.Time) transitionFeature {
	transitionName := stopTransitionName(stop1.StopId, stop2.StopId)
	lastOst := osts.getOst(stop1.StopId, stop2.StopId, at)
	if lastOst == nil {
		return transitionFeature{
			Description:       transitionName,
			TransitionSeconds: stop2.ArrivalTime - stop1.ArrivalTime, //schedule seconds
			TransitionAge:     7200,
		}
	}
	diff := at.Sub(lastOst.ObservedTime)
	return transitionFeature{
		Description:       transitionName,
		TransitionSeconds: lastOst.TravelSeconds,
		TransitionAge:     int(diff.Seconds()),
	}
}
