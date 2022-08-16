package aggregator

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/OpenTransitTools/transitcast/business/data/mlmodels"
	"github.com/jmoiron/sqlx"
	"sync"
	"time"
)

//tripPredictorsCollection factory and cache of tripPredictions
type tripPredictorsCollection struct {
	db               *sqlx.DB
	predictorFactory *segmentPredictorFactory
	expireSeconds    int
	locker           *tripPredictorsLocker
}

//makeTripPredictorsCollection builds tripPredictorsCollection
func makeTripPredictorsCollection(db *sqlx.DB,
	osts *observedStopTransitions,
	minimumRMSEModelImprovement float64,
	minimumObservedStopCount int,
	tripPredictorExpireSeconds int) (*tripPredictorsCollection, error) {
	modelsByName, err := mlmodels.GetAllCurrentMLModelsByName(db, true)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve models in makeTripPredictorsCollection: %w", err)
	}
	predictorFactory := makeSegmentPredictionFactory(modelsByName, osts,
		minimumRMSEModelImprovement, minimumObservedStopCount)
	return &tripPredictorsCollection{
		db:               db,
		predictorFactory: predictorFactory,
		expireSeconds:    tripPredictorExpireSeconds,
		locker:           makeTripPredictorLocker(),
	}, nil
}

//retrieveTripPredictor finds the tripPredictor for use on gtfs.TripDeviation in cache or loads it if not in cache
func (t *tripPredictorsCollection) retrieveTripPredictor(deviation *gtfs.TripDeviation) (*tripPredictor, error) {
	predictorMapId := makePredictorMapId(deviation.DataSetId, deviation.TripId)
	predictor := t.locker.retrieve(predictorMapId)
	if predictor != nil {
		return predictor, nil
	}
	tripInstance, err := gtfs.GetTripInstance(t.db, deviation.DataSetId, deviation.TripId,
		deviation.DeviationTimestamp, 60*60*8)
	if err != nil {
		return nil, err
	}
	predictor = makeTripPredictor(tripInstance, t.predictorFactory)
	t.locker.put(predictorMapId, predictor)
	return predictor, nil
}

//removeExpiredPredictors removes all expired predictors from cache as of "now"
func (t *tripPredictorsCollection) removeExpiredPredictors(now time.Time) (int, int) {
	return t.locker.removeExpiredPredictors(now, t.expireSeconds)
}

//tripPredictorsLocker thread safe wrapper around map containing tripPredictor for use by tripPredictorsCollection
type tripPredictorsLocker struct {
	mu               sync.Mutex
	tripPredictorMap map[string]*tripPredictor
}

//makeTripPredictorLocker builds tripPredictorsLocker
func makeTripPredictorLocker() *tripPredictorsLocker {
	return &tripPredictorsLocker{
		mu:               sync.Mutex{},
		tripPredictorMap: make(map[string]*tripPredictor),
	}
}

func (t *tripPredictorsLocker) retrieve(predictorMapId string) *tripPredictor {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tripPredictorMap[predictorMapId]
}

func (t *tripPredictorsLocker) put(predictorMapId string, predictor *tripPredictor) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tripPredictorMap[predictorMapId] = predictor
}

//removeExpiredPredictors builds new tripPredictor with only items that have not expired as of "expireSeconds"
//a tripPredictor has expired if its final stop's arrival time is "expireSeconds" after "now"
func (t *tripPredictorsLocker) removeExpiredPredictors(now time.Time, expireSeconds int) (int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	startSize := len(t.tripPredictorMap)
	newMap := make(map[string]*tripPredictor)
	expireBefore := now.Add(time.Duration(-expireSeconds) * time.Second)
	for key, predictor := range t.tripPredictorMap {
		lastStop := predictor.tripInstance.LastStopTimeInstance()
		if lastStop != nil && lastStop.ArrivalDateTime.After(expireBefore) {
			newMap[key] = predictor
		}
	}
	t.tripPredictorMap = newMap
	return startSize, len(newMap)
}

//makePredictorMapId returns string key for tripPredictor map used by tripPredictorsCollection and tripPredictorsLocker
func makePredictorMapId(dataSetId int64, tripId string) string {
	return fmt.Sprintf("%d:%s", dataSetId, tripId)
}

//tripPredictor a tripPrediction factory for a gtfs.TripInstance that can be reused for every gtfs.TripDeviation
//for that trip
type tripPredictor struct {
	tripInstance      *gtfs.TripInstance
	segmentPredictors []*segmentPredictor
}

//makeTripPredictor builds tripPredictor
func makeTripPredictor(tripInstance *gtfs.TripInstance,
	factory *segmentPredictorFactory) *tripPredictor {

	segmentPredictors := make([]*segmentPredictor, 0)

	//for each timepoint pair create segmentPredictor
	var segmentStops []*gtfs.StopTimeInstance
	for _, stop := range tripInstance.StopTimeInstances {

		segmentStops = append(segmentStops, stop)
		if len(segmentStops) > 1 && stop.IsTimepoint() {
			segmentPredictors = append(segmentPredictors, factory.makeSegmentPredictors(segmentStops)...)
			segmentStops = []*gtfs.StopTimeInstance{stop}
		}
	}

	predictor := tripPredictor{
		tripInstance:      tripInstance,
		segmentPredictors: segmentPredictors,
	}
	return &predictor
}

//predict produces tripPrediction and InferenceRequest from a gtfs.TripDeviation
func (p *tripPredictor) predict(tripDeviation *gtfs.TripDeviation) (*tripPrediction, []*InferenceRequest) {
	stopPredictions := make([]*stopPrediction, 0)
	inferenceRequests := make([]*InferenceRequest, 0)
	for _, sp := range p.segmentPredictors {
		result := sp.predict(tripDeviation)
		if result.inferenceRequest != nil {
			inferenceRequests = append(inferenceRequests, result.inferenceRequest)
		}
		stopPredictions = append(stopPredictions, result.stopPredictions...)
	}
	prediction := makeTripPrediction(tripDeviation, p.tripInstance, stopPredictions)
	return prediction, inferenceRequests
}