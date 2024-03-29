package aggregator

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/OpenTransitTools/transitcast/business/data/mlmodels"
	"time"
)

// predictionResult holds the result of a segmentPredictor prediction
type predictionResult struct {
	inferenceRequest *InferenceRequest
	stopPredictions  []*stopPrediction
}

// segmentPredictor responsible for generating predictions and InferenceRequests for segments of a trip
// (one or more stops)
type segmentPredictor struct {
	model             *mlmodels.MLModel
	osts              *observedStopTransitions
	stopTimeInstances []*gtfs.StopTimeInstance
	useInference      bool
	useStatistics     bool
	holidayCalendar   *transitHolidayCalendar
}

// scheduledTime returns the scheduled arrival time of the first stop in this segment in seconds since midnight
func (s *segmentPredictor) scheduledTime() int {
	return s.stopTimeInstances[len(s.stopTimeInstances)-1].ArrivalTime - s.stopTimeInstances[0].ArrivalTime
}

func (s *segmentPredictor) firstScheduledStopTimeInstances() (*gtfs.StopTimeInstance, *gtfs.StopTimeInstance) {
	return s.stopTimeInstances[0], s.stopTimeInstances[1]
}

// relevantForDistance returns true if this segment is relevant for predictions after the distance on the trip
func (s *segmentPredictor) relevantForDistance(distance float64) bool {
	lastIndex := len(s.stopTimeInstances) - 1
	if lastIndex <= 0 {
		return false
	}
	return distance <= s.stopTimeInstances[lastIndex].ShapeDistTraveled
}

// predict produces predictionResult for this segment. If predictionResult.inferenceRequest is non-nil
// then this segment needs am inference response before the prediction is complete
func (s *segmentPredictor) predict(tripDeviation *gtfs.TripDeviation) *predictionResult {
	needsInference := s.useInference && s.relevantForDistance(tripDeviation.TripProgress)
	result := predictionResult{}
	segmentTime, source := s.statisticalSegmentTime()
	result.stopPredictions = s.applySegmentTime(segmentTime, source, !needsInference, tripDeviation.TripProgress)

	if needsInference {
		result.inferenceRequest = s.buildInferenceRequest(tripDeviation)
	}
	return &result
}

// buildInferenceRequest creates an InferenceRequest for tripDeviation on its segment
func (s *segmentPredictor) buildInferenceRequest(tripDeviation *gtfs.TripDeviation) *InferenceRequest {

	at := tripDeviation.DeviationTimestamp

	transitions := make([]transitionFeature, 0)
	var previousStopTime *gtfs.StopTimeInstance
	for _, stopTime := range s.stopTimeInstances {
		if previousStopTime != nil {
			transitions = append(transitions,
				buildTransitionFeature(previousStopTime, stopTime, s.osts, at))
		}
		previousStopTime = stopTime
	}

	segmentScheduleSeconds := previousStopTime.ArrivalTime - s.stopTimeInstances[0].ArrivalTime

	return &InferenceRequest{
		MLModelId:        s.model.MLModelId,
		Version:          s.model.Version,
		segmentPredictor: s,
		Features: inferenceFeatures{
			month:              int(at.Month()),
			weekDay:            int(at.Weekday()),
			hour:               at.Hour(),
			minute:             at.Minute(),
			second:             at.Second(),
			holiday:            s.isHoliday(at),
			scheduledSeconds:   segmentScheduleSeconds,
			scheduledTime:      previousStopTime.ArrivalTime,
			delay:              tripDeviation.Delay,
			distanceToStop:     previousStopTime.ShapeDistTraveled - tripDeviation.TripProgress,
			transitionFeatures: transitions,
		},
	}
}

// statisticalSegmentTime returns time to use for the segment prediction when inference is not used
// and returns the gtfs.PredictionSource describing where this value derived from
func (s *segmentPredictor) statisticalSegmentTime() (float64, gtfs.PredictionSource) {
	if s.useStatistics && s.model != nil && s.model.Average != nil {
		if len(s.stopTimeInstances) > 2 {
			return *s.model.Average, gtfs.TimepointStatisticsPrediction
		}
		return *s.model.Average, gtfs.StopStatisticsPrediction
	}
	return float64(s.scheduledTime()), gtfs.SchedulePrediction
}

// applyInferenceResponse uses inferenceResponse value among the segments stops and returns resulting
// stopPrediction slice
func (s *segmentPredictor) applyInferenceResponse(inferenceResponse float64,
	tripProgress float64) []*stopPrediction {
	src := gtfs.TimepointMLPrediction
	if len(s.stopTimeInstances) <= 2 {
		src = gtfs.StopMLPrediction
	}
	return s.applySegmentTime(inferenceResponse, src, true, tripProgress)
}

// applySegmentTime distributes seconds across stopTimeInstances and returns stopPrediction slice
// with gtfs.PredictionSource
// seconds is the number of seconds predicted to have been traveled for this segment, it may be derived from
// any of the sources defined by the gtfs.PredictionSource
// predictionComplete should be false when an inference request is required
func (s *segmentPredictor) applySegmentTime(seconds float64,
	src gtfs.PredictionSource,
	predictionComplete bool,
	tripProgress float64) []*stopPrediction {

	results := make([]*stopPrediction, 0)

	allStopsScheduledTime := s.scheduledTime()
	var previousStop *gtfs.StopTimeInstance
	for _, stop := range s.stopTimeInstances {
		if previousStop != nil {
			results = append(results, &stopPrediction{
				fromStop:              previousStop,
				toStop:                stop,
				predictedTime:         calcStopSegmentTime(previousStop, stop, allStopsScheduledTime, seconds),
				predictionSource:      src,
				stopUpdateDisposition: makeStopUpdateDisposition(tripProgress, stop.ShapeDistTraveled),
				predictionComplete:    predictionComplete,
			})
		}
		previousStop = stop
	}
	return results
}

// isHoliday returns true if "at" is on an observed holiday
func (s *segmentPredictor) isHoliday(at time.Time) bool {
	return s.holidayCalendar.isHoliday(at)
}

// calcStopSegmentTime calculates the amount of time to be applied from "totalPredictedTime" for travel between
// "stop1" and "stop2", where the "totalPredictedTime" is the prediction for a trip segment that's
// scheduled for "allStopsScheduledTime" seconds, of which "stop1" and "stop2" are a part.
func calcStopSegmentTime(stop1 *gtfs.StopTimeInstance,
	stop2 *gtfs.StopTimeInstance,
	allStopsScheduledTime int,
	totalPredictedTime float64) float64 {
	stopPairTime := stop2.ArrivalTime - stop1.ArrivalTime
	stopPairRatio := float64(stopPairTime) / float64(allStopsScheduledTime)
	ourSegmentTime := totalPredictedTime * stopPairRatio
	return ourSegmentTime
}

// segmentPredictorFactory creates segmentPredictor from loaded mlmodels.MLModel
type segmentPredictorFactory struct {
	modelByName                 map[string]*mlmodels.MLModel
	osts                        *observedStopTransitions
	minimumRMSEModelImprovement float64
	minimumObservedStopCount    int
	holidayCalendar             *transitHolidayCalendar
	makePredictions             bool
	useStatistics               bool
}

// makeSegmentPredictionFactory builds segmentPredictorFactory
func makeSegmentPredictionFactory(modelByName map[string]*mlmodels.MLModel,
	osts *observedStopTransitions,
	minimumRMSEModelImprovement float64,
	minimumObservedStopCount int,
	makePredictions bool,
	useStatistics bool) *segmentPredictorFactory {

	factory := segmentPredictorFactory{
		modelByName:                 modelByName,
		osts:                        osts,
		minimumRMSEModelImprovement: minimumRMSEModelImprovement,
		minimumObservedStopCount:    minimumObservedStopCount,
		holidayCalendar:             makeTransitHolidayCalendar(),
		makePredictions:             makePredictions,
		useStatistics:               useStatistics,
	}

	return &factory
}

// makeSegmentPredictors given a series of stopTimeInstances create segmentPredictor, preferring timepoint based
// models over stop to stop based models.
func (f *segmentPredictorFactory) makeSegmentPredictors(
	stopTimeInstances []*gtfs.StopTimeInstance) []*segmentPredictor {

	results := make([]*segmentPredictor, 0)

	//check if entire segment can be done with the timepoint predictor
	timePointModelName := mlmodels.GetModelNameForStopTimeInstances(stopTimeInstances)
	tpModel, ok := f.modelByName[timePointModelName]
	if ok && f.shouldUseModelToPredict(tpModel) {
		return append(results, f.makeSegmentPredictor(tpModel, stopTimeInstances))
	}

	return f.makeStopSegmentPredictors(stopTimeInstances)
}

// makeStopSegmentPredictors create slice of segmentPredictor with stop to stop based models for gtfs.StopTimeInstance
func (f *segmentPredictorFactory) makeStopSegmentPredictors(stopTimeInstances []*gtfs.StopTimeInstance) []*segmentPredictor {
	results := make([]*segmentPredictor, 0)

	var lastStop *gtfs.StopTimeInstance
	for _, stop := range stopTimeInstances {
		if lastStop != nil {
			stopTimePair := []*gtfs.StopTimeInstance{lastStop, stop}
			stopModel := f.modelByName[mlmodels.GetModelNameForStopTimeInstances(stopTimePair)]
			results = append(results, f.makeSegmentPredictor(stopModel, stopTimePair))
		}
		lastStop = stop
	}
	return results
}

// makeSegmentPredictor makes a segmentPredictor with mlModel for slice of gtfs.StopTimeInstance
func (f *segmentPredictorFactory) makeSegmentPredictor(mlModel *mlmodels.MLModel,
	stopTimeInstances []*gtfs.StopTimeInstance,
) *segmentPredictor {
	return &segmentPredictor{
		model:             mlModel,
		osts:              f.osts,
		stopTimeInstances: stopTimeInstances,
		useInference:      f.shouldUseModelToPredict(mlModel),
		useStatistics:     f.shouldUseStatisticsToPredict(mlModel),
		holidayCalendar:   f.holidayCalendar,
	}
}

// shouldUseModelToPredict returns true if mlModel is suitable for inference
func (f *segmentPredictorFactory) shouldUseModelToPredict(mlModel *mlmodels.MLModel) bool {
	return f.makePredictions &&
		mlModel != nil &&
		mlModel.TrainedTimestamp != nil &&
		mlModel.AvgRMSE-mlModel.MLRMSE >= f.minimumRMSEModelImprovement
}

// shouldUseStatisticsToPredict returns true if mlModel can be used for predictions based on average travel times
func (f *segmentPredictorFactory) shouldUseStatisticsToPredict(mlModel *mlmodels.MLModel) bool {
	return f.useStatistics &&
		mlModel != nil &&
		mlModel.ObservedStopCount != nil &&
		*mlModel.ObservedStopCount > f.minimumObservedStopCount
}
