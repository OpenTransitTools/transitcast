package aggregator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//holds structs related to predictions that are in process of being completed.

// predictionBatch holds all predictions for a vehicle and its current and upcoming trips
type predictionBatch struct {
	id                     string
	createdAt              time.Time
	pendingTripPredictions []*pendingTripPrediction
}

// makePredictionBatch builds predictionBatch
func makePredictionBatch(at time.Time, vehicleId string) *predictionBatch {
	return &predictionBatch{
		id:        makePredictionsBatchId(at, vehicleId),
		createdAt: at,
	}
}

// predictionsRemaining returns the number of predictions awaiting inference responses in this batch
func (p *predictionBatch) predictionsRemaining() int {
	remaining := 0
	for _, pendingTrip := range p.pendingTripPredictions {

		remaining += pendingTrip.tripPrediction.predictionsRemaining()
	}
	return remaining
}

// addPendingTripPrediction files tripPrediction and its inferenceRequests
func (p *predictionBatch) addPendingTripPrediction(tripPrediction *tripPrediction,
	inferenceRequests []*InferenceRequest) {

	for _, inferenceRequest := range inferenceRequests {
		inferenceRequest.RequestId = makePredictionRequestId(p.id, tripPrediction, inferenceRequest)
	}
	p.pendingTripPredictions = append(p.pendingTripPredictions, &pendingTripPrediction{
		tripPrediction:    tripPrediction,
		inferenceRequests: inferenceRequests,
	})
}

// findInferenceRequest looks for a tripPrediction and InferenceRequest in this batch
func (p *predictionBatch) findInferenceRequest(requestId *predictionIdParts) (*tripPrediction, *InferenceRequest) {
	for _, prediction := range p.pendingTripPredictions {
		if prediction.tripPrediction.tripInstance.TripId == requestId.tripId {
			return prediction.tripPrediction, prediction.findInferenceRequest(requestId)
		}
	}
	return nil, nil
}

// orderedTripPredictions returns this batch's tripPredictions ordered by the trip start times
func (p *predictionBatch) orderedTripPredictions() []*tripPrediction {
	var results []*tripPrediction
	for _, pending := range p.pendingTripPredictions {
		results = append(results, pending.tripPrediction)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].tripInstance.StartTime < results[j].tripInstance.StartTime
	})
	return results
}

// allInferenceRequests returns slice of all InferenceRequests for this batch
func (p *predictionBatch) allInferenceRequests() []*InferenceRequest {
	var results []*InferenceRequest
	for _, pending := range p.pendingTripPredictions {
		results = append(results, pending.inferenceRequests...)
	}
	return results
}

// pendingTripPrediction contains tripPrediction and it's InferenceRequests
type pendingTripPrediction struct {
	tripPrediction    *tripPrediction
	inferenceRequests []*InferenceRequest
}

// findInferenceRequest returns the InferenceRequest associated with a requests predictionIdParts
func (t *pendingTripPrediction) findInferenceRequest(requestId *predictionIdParts) *InferenceRequest {
	for _, request := range t.inferenceRequests {
		if request.MLModelId == requestId.mlModelId &&
			request.Version == requestId.mlModelVersion {
			return request
		}
	}
	return nil
}

// pendingPredictionBatch wraps a predictionBatch and provides an expiration time so the batch can be cleared
// if not completed in time
type pendingPredictionBatch struct {
	expireTime      time.Time
	predictionBatch *predictionBatch
}

// pendingPredictionsCollection contains and manages all predictionBatch structs, and allows for them to be expired
type pendingPredictionsCollection struct {
	mu                 sync.Mutex
	pendingList        []*pendingPredictionBatch
	expirationDuration time.Duration
}

// makePendingPredictionsCollection builds pendingPredictionsCollection
func makePendingPredictionsCollection(expireAfterSeconds int) *pendingPredictionsCollection {
	return &pendingPredictionsCollection{
		mu:                 sync.Mutex{},
		pendingList:        make([]*pendingPredictionBatch, 0),
		expirationDuration: time.Duration(expireAfterSeconds) * time.Second,
	}
}

// addPendingPredictionBatch store a predictionBatch for later completion when InferenceResponses have been received
func (p *pendingPredictionsCollection) addPendingPredictionBatch(at time.Time, batch *predictionBatch) {

	p.mu.Lock()
	defer p.mu.Unlock()

	newPrediction := pendingPredictionBatch{
		expireTime:      at.Add(p.expirationDuration),
		predictionBatch: batch,
	}
	p.pendingList = append(p.pendingList, &newPrediction)
}

// getPendingPrediction for an InferenceResponse, retrieve its non-expired predictionBatch, tripPrediction,
// and InferenceRequest
func (p *pendingPredictionsCollection) getPendingPrediction(at time.Time,
	response InferenceResponse) (*predictionBatch, *tripPrediction, *InferenceRequest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	requestIds, err := extractPredictionIdParts(response.RequestId)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, request := range p.pendingList {
		if request.predictionBatch.id == requestIds.predictionBatchId {
			if request.expireTime.Before(at) {
				return nil, nil, nil, fmt.Errorf("inference request has expired for %v", response)
			}
			prediction, inferenceRequest := request.predictionBatch.findInferenceRequest(requestIds)
			if prediction == nil || inferenceRequest == nil {
				return nil, nil, nil, fmt.Errorf("unable to find inference request for %v", response)
			}
			return request.predictionBatch, prediction, inferenceRequest, nil
		}
	}
	return nil, nil, nil, fmt.Errorf("unable to find inference request for %v", response)
}

// removeExpiredPredictions remove all expired predictionBatch that have expired. Called by a background cleanup routine
// returns slice of expired predictionBatch and size of current predictionBatch in collection
func (p *pendingPredictionsCollection) removeExpiredPredictions(at time.Time) ([]*predictionBatch, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var expiredList []*predictionBatch
	var newPendingList []*pendingPredictionBatch
	for _, pending := range p.pendingList {
		if pending.expireTime.After(at) {
			newPendingList = append(newPendingList, pending)
		} else {
			expiredList = append(expiredList, pending.predictionBatch)
		}
	}
	p.pendingList = newPendingList

	return expiredList, len(p.pendingList)
}

// makePredictionsBatchId builds an identifier for use in a predictionBatch
func makePredictionsBatchId(at time.Time, vehicleId string) string {
	//replace underscores and dashes from vehicleId, so they don't clash with our own prediction strings
	vehicleId = strings.ReplaceAll(vehicleId, "_", "~")
	vehicleId = strings.ReplaceAll(vehicleId, "-", "~")
	return fmt.Sprintf("%s_%d", vehicleId, at.UnixMilli())
}

// makePredictionRequestId builds an identifier for use in a InferenceRequest
func makePredictionRequestId(predictionsBatchId string,
	tripPrediction *tripPrediction,
	inferenceRequest *InferenceRequest) string {
	return fmt.Sprintf("%s-%s-%d-%d",
		predictionsBatchId,
		tripPrediction.tripInstance.TripId,
		inferenceRequest.MLModelId,
		inferenceRequest.Version)
}

// predictionIdParts contains the identifiable parts of a InferenceRequest.RequestId or InferenceResponse.RequestId,
// for use looking up the pending prediction in pendingPredictionsCollection
type predictionIdParts struct {
	predictionBatchId string
	tripId            string
	mlModelId         int64
	mlModelVersion    int
}

// extractPredictionIdParts returns a predictionIdParts given a InferenceRequest.RequestId or InferenceResponse.RequestId
func extractPredictionIdParts(requestId string) (*predictionIdParts, error) {
	split := strings.Split(requestId, "-")
	if len(split) < 4 {
		return nil, fmt.Errorf("unable to extract tripPredictionRequestId part from %s", requestId)
	}
	mlModelId, err := strconv.ParseInt(split[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to extract mlModelId part from %s, error: %w", requestId, err)
	}
	mlModelVersion, err := strconv.ParseInt(split[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to extract mlModelVersion part from %s, error: %w", requestId, err)
	}
	return &predictionIdParts{
		predictionBatchId: split[0],
		tripId:            split[1],
		mlModelId:         mlModelId,
		mlModelVersion:    int(mlModelVersion),
	}, nil
}
