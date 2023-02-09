package aggregator

import (
	"encoding/json"
	"github.com/nats-io/nats.go"
	logger "log"
	"os"
	"sync"
	"time"
)

// InferenceResponse holds the results of an InferenceRequest sent back from the model runner
type InferenceResponse struct {
	RequestId  string  `json:"request_id"`
	MLModelId  int64   `json:"ml_model_id"`
	Version    int     `json:"version"`
	Prediction float64 `json:"prediction"`
	Error      string  `json:"error"`
	Timestamp  int64   `json:"timestamp"`
}

// startInferenceResponseListener starts a listener on nats connection and applies these results to the predictions in
// pendingPredictionsCollection. When an inference response completes a prediction the result is sent to
// the predictionPublisher as a completed TripUpdate.
func startInferenceResponseListener(
	log *logger.Logger,
	wg *sync.WaitGroup,
	natsConn *nats.Conn,
	shutdownSignal chan bool,
	pendingPredictions *pendingPredictionsCollection,
	predictionPublisher *predictionPublisher) {
	wg.Add(1)
	defer wg.Done()

	ch := make(chan *nats.Msg, 64)
	log.Printf("Subscribing to inference-response on nats: %v\n", natsConn.Servers())
	sub, err := natsConn.ChanSubscribe("inference-response", ch)
	if err != nil {
		log.Printf("Unable to establish subscription to nats server: %v\n", err)
		os.Exit(1)
	}
	//clean up nats
	defer func() {
		log.Printf("Unsubscribing to inference-response in ObservedStopTransitionListener\n")
		err = sub.Unsubscribe()
		if err != nil {
			log.Printf("Error when attempting to unsubscribe: %v\n", err)
		}
	}()

	handler := makeInferenceResultHandler(log, pendingPredictions, predictionPublisher)

	for {
		select {
		case msg := <-ch:
			handler.applyInferenceResultFromMsg(msg)
			break
		case <-shutdownSignal:
			log.Printf("exiting inference response listener on shutdown signal\n")
			return
		}
	}
}

// inferenceResultHandler applies inference results to predictions in pendingPredictionsCollection
type inferenceResultHandler struct {
	log                 *logger.Logger
	pendingPredictions  *pendingPredictionsCollection
	predictionPublisher *predictionPublisher
}

// makeInferenceResultHandler builds inferenceResultHandler
func makeInferenceResultHandler(log *logger.Logger,
	pendingPredictions *pendingPredictionsCollection,
	predictionPublisher *predictionPublisher) *inferenceResultHandler {
	return &inferenceResultHandler{
		log:                 log,
		pendingPredictions:  pendingPredictions,
		predictionPublisher: predictionPublisher,
	}
}

// applyInferenceResultFromMsg unmarshal nats message and applies result to pending prediction
func (i *inferenceResultHandler) applyInferenceResultFromMsg(msg *nats.Msg) {
	inferenceResponse := InferenceResponse{}
	err := json.Unmarshal(msg.Data, &inferenceResponse)
	if err != nil {
		i.log.Printf("error parsing InferenceResponse: %v, payload:%s", err, string(msg.Data))
		return
	}
	if len(inferenceResponse.Error) > 0 {
		i.log.Printf("InferenceResponse RequestId:%s error:%s", inferenceResponse.RequestId,
			inferenceResponse.Error)
		return
	}
	i.applyInferenceResult(inferenceResponse)
}

// applyInferenceResult finds pending prediction, applies the InferenceResponse,
// if this completes the prediction passes the prediction on to be published by predictionPublisher
func (i *inferenceResultHandler) applyInferenceResult(response InferenceResponse) {
	batch, prediction, inferenceRequest, err := i.pendingPredictions.getPendingPrediction(time.Now(), response)
	if err != nil {
		i.log.Printf("error applying inference response:%s, error:%v", response.RequestId, err)
		return
	}
	err = prediction.applyInferenceResponse(inferenceRequest.segmentPredictor, response.Prediction)
	if err != nil {
		i.log.Printf("error applying inference response:%s, error:%v", response.RequestId, err)
		return
	}
	remainingPredictions := batch.predictionsRemaining()

	if remainingPredictions == 0 {
		i.predictionPublisher.publishPredictionBatch(batch)
	}
}
