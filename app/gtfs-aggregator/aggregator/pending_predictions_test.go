package aggregator

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"reflect"
	"testing"
	"time"
)

func TestPendingPredictionsCollection_getPendingPrediction(t *testing.T) {

	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}

	twelve := time.Date(2022, 5, 22, 12, 00, 00, 0, location)

	tripPredictionAtTwelve := &tripPrediction{
		tripDeviation: &gtfs.TripDeviation{CreatedAt: twelve},
		stopPredictions: []*stopPrediction{
			{
				fromStop: &gtfs.StopTimeInstance{
					StopTime: gtfs.StopTime{
						TripId:            "trip1",
						StopSequence:      1,
						StopId:            "A",
						ArrivalTime:       1000,
						DepartureTime:     1000,
						ShapeDistTraveled: 0,
					},
					FirstStop: true,
				},
				toStop: &gtfs.StopTimeInstance{
					StopTime: gtfs.StopTime{
						TripId:            "trip1",
						StopSequence:      2,
						StopId:            "B",
						ArrivalTime:       2000,
						DepartureTime:     2000,
						ShapeDistTraveled: 1000,
					},
					FirstStop: true,
				},
				predictedTime:      10,
				predictionComplete: false,
			},
		},
		tripInstance: &gtfs.TripInstance{
			Trip: gtfs.Trip{TripId: "trip1"},
		},
		pendingPredictions: 1,
	}

	inferenceRequest1 := &InferenceRequest{
		MLModelId: 1,
		Version:   1,
		Features:  inferenceFeatures{},
	}

	predictionBatch1 := makePredictionBatch(twelve, "101")

	predictionBatch1.addPendingTripPrediction(tripPredictionAtTwelve, []*InferenceRequest{inferenceRequest1})

	pendingPrediction1 := &pendingPredictionBatch{

		expireTime:      twelve,
		predictionBatch: predictionBatch1,
	}

	type fields struct {
		pendingList        []*pendingPredictionBatch
		expireAfterSeconds int
	}
	type args struct {
		response InferenceResponse
		at       time.Time
	}
	tests := []struct {
		name                 string
		fields               fields
		args                 args
		wantPredictionBatch  *predictionBatch
		wantTripPrediction   *tripPrediction
		wantInferenceRequest *InferenceRequest
		wantErr              bool
	}{
		{
			name: "basic retrieval",
			fields: fields{
				pendingList: []*pendingPredictionBatch{
					pendingPrediction1,
				},
				expireAfterSeconds: 3,
			},
			args: args{
				response: InferenceResponse{
					RequestId:  inferenceRequest1.RequestId,
					MLModelId:  1,
					Version:    1,
					Prediction: 1.0,
					Error:      "",
				},
				at: twelve,
			},
			wantPredictionBatch:  predictionBatch1,
			wantTripPrediction:   tripPredictionAtTwelve,
			wantInferenceRequest: inferenceRequest1,
			wantErr:              false,
		},
		{
			name: "expired retrieval",
			fields: fields{
				pendingList: []*pendingPredictionBatch{
					pendingPrediction1,
				},
				expireAfterSeconds: 3,
			},
			args: args{
				response: InferenceResponse{
					RequestId:  inferenceRequest1.RequestId,
					MLModelId:  1,
					Version:    1,
					Prediction: 1.0,
					Error:      "",
				},
				at: twelve.Add(time.Duration(4) * time.Second),
			},
			wantPredictionBatch:  nil,
			wantTripPrediction:   nil,
			wantInferenceRequest: nil,
			wantErr:              true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &pendingPredictionsCollection{
				pendingList:        tt.fields.pendingList,
				expirationDuration: time.Duration(tt.fields.expireAfterSeconds) * time.Second,
			}
			gotPredictionBatch, gotTripPrediction, gotInferenceRequest, err := p.getPendingPrediction(tt.args.at, tt.args.response)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPendingPrediction() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPredictionBatch, tt.wantPredictionBatch) {
				t.Errorf("getPendingPrediction() gotPredictionBatch = %v, wantPredictionBatch %v", gotPredictionBatch, tt.wantPredictionBatch)
			}
			if !reflect.DeepEqual(gotTripPrediction, tt.wantTripPrediction) {
				t.Errorf("getPendingPrediction() gotPendingPrediction = %v, wantPendingPrediction %v", gotTripPrediction, tt.wantTripPrediction)
			}
			if !reflect.DeepEqual(gotInferenceRequest, tt.wantInferenceRequest) {
				t.Errorf("getPendingPrediction() gotInferenceRequest = %v, wantPendingPrediction %v", gotInferenceRequest, tt.wantInferenceRequest)
			}
		})
	}
}
