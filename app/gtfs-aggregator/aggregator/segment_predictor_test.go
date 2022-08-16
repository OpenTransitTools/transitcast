package aggregator

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/OpenTransitTools/transitcast/business/data/mlmodels"
	"math"
	"reflect"
	"testing"
	"time"
)

func Test_segmentPredictorFactory(t *testing.T) {

	modelMap := getTestModelMap(t, "trip_instance_1_stop_models.json", "trip_instance_1_tp_models.json")

	osts := makeObservedStopTransitions(3600)

	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}
	trip := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_1.json", t)

	type factoryArgs struct {
		modelMap                    map[string]*mlmodels.MLModel
		minimumRMSEModelImprovement float64
		minimumObservedStopCount    int
	}

	tests := []struct {
		name              string
		factoryArgs       factoryArgs
		stopTimeInstances []*gtfs.StopTimeInstance
		want              []*segmentPredictor
	}{
		{
			name: "empty models",
			factoryArgs: factoryArgs{
				modelMap:                    nil,
				minimumRMSEModelImprovement: 0,
			},
			stopTimeInstances: []*gtfs.StopTimeInstance{
				trip.StopTimeInstances[0], trip.StopTimeInstances[1], trip.StopTimeInstances[2],
			},
			want: []*segmentPredictor{
				{
					model: nil,
					stopTimeInstances: []*gtfs.StopTimeInstance{
						trip.StopTimeInstances[0], trip.StopTimeInstances[1],
					},
					useInference: false,
				},
				{
					model: nil,
					stopTimeInstances: []*gtfs.StopTimeInstance{
						trip.StopTimeInstances[1], trip.StopTimeInstances[2],
					},
					useInference: false,
				},
			},
		},
		{
			name: "use stop models when timepoint model under performs",
			factoryArgs: factoryArgs{
				modelMap:                    modelMap,
				minimumRMSEModelImprovement: 0.0,
			},
			stopTimeInstances: []*gtfs.StopTimeInstance{
				trip.StopTimeInstances[0], trip.StopTimeInstances[1], trip.StopTimeInstances[2],
			},
			want: []*segmentPredictor{
				{
					model: modelMap["A_B"],
					stopTimeInstances: []*gtfs.StopTimeInstance{
						trip.StopTimeInstances[0], trip.StopTimeInstances[1],
					},
					useInference: true,
				},
				{
					model: modelMap["B_C"],
					stopTimeInstances: []*gtfs.StopTimeInstance{
						trip.StopTimeInstances[1], trip.StopTimeInstances[2],
					},
					useInference: false,
				},
			},
		},
		{
			name: "use tp models when timepoint model performs",
			factoryArgs: factoryArgs{
				modelMap:                    modelMap,
				minimumRMSEModelImprovement: 0.0,
			},
			stopTimeInstances: []*gtfs.StopTimeInstance{
				trip.StopTimeInstances[2], trip.StopTimeInstances[3], trip.StopTimeInstances[4],
			},
			want: []*segmentPredictor{
				{
					model: modelMap["C_D_E"],
					stopTimeInstances: []*gtfs.StopTimeInstance{
						trip.StopTimeInstances[2], trip.StopTimeInstances[3], trip.StopTimeInstances[4],
					},
					useInference: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := makeSegmentPredictionFactory(tt.factoryArgs.modelMap, osts,
				tt.factoryArgs.minimumRMSEModelImprovement, 1)
			result := factory.makeSegmentPredictors(tt.stopTimeInstances)
			same, discrepancyDescription := segmentPredictorsAreTheSame(result, tt.want)
			if !same {
				t.Errorf("Mismatch = %s\n", discrepancyDescription)
			}
		})
	}

}

func segmentPredictorsAreTheSame(got []*segmentPredictor, want []*segmentPredictor) (bool, string) {
	if len(got) != len(want) {
		return false, fmt.Sprintf("len(got) = %d != len(*wantPendingPrediction) %d", len(got), len(want))
	}
	for i, s1 := range got {
		s2 := (want)[i]
		if s1.model != s2.model {
			return false, fmt.Sprintf("row %v, model %v != %v", i, describeModel(s1.model),
				describeModel(s2.model))
		}
		if s1.useInference != s2.useInference {
			return false, fmt.Sprintf("row %v, useInference %v != %v", i, s1.useInference, s2.useInference)
		}

	}
	return true, ""
}

func describeModel(model *mlmodels.MLModel) string {
	if model == nil {
		return "<nil model>"
	}
	return model.ModelName
}

func Test_segmentPredictor_applySegmentTime(t *testing.T) {

	osts := makeObservedStopTransitions(3600)

	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}
	trip1 := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_1.json", t)

	type fields struct {
		model             *mlmodels.MLModel
		osts              *observedStopTransitions
		stopTimeInstances []*gtfs.StopTimeInstance
	}
	type args struct {
		seconds            float64
		src                gtfs.PredictionSource
		predictionComplete bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*stopPrediction
	}{
		{
			name: "Single stop produces same time",
			fields: fields{
				model: nil,
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[0], trip1.StopTimeInstances[1],
				},
			},
			args: args{
				seconds:            float64(trip1.StopTimeInstances[1].ArrivalTime - trip1.StopTimeInstances[0].ArrivalTime),
				src:                gtfs.SchedulePrediction,
				predictionComplete: true,
			},
			want: []*stopPrediction{
				{
					fromStop:           trip1.StopTimeInstances[0],
					toStop:             trip1.StopTimeInstances[1],
					predictedTime:      1200,
					predictionSource:   gtfs.SchedulePrediction,
					predictionComplete: true,
				},
			},
		},
		{
			name: "Multiple stops produces proportional times",
			fields: fields{
				model: nil,
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[0], trip1.StopTimeInstances[1], trip1.StopTimeInstances[2],
				},
			},
			args: args{
				seconds:            2400 * 2, //twice the length of each stop
				src:                gtfs.SchedulePrediction,
				predictionComplete: true,
			},
			want: []*stopPrediction{
				{
					fromStop:           trip1.StopTimeInstances[0],
					toStop:             trip1.StopTimeInstances[1],
					predictedTime:      2400,
					predictionSource:   gtfs.SchedulePrediction,
					predictionComplete: true,
				},
				{
					fromStop:           trip1.StopTimeInstances[1],
					toStop:             trip1.StopTimeInstances[2],
					predictedTime:      2400,
					predictionSource:   gtfs.SchedulePrediction,
					predictionComplete: true,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &segmentPredictor{
				model:             tt.fields.model,
				osts:              tt.fields.osts,
				stopTimeInstances: tt.fields.stopTimeInstances,
			}
			got := s.applySegmentTime(tt.args.seconds, tt.args.src, tt.args.predictionComplete)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("applySegmentTime() got = %+v, wantPendingPrediction %+v", got, tt.want)
			}
		})
	}
}

func Test_segmentPredictor_predict(t *testing.T) {

	modelMap := getTestModelMap(t, "trip_instance_1_stop_models.json", "trip_instance_1_tp_models.json")

	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}

	stopBCOst := gtfs.ObservedStopTime{
		ObservedTime:  time.Date(2022, 5, 22, 12, 20, 10, 0, location),
		StopId:        "B",
		NextStopId:    "C",
		VehicleId:     "A",
		RouteId:       "A",
		TravelSeconds: 900,
	}

	stopEFOst := gtfs.ObservedStopTime{
		ObservedTime:  time.Date(2022, 5, 22, 12, 20, 10, 0, location),
		StopId:        "E",
		NextStopId:    "F",
		VehicleId:     "A",
		RouteId:       "A",
		TravelSeconds: 1250,
	}

	osts := makeObservedStopTransitions(3600)
	osts.newOST(&stopBCOst)
	osts.newOST(&stopEFOst)

	aDeviationTimestamp := time.Date(2022, 5, 22, 12, 30, 10, 0, location)

	trip1 := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_1.json", t)

	type fields struct {
		model             *mlmodels.MLModel
		osts              *observedStopTransitions
		stopTimeInstances []*gtfs.StopTimeInstance
		useInference      bool
		useStatistics     bool
	}
	type args struct {
		deviation *gtfs.TripDeviation
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *predictionResult
	}{
		{
			name: "false on useInference and useStatistics results in no InferenceRequest and scheduled times",
			fields: fields{
				model: nil,
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[0], trip1.StopTimeInstances[1],
				},
				useInference:  false,
				useStatistics: false,
			},
			args: args{
				deviation: &gtfs.TripDeviation{
					TripProgress: 0,
					TripId:       trip1.TripId,
					VehicleId:    "A",
				},
			},
			want: &predictionResult{
				inferenceRequest: nil,
				stopPredictions: []*stopPrediction{
					{
						fromStop:           trip1.StopTimeInstances[0],
						toStop:             trip1.StopTimeInstances[1],
						predictedTime:      float64(trip1.StopTimeInstances[1].ArrivalTime - trip1.StopTimeInstances[0].ArrivalTime),
						predictionSource:   gtfs.SchedulePrediction,
						predictionComplete: true,
					},
				},
			},
		},
		{
			name: "false on useInference and useStatistics results in no InferenceRequest and scheduled times on timepoints",
			fields: fields{
				model: modelMap["A_B_C"],
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[0], trip1.StopTimeInstances[1], trip1.StopTimeInstances[2],
				},
				useInference:  false,
				useStatistics: false,
			},
			args: args{
				deviation: &gtfs.TripDeviation{
					TripProgress: 0,
					TripId:       trip1.TripId,
					VehicleId:    "A",
				},
			},
			want: &predictionResult{
				inferenceRequest: nil,
				stopPredictions: []*stopPrediction{
					{
						fromStop:           trip1.StopTimeInstances[0],
						toStop:             trip1.StopTimeInstances[1],
						predictedTime:      float64(trip1.StopTimeInstances[1].ArrivalTime - trip1.StopTimeInstances[0].ArrivalTime),
						predictionSource:   gtfs.SchedulePrediction,
						predictionComplete: true,
					},
					{
						fromStop:           trip1.StopTimeInstances[1],
						toStop:             trip1.StopTimeInstances[2],
						predictedTime:      float64(trip1.StopTimeInstances[2].ArrivalTime - trip1.StopTimeInstances[1].ArrivalTime),
						predictionSource:   gtfs.SchedulePrediction,
						predictionComplete: true,
					},
				},
			},
		},
		{
			name: "false on useInference and true on useStatistics results in no InferenceRequest and stat based times",
			fields: fields{
				model: modelMap["A_B"],
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[0], trip1.StopTimeInstances[1],
				},
				useInference:  false,
				useStatistics: true,
			},
			args: args{
				deviation: &gtfs.TripDeviation{
					TripProgress: 0,
					TripId:       trip1.TripId,
					VehicleId:    "A",
				},
			},
			want: &predictionResult{
				inferenceRequest: nil,
				stopPredictions: []*stopPrediction{
					{
						fromStop:           trip1.StopTimeInstances[0],
						toStop:             trip1.StopTimeInstances[1],
						predictedTime:      *modelMap["A_B"].Average,
						predictionSource:   gtfs.StopStatisticsPrediction,
						predictionComplete: true,
					},
				},
			},
		},
		{
			name: "false on useInference and useStatistics results in no InferenceRequest and stat based times on timepoints",
			fields: fields{
				model: modelMap["A_B_C"],
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[0], trip1.StopTimeInstances[1], trip1.StopTimeInstances[2],
				},
				useInference:  false,
				useStatistics: true,
			},
			args: args{
				deviation: &gtfs.TripDeviation{
					TripProgress: 0,
					TripId:       trip1.TripId,
					VehicleId:    "A",
				},
			},
			want: &predictionResult{
				inferenceRequest: nil,
				stopPredictions: []*stopPrediction{
					{
						fromStop:           trip1.StopTimeInstances[0],
						toStop:             trip1.StopTimeInstances[1],
						predictedTime:      float64(1800),
						predictionSource:   gtfs.TimepointStatisticsPrediction,
						predictionComplete: true,
					},
					{
						fromStop:           trip1.StopTimeInstances[1],
						toStop:             trip1.StopTimeInstances[2],
						predictedTime:      float64(1800),
						predictionSource:   gtfs.TimepointStatisticsPrediction,
						predictionComplete: true,
					},
				},
			},
		},
		{
			name: "true on useInference and true on useStatistics results in InferenceRequest and stat based times, no ost",
			fields: fields{
				model: modelMap["A_B"],
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[0], trip1.StopTimeInstances[1],
				},
				useInference:  true,
				useStatistics: true,
			},
			args: args{
				deviation: &gtfs.TripDeviation{
					TripProgress:       0,
					TripId:             trip1.TripId,
					VehicleId:          "A",
					DeviationTimestamp: aDeviationTimestamp,
					Delay:              1000,
				},
			},
			want: &predictionResult{
				inferenceRequest: &InferenceRequest{
					MLModelId: modelMap["A_B"].MLModelId,
					Version:   0,
					Features: inferenceFeatures{
						month:            5,
						weekDay:          0,
						hour:             12,
						minute:           30,
						second:           10,
						holiday:          false,
						scheduledSeconds: 1200,
						scheduledTime:    44400,
						delay:            1000,
						distanceToStop:   1000.0,
						transitionFeatures: []transitionFeature{
							{
								Description:       "A_B",
								TransitionSeconds: 1200, //scheduled time
								TransitionAge:     7200, //default
							},
						},
					},
				},
				stopPredictions: []*stopPrediction{
					{
						fromStop:           trip1.StopTimeInstances[0],
						toStop:             trip1.StopTimeInstances[1],
						predictedTime:      *modelMap["A_B"].Average,
						predictionSource:   gtfs.StopStatisticsPrediction,
						predictionComplete: false,
					},
				},
			},
		},
		{
			name: "true on useInference and true on useStatistics results in InferenceRequest and stat based times, one ost",
			fields: fields{
				model: modelMap["E_F"],
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[4], trip1.StopTimeInstances[5],
				},
				useInference:  true,
				useStatistics: true,
			},
			args: args{
				deviation: &gtfs.TripDeviation{
					TripProgress:       0,
					TripId:             trip1.TripId,
					VehicleId:          "A",
					DeviationTimestamp: aDeviationTimestamp,
					Delay:              1010,
				},
			},
			want: &predictionResult{
				inferenceRequest: &InferenceRequest{
					MLModelId: modelMap["E_F"].MLModelId,
					Version:   0,
					Features: inferenceFeatures{
						month:            5,
						weekDay:          0,
						hour:             12,
						minute:           30,
						second:           10,
						holiday:          false,
						scheduledSeconds: 600,
						scheduledTime:    48600,
						delay:            1010,
						distanceToStop:   5000.0,
						transitionFeatures: []transitionFeature{
							{
								Description:       "E_F",
								TransitionSeconds: stopEFOst.TravelSeconds, //time from stopEF
								TransitionAge:     10 * 60,                 //time difference between stopEFOst and aTripDeviation
							},
						},
					},
				},
				stopPredictions: []*stopPrediction{
					{
						fromStop:           trip1.StopTimeInstances[4],
						toStop:             trip1.StopTimeInstances[5],
						predictedTime:      *modelMap["E_F"].Average,
						predictionSource:   gtfs.StopStatisticsPrediction,
						predictionComplete: false,
					},
				},
			},
		},
		{
			name: "true on useInference and true on useStatistics, but trip located after stop results in no InferenceRequest and stat based times, no ost",
			fields: fields{
				model: modelMap["E_F"],
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[4], trip1.StopTimeInstances[5],
				},
				useInference:  true,
				useStatistics: true,
			},
			args: args{
				deviation: &gtfs.TripDeviation{
					TripProgress:       trip1.StopTimeInstances[5].ShapeDistTraveled + 1.0,
					TripId:             trip1.TripId,
					VehicleId:          "A",
					DeviationTimestamp: aDeviationTimestamp,
					Delay:              1010,
				},
			},
			want: &predictionResult{
				inferenceRequest: nil,
				stopPredictions: []*stopPrediction{
					{
						fromStop:           trip1.StopTimeInstances[4],
						toStop:             trip1.StopTimeInstances[5],
						predictedTime:      *modelMap["E_F"].Average,
						predictionSource:   gtfs.StopStatisticsPrediction,
						predictionComplete: true,
					},
				},
			},
		},

		{
			name: "multistop, true on useInference and true on useStatistics results in InferenceRequest and stat based times, one ost",
			fields: fields{
				model: modelMap["E_F"],
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[4], trip1.StopTimeInstances[5],
				},
				useInference:  true,
				useStatistics: true,
			},
			args: args{
				deviation: &gtfs.TripDeviation{
					TripProgress:       0,
					TripId:             trip1.TripId,
					VehicleId:          "A",
					DeviationTimestamp: aDeviationTimestamp,
					Delay:              1010,
				},
			},
			want: &predictionResult{
				inferenceRequest: &InferenceRequest{
					MLModelId: modelMap["E_F"].MLModelId,
					Version:   0,
					Features: inferenceFeatures{
						month:            5,
						weekDay:          0,
						hour:             12,
						minute:           30,
						second:           10,
						holiday:          false,
						scheduledSeconds: 600,
						scheduledTime:    48600,
						delay:            1010,
						distanceToStop:   5000.0,
						transitionFeatures: []transitionFeature{
							{
								Description:       "E_F",
								TransitionSeconds: stopEFOst.TravelSeconds, //time from stopEF
								TransitionAge:     10 * 60,                 //time difference between stopEFOst and aTripDeviation
							},
						},
					},
				},
				stopPredictions: []*stopPrediction{
					{
						fromStop:           trip1.StopTimeInstances[4],
						toStop:             trip1.StopTimeInstances[5],
						predictedTime:      *modelMap["E_F"].Average,
						predictionSource:   gtfs.StopStatisticsPrediction,
						predictionComplete: false,
					},
				},
			},
		},
		{
			name: "false on useInference and useStatistics results in no InferenceRequest and scheduled times on timepoints",
			fields: fields{
				model: modelMap["A_B_C"],
				osts:  osts,
				stopTimeInstances: []*gtfs.StopTimeInstance{
					trip1.StopTimeInstances[0], trip1.StopTimeInstances[1], trip1.StopTimeInstances[2],
				},
				useInference:  true,
				useStatistics: true,
			},
			args: args{
				deviation: &gtfs.TripDeviation{
					TripProgress:       100.0,
					TripId:             trip1.TripId,
					VehicleId:          "A",
					DeviationTimestamp: aDeviationTimestamp,
					Delay:              900,
				},
			},
			want: &predictionResult{
				inferenceRequest: &InferenceRequest{
					MLModelId: modelMap["E_F"].MLModelId,
					Version:   0,
					Features: inferenceFeatures{
						month:            5,
						weekDay:          0,
						hour:             12,
						minute:           30,
						second:           10,
						holiday:          false,
						scheduledSeconds: 2400,
						scheduledTime:    45600,
						delay:            900,
						distanceToStop:   1900.0,
						transitionFeatures: []transitionFeature{
							{
								Description:       "A_B",
								TransitionSeconds: trip1.StopTimeInstances[1].ArrivalTime - trip1.StopTimeInstances[0].ArrivalTime, //scheduled_time
								TransitionAge:     7200,                                                                            //default
							},
							{
								Description:       "B_C",
								TransitionSeconds: stopBCOst.TravelSeconds, //time from stopEF
								TransitionAge:     10 * 60,                 //time difference between stopEFOst and aTripDeviation
							},
						},
					},
				},
				stopPredictions: []*stopPrediction{
					{
						fromStop:           trip1.StopTimeInstances[0],
						toStop:             trip1.StopTimeInstances[1],
						predictedTime:      1800,
						predictionSource:   gtfs.TimepointStatisticsPrediction,
						predictionComplete: false,
					},
					{
						fromStop:           trip1.StopTimeInstances[1],
						toStop:             trip1.StopTimeInstances[2],
						predictedTime:      1800,
						predictionSource:   gtfs.TimepointStatisticsPrediction,
						predictionComplete: false,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		holidayCalendar := makeTransitHolidayCalendar()
		t.Run(tt.name, func(t *testing.T) {
			s := &segmentPredictor{
				model:             tt.fields.model,
				osts:              tt.fields.osts,
				stopTimeInstances: tt.fields.stopTimeInstances,
				useInference:      tt.fields.useInference,
				useStatistics:     tt.fields.useStatistics,
				holidayCalendar:   holidayCalendar,
			}
			got := s.predict(tt.args.deviation)
			// if we wantPendingPrediction an inferenceRequest add the reference to the segmentPredictor here.
			if tt.want.inferenceRequest != nil {
				tt.want.inferenceRequest.segmentPredictor = s
			}
			match, mismatchDesc := predictionResultMatches(got, tt.want)
			if !match {
				t.Errorf(mismatchDesc)
			}
		})
	}
}

func predictionResultMatches(got *predictionResult, want *predictionResult) (bool, string) {

	if got == nil {
		if want == nil {
			return true, ""
		}
		return false, fmt.Sprintf("Got nil, but expected %+v", want)
	} else if want == nil {
		return false, fmt.Sprintf("Expected nil, but got %+v", got)
	}
	if !reflect.DeepEqual(got.inferenceRequest, want.inferenceRequest) {
		return false, fmt.Sprintf("inferenceRequest doesn't match \ngot =  %+v, \nwantPendingPrediction = %+v", got.inferenceRequest, want.inferenceRequest)
	}
	if len(got.stopPredictions) != len(want.stopPredictions) {
		return false, fmt.Sprintf("len(got) = %d != len(*wantPendingPrediction) %d",
			len(got.stopPredictions), len(want.stopPredictions))
	}
	for i, s1 := range got.stopPredictions {
		s2 := (want.stopPredictions)[i]
		if !reflect.DeepEqual(s1.fromStop, s2.fromStop) {
			return false, stopPredictionMismatchDesc(i, "FromStop", s1, s2)
		}
		if !reflect.DeepEqual(s1.toStop, s2.toStop) {
			return false, stopPredictionMismatchDesc(i, "ToStop", s1, s2)
		}
		if s1.predictedTime != s2.predictedTime {
			return false, stopPredictionMismatchDesc(i, "PredictedTime", s1, s2)
		}
		if s1.predictionSource != s2.predictionSource {
			return false, stopPredictionMismatchDesc(i, "PredictionSource", s1, s2)
		}
		diff := s1.predictedTime - s2.predictedTime
		if math.Abs(diff) > 0.1 {
			return false, stopPredictionMismatchDesc(i, "PredictedTime", s1, s2)
		}

	}
	return true, ""
}

func stopPredictionMismatchDesc(row int, fieldName string, got *stopPrediction, want *stopPrediction) string {
	return fmt.Sprintf("stopPrediction row %v mismatch on %v\n got:  %+v\n wantPendingPrediction: %+v", row, fieldName, got, want)
}
