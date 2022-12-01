package aggregator

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"strings"
	"testing"
	"time"
)

func Test_makeTripPredictor(t *testing.T) {

	modelMap := getTestModelMap(t, "trip_instance_1_stop_models.json", "trip_instance_1_tp_models.json")

	osts := makeObservedStopTransitions(3600)

	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}
	trip1 := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_1.json", t)

	segmentPredictorFactory1 := makeSegmentPredictionFactory(modelMap, osts, 0.0, 1)

	type args struct {
		tripInstance *gtfs.TripInstance
		factory      *segmentPredictorFactory
	}
	tests := []struct {
		name string
		args args
		want *tripPredictor
	}{
		{
			name: "build tripInstance",
			args: args{
				tripInstance: trip1,
				factory:      segmentPredictorFactory1,
			},
			want: &tripPredictor{
				tripInstance: trip1,
				segmentPredictors: []*segmentPredictor{
					{
						model:        modelMap["A_B"],
						useInference: true,
					},
					{
						model:        modelMap["B_C"],
						useInference: false,
					},
					{
						model:        modelMap["C_D_E"],
						useInference: true,
					},
					{
						model:        modelMap["E_F"],
						useInference: true,
					},
					{
						model:        modelMap["F_G"],
						useInference: false,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := makeTripPredictor(tt.args.tripInstance, tt.args.factory, 60)
			same, discrepancyDescription := segmentPredictorsAreTheSame(got.segmentPredictors, tt.want.segmentPredictors)
			if !same {
				t.Errorf("Mismatch = %s\n", discrepancyDescription)
			}
		})
	}
}

func Test_tripPredictor_predict(t *testing.T) {

	modelMap := getTestModelMap(t, "trip_instance_1_stop_models.json", "trip_instance_1_tp_models.json")

	osts := makeObservedStopTransitions(3600)

	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}
	trip := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_1.json", t)

	timeAt1200 := time.Date(2022, 5, 22, 12, 0, 0, 0, location)
	timeAt1101 := time.Date(2022, 5, 22, 11, 1, 0, 0, location)
	timeAt1310 := time.Date(2022, 5, 22, 13, 10, 0, 0, location)

	segmentPredictionFactory := makeSegmentPredictionFactory(modelMap, osts,
		0.0, 1)

	tests := []struct {
		name                     string
		maximumPredictionMinutes int
		tripDeviation            *gtfs.TripDeviation
		want                     *tripPrediction
	}{
		{
			name:                     "Up to timepoint predicted stops",
			maximumPredictionMinutes: 60,
			tripDeviation: &gtfs.TripDeviation{
				DeviationTimestamp: timeAt1200,
				TripId:             trip.TripId,
			},
			want: &tripPrediction{
				tripDeviation: &gtfs.TripDeviation{
					DeviationTimestamp: timeAt1200,
					TripId:             trip.TripId,
				},
				stopPredictions: []*stopPrediction{
					{
						fromStop:         trip.StopTimeInstances[0],
						toStop:           trip.StopTimeInstances[1],
						predictionSource: gtfs.StopStatisticsPrediction,
					},
					{
						fromStop:         trip.StopTimeInstances[1],
						toStop:           trip.StopTimeInstances[2],
						predictionSource: gtfs.SchedulePrediction,
					},
					{
						fromStop:         trip.StopTimeInstances[2],
						toStop:           trip.StopTimeInstances[3],
						predictionSource: gtfs.TimepointStatisticsPrediction,
					},
					{
						fromStop:         trip.StopTimeInstances[3],
						toStop:           trip.StopTimeInstances[4],
						predictionSource: gtfs.TimepointStatisticsPrediction,
					},
					{
						fromStop:         trip.StopTimeInstances[4],
						toStop:           trip.StopTimeInstances[5],
						predictionSource: gtfs.NoFurtherPredictions,
					},
				},
				tripInstance:       trip,
				pendingPredictions: 3,
			},
		},
		{
			name:                     "Only first stop",
			maximumPredictionMinutes: 60,
			tripDeviation: &gtfs.TripDeviation{
				DeviationTimestamp: timeAt1101,
				TripId:             trip.TripId,
			},
			want: &tripPrediction{
				tripDeviation: &gtfs.TripDeviation{
					DeviationTimestamp: timeAt1101,
					TripId:             trip.TripId,
				},
				stopPredictions: []*stopPrediction{
					{
						fromStop:         trip.StopTimeInstances[0],
						toStop:           trip.StopTimeInstances[1],
						predictionSource: gtfs.StopStatisticsPrediction,
					},
					{
						fromStop:         trip.StopTimeInstances[1],
						toStop:           trip.StopTimeInstances[2],
						predictionSource: gtfs.NoFurtherPredictions,
					},
				},
				tripInstance:       trip,
				pendingPredictions: 1,
			},
		},
		{
			name:                     "First stop and skip passed stops",
			maximumPredictionMinutes: 60,
			tripDeviation: &gtfs.TripDeviation{
				DeviationTimestamp: timeAt1310,
				TripId:             trip.TripId,
				TripProgress:       4500.0,
			},
			want: &tripPrediction{
				tripDeviation: &gtfs.TripDeviation{
					DeviationTimestamp: timeAt1310,
					TripId:             trip.TripId,
				},
				stopPredictions: []*stopPrediction{
					{
						fromStop:         trip.StopTimeInstances[0],
						toStop:           trip.StopTimeInstances[1],
						predictionSource: gtfs.StopStatisticsPrediction,
					},
					{
						fromStop:         trip.StopTimeInstances[1],
						toStop:           trip.StopTimeInstances[2],
						predictionSource: gtfs.SchedulePrediction,
					},
					{
						fromStop:         trip.StopTimeInstances[2],
						toStop:           trip.StopTimeInstances[3],
						predictionSource: gtfs.TimepointStatisticsPrediction,
					},
					{
						fromStop:         trip.StopTimeInstances[3],
						toStop:           trip.StopTimeInstances[4],
						predictionSource: gtfs.TimepointStatisticsPrediction,
					},
					{
						fromStop:         trip.StopTimeInstances[4],
						toStop:           trip.StopTimeInstances[5],
						predictionSource: gtfs.StopStatisticsPrediction,
					},
					{
						fromStop:         trip.StopTimeInstances[5],
						toStop:           trip.StopTimeInstances[6],
						predictionSource: gtfs.SchedulePrediction,
					},
				},
				tripInstance:       trip,
				pendingPredictions: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := makeTripPredictor(trip, segmentPredictionFactory, tt.maximumPredictionMinutes)

			got, _ := p.predict(tt.tripDeviation)
			err = checkForExpectedTripPrediction(got, tt.want)
			if err != nil {
				t.Errorf("%s", err)
			}
		})
	}
}

func checkForExpectedTripPrediction(got *tripPrediction, want *tripPrediction) error {
	if got.pendingPredictions != want.pendingPredictions || !compareInterestingStopPredictions(got, want) {
		return fmt.Errorf("predict()\ngot = %+v, \nwant=%+v", sprintTripPrediction(got), sprintTripPrediction(want))
	}
	return nil
}

func compareInterestingStopPredictions(got *tripPrediction, want *tripPrediction) bool {
	if len(got.stopPredictions) != len(want.stopPredictions) {
		return false
	}
	for i, gotStop := range got.stopPredictions {
		wantStop := want.stopPredictions[i]
		if gotStop.toStop.StopId != wantStop.toStop.StopId ||
			gotStop.fromStop.StopId != wantStop.fromStop.StopId ||
			gotStop.predictionSource != wantStop.predictionSource {
			return false
		}
	}
	return true
}

func sprintTripPrediction(p *tripPrediction) string {
	if p == nil {
		return "nil"
	}
	var parts []string
	for _, sp := range p.stopPredictions {
		parts = append(parts,
			fmt.Sprintf("{fromStopId:%s toStopId:%s predictionSource:%d}",
				sp.fromStop.StopId, sp.toStop.StopId, sp.predictionSource))
	}
	return fmt.Sprintf("TripPrediction TripId:%s pendingPredictions:%d stopPredictions:\n%s",
		p.tripDeviation.TripId, p.pendingPredictions, strings.Join(parts, "\n"))
}

func Test_tripIsWithinPredictionRange(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}

	tripInstance := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_1.json", t)

	type args struct {
		tripDeviation            *gtfs.TripDeviation
		maximumPredictionMinutes int
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Trip should be included",
			args: args{
				tripDeviation: &gtfs.TripDeviation{
					DeviationTimestamp: time.Date(2022, 5, 22, 12, 0, 0, 0, location),
				},
				maximumPredictionMinutes: 60,
			},
			want: true,
		},
		{
			name: "Trip should be not be included an hour before departure",
			args: args{
				tripDeviation: &gtfs.TripDeviation{
					DeviationTimestamp: time.Date(2022, 5, 22, 11, 0, 0, 0, location),
				},
				maximumPredictionMinutes: 60,
			},
			want: false,
		},
		{
			name: "Trip should be not be when its past time for the trip to begin",
			args: args{
				tripDeviation: &gtfs.TripDeviation{
					DeviationTimestamp: time.Date(2022, 5, 22, 13, 0, 0, 0, location),
				},
				maximumPredictionMinutes: 60,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tripIsWithinPredictionRange(tt.args.tripDeviation, tripInstance, tt.args.maximumPredictionMinutes); got != tt.want {
				t.Errorf("tripIsWithinPredictionRange() = %v, want %v", got, tt.want)
			}
		})
	}
}
