package aggregator

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
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
			got := makeTripPredictor(tt.args.tripInstance, tt.args.factory)
			same, discrepancyDescription := segmentPredictorsAreTheSame(got.segmentPredictors, tt.want.segmentPredictors)
			if !same {
				t.Errorf("Mismatch = %s\n", discrepancyDescription)
			}
		})
	}
}
