package monitor

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"reflect"
	"testing"
	"time"
)

func Test_collectBlockDeviations(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}
	testTrips := getTestTrips(time.Date(2021, 10, 14, 0, 0, 0, 0, location), t)

	type args struct {
		tripInstances   []*gtfs.TripInstance
		newTripPosition tripStopPosition
	}
	tests := []struct {
		name string
		args args
		want []*gtfs.TripDeviation
	}{
		{
			name: "Simple test at start of first trip",
			args: args{
				tripInstances: testTrips,
				newTripPosition: tripStopPosition{
					dataSetId:            testTrips[0].DataSetId,
					vehicleId:            "200",
					atPreviousStop:       true,
					tripInstance:         testTrips[0],
					lastTimestamp:        testDate("2021-10-14T09:00:00-07:00").Unix(),
					delay:                50,
					tripDistancePosition: float64Ptr(0.0),
				},
			},
			want: []*gtfs.TripDeviation{
				{
					DeviationTimestamp: testDate("2021-10-14T09:00:00-07:00"),
					TripProgress:       0,
					DataSetId:          testTrips[0].DataSetId,
					TripId:             testTrips[0].TripId,
					VehicleId:          "200",
					AtStop:             true,
					Delay:              50,
					RouteId:            "100",
				},
				{
					DeviationTimestamp: testDate("2021-10-14T09:00:00-07:00"),
					TripProgress:       -testTrips[0].TripDistance,
					DataSetId:          testTrips[0].DataSetId,
					TripId:             testTrips[1].TripId,
					VehicleId:          "200",
					AtStop:             true,
					Delay:              50,
					RouteId:            "100",
				},
			},
		},
		{
			name: "Located about half way through first trip",
			args: args{
				tripInstances: testTrips,
				newTripPosition: tripStopPosition{
					dataSetId:            testTrips[0].DataSetId,
					vehicleId:            "200",
					atPreviousStop:       false,
					tripInstance:         testTrips[0],
					lastTimestamp:        testDate("2021-10-14T09:44:00-07:00").Unix(),
					delay:                2,
					tripDistancePosition: float64Ptr(85936.0),
				},
			},
			want: []*gtfs.TripDeviation{
				{
					DeviationTimestamp: testDate("2021-10-14T09:44:00-07:00"),
					TripProgress:       85936.0,
					DataSetId:          testTrips[0].DataSetId,
					TripId:             testTrips[0].TripId,
					VehicleId:          "200",
					AtStop:             false,
					Delay:              2,
					RouteId:            "100",
				},
				{
					DeviationTimestamp: testDate("2021-10-14T09:44:00-07:00"),
					TripProgress:       -(testTrips[0].TripDistance - 85936.0),
					DataSetId:          testTrips[0].DataSetId,
					TripId:             testTrips[1].TripId,
					VehicleId:          "200",
					AtStop:             false,
					Delay:              2,
					RouteId:            "100",
				},
			},
		},
		{
			name: "Located on second trip, ignore earlier trip",
			args: args{
				tripInstances: testTrips,
				newTripPosition: tripStopPosition{
					dataSetId:            testTrips[0].DataSetId,
					vehicleId:            "200",
					atPreviousStop:       false,
					tripInstance:         testTrips[1],
					lastTimestamp:        testDate("2021-10-14T11:05:00-07:00").Unix(),
					delay:                2,
					tripDistancePosition: float64Ptr(500),
				},
			},
			want: []*gtfs.TripDeviation{
				{
					DeviationTimestamp: testDate("2021-10-14T11:05:00-07:00"),
					TripProgress:       500,
					DataSetId:          testTrips[0].DataSetId,
					TripId:             testTrips[1].TripId,
					VehicleId:          "200",
					AtStop:             false,
					Delay:              2,
					RouteId:            "100",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loadedTripInstancesByTripId := make(map[string]*gtfs.TripInstance)
			for _, trip := range tt.args.tripInstances {
				loadedTripInstancesByTripId[trip.TripId] = trip
			}
			got := collectBlockDeviations(loadedTripInstancesByTripId, &tt.args.newTripPosition)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("collectTripDeviations() "+
					"\ngot  = %+v,"+
					"\nwant = %+v",
					describeTripDeviationResults(got), describeTripDeviationResults(tt.want))
			}
		})
	}
}

func describeTripDeviationResults(results []*gtfs.TripDeviation) []string {
	gotDesc := make([]string, 0)
	for _, tripDeviation := range results {
		gotDesc = append(gotDesc, fmt.Sprintf("%+v", fmt.Sprintf("%+v", *tripDeviation)))
	}
	return gotDesc
}
