package monitor

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"math"
	"testing"
)

func Test_findTripDistanceOfVehicleFromPosition(t *testing.T) {
	testTripOne := getFirstTestTripFromJson("trip_10900607_2021_07_22.json", t)
	stopOne := testTripOne.StopTimeInstances[0]
	stopTwo := testTripOne.StopTimeInstances[1]
	stopThree := testTripOne.StopTimeInstances[2]

	tests := []struct {
		name      string
		position  tripStopPosition
		want      *float64
		tolerance float64
	}{
		{
			name: "find a short distance from stop",
			position: tripStopPosition{
				atPreviousStop: false,
				tripInstance:   testTripOne,
				previousSTI:    stopOne,
				nextSTI:        stopTwo,
				latitude:       float32Ptr(45.426831), //about 45 feet
				longitude:      float32Ptr(-122.485909),
			},
			want:      float64Ptr(45.0),
			tolerance: 5.0,
		},
		{
			name: "Missing lat produces no result",
			position: tripStopPosition{
				atPreviousStop: false,
				tripInstance:   testTripOne,
				previousSTI:    stopOne,
				nextSTI:        stopTwo,
				longitude:      float32Ptr(-122.485909),
			},
			want: nil,
		},
		{
			name: "directly on top of stop",
			position: tripStopPosition{
				atPreviousStop: false,
				tripInstance:   testTripOne,
				previousSTI:    stopOne,
				nextSTI:        stopTwo,
				latitude:       float32Ptr(45.426947), //same values as first shape
				longitude:      float32Ptr(-122.485885),
			},
			want:      float64Ptr(0.0),
			tolerance: 1,
		},
		{
			name: "vehicle at a stop has no distance from that stop",
			position: tripStopPosition{
				atPreviousStop: true,
				tripInstance:   testTripOne,
				previousSTI:    stopOne,
				nextSTI:        stopTwo,
				latitude:       float32Ptr(45.426947), //same values as first shape
				longitude:      float32Ptr(-122.485885),
			},
			want:      float64Ptr(0.0),
			tolerance: 0,
		},
		{
			name: "vehicle close to next stop",
			position: tripStopPosition{
				atPreviousStop: false,
				tripInstance:   testTripOne,
				previousSTI:    stopTwo,
				nextSTI:        stopThree,
				latitude:       float32Ptr(45.427055), //close to the end of the pattern segment
				longitude:      float32Ptr(-122.497236),
			},
			want:      float64Ptr(3074.5),
			tolerance: 5,
		},
		{
			name: "vehicle too far from line produces no result",
			position: tripStopPosition{
				atPreviousStop: false,
				tripInstance:   testTripOne,
				previousSTI:    stopTwo,
				nextSTI:        stopThree,
				latitude:       float32Ptr(45.429282), //same values as first shape
				longitude:      float32Ptr(-122.494964),
			},
			want: nil,
		},
		{
			name: "vehicle beyond end of pattern is no further away than last position on pattern",
			position: tripStopPosition{
				atPreviousStop: false,
				tripInstance:   testTripOne,
				previousSTI:    stopTwo,
				nextSTI:        stopThree,
				latitude:       float32Ptr(45.426990), //same values as first shape
				longitude:      float32Ptr(-122.499481),
			},
			want:      float64Ptr(3105.5),
			tolerance: 0.1,
		},
		{
			name: "approximately in the middle of stops",
			position: tripStopPosition{
				atPreviousStop: false,
				tripInstance:   testTripOne,
				previousSTI:    stopTwo,
				nextSTI:        stopThree,
				latitude:       float32Ptr(45.427385),
				longitude:      float32Ptr(-122.493237),
			},
			want:      float64Ptr(2050),
			tolerance: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findTripDistanceOfVehicleFromPosition(&tt.position)
			if tt.want == nil {
				if got != nil {
					t.Errorf("expected nil result, but got %f", *got)
				}
			} else if got == nil {
				t.Errorf("expected %f result, but got nil", *tt.want)
			} else {
				diff := *got - *tt.want
				if math.Abs(diff) > tt.tolerance {
					t.Errorf("expected difference to be less than %f away from %f, got %f which is %f away", tt.tolerance, *tt.want, *got, diff)
				}
			}

		})
	}
}

func Test_simpleLatLngDistance(t *testing.T) {

	tests := []struct {
		name string
		lat1 float64
		lon1 float64
		lat2 float64
		lon2 float64
		want float64
	}{
		{
			name: "close together",
			lat1: 45.517539,
			lon1: -122.678221,
			lat2: 45.517462,
			lon2: -122.678283,
			want: 9.84504,
		},
		{
			name: "almost 3 kilometers",
			lat1: 45.522922,
			lon1: -122.675383,
			lat2: 45.497057,
			lon2: -122.681878,
			want: 2923.5,
		},
		{
			name: "between negative and positive longitudes",
			lat1: 51.215830,
			lon1: -0.009544,
			lat2: 51.215830,
			lon2: 0.020001,
			want: 2060.138586,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := simpleLatLngDistance(tt.lat1, tt.lon1, tt.lat2, tt.lon2)
			diff := got - tt.want
			if math.Abs(diff) >= .5 {
				t.Errorf("expected difference to be less than half a meter from %f, got %f", tt.want, diff)
			}
		})
	}
}

func Test_nearestLatLngToLineFromPoint(t *testing.T) {
	tests := []struct {
		name     string
		startLat float64
		startLon float64
		endLat   float64
		endLon   float64
		pointLat float64
		pointLon float64
		wantLat  float64
		wantLon  float64
	}{
		{
			name:     "Near middle",
			startLat: 45.542247,
			startLon: -122.661516,
			endLat:   45.542187,
			endLon:   -122.630768,
			pointLat: 45.548378,
			pointLon: -122.644338,
			wantLat:  45.542214,
			wantLon:  -122.644350,
		},
		{
			name:     "Nearer to start",
			startLat: 45.542247,
			startLon: -122.661516,
			endLat:   45.542187,
			endLon:   -122.630768,
			pointLat: 45.541225,
			pointLon: -122.655132,
			wantLat:  45.542235,
			wantLon:  -122.655130,
		},
		{
			name:     "Near equator",
			startLat: 0.003476,
			startLon: -78.451130,
			endLat:   -0.004764,
			endLon:   -78.451860,
			pointLat: 0.002017,
			pointLon: -78.449154,
			wantLat:  0.002202,
			wantLon:  -78.451243,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLat, gotLon := nearestLatLngToLineFromPoint(tt.startLat, tt.startLon, tt.endLat, tt.endLon, tt.pointLat, tt.pointLon)
			diff := simpleLatLngDistance(tt.wantLat, tt.wantLon, gotLat, gotLon)
			if math.Abs(diff) >= .2 {
				t.Errorf("nearestLatLngToLineFromPoint() produced result %f away from expected result", diff)
			}
		})
	}
}

func Test_calculateDelay(t *testing.T) {
	trip10856058 := getFirstTestTripFromJson("trip_10856058_2021_07_13.json", t)
	stopTwo := trip10856058.StopTimeInstances[1]
	type args struct {
		previousStop    *gtfs.StopTimeInstance
		secondsFromStop int
		timestamp       int64
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "10 seconds early at stop",
			args: args{
				previousStop:    stopTwo,
				secondsFromStop: 0,
				timestamp:       stopTwo.DepartureDateTime.Unix() - 10,
			},
			want: 10,
		},
		{
			name: "20 seconds early between stops",
			args: args{
				previousStop:    stopTwo,
				secondsFromStop: 10,
				timestamp:       stopTwo.DepartureDateTime.Unix() - 10,
			},
			want: 20,
		},
		{
			name: "10 seconds late at stop",
			args: args{
				previousStop:    stopTwo,
				secondsFromStop: 0,
				timestamp:       stopTwo.DepartureDateTime.Unix() + 10,
			},
			want: -10,
		},
		{
			name: "20 seconds late between stops",
			args: args{
				previousStop:    stopTwo,
				secondsFromStop: 10,
				timestamp:       stopTwo.DepartureDateTime.Unix() + 30,
			},
			want: -20,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateDelay(tt.args.previousStop, tt.args.secondsFromStop, tt.args.timestamp); got != tt.want {
				t.Errorf("calculateDelay() = %v, want %v", got, tt.want)
			}
		})
	}
}
