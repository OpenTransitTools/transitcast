package monitor

import (
	"math"
	"testing"
)


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
		name  string
		startLat float64
		startLon float64
		endLat   float64
		endLon   float64
		pointLat float64
		pointLon float64
		wantLat  float64
		wantLon float64
	}{
		{
			name: "Near middle",
			startLat: 45.542247,
			startLon: -122.661516,
			endLat:   45.542187,
			endLon:   -122.630768,
			pointLat: 45.548378,
			pointLon: -122.644338,
			wantLat: 45.542214,
			wantLon: -122.644350,
		},
		{
			name: "Nearer to start",
			startLat: 45.542247,
			startLon: -122.661516,
			endLat:   45.542187,
			endLon:   -122.630768,
			pointLat: 45.541225,
			pointLon: -122.655132,
			wantLat: 45.542235,
			wantLon: -122.655130,
		},
		{
			name: "Near equator",
			startLat: 0.003476,
			startLon: -78.451130,
			endLat:   -0.004764,
			endLon:   -78.451860,
			pointLat: 0.002017,
			pointLon: -78.449154,
			wantLat: 0.002202,
			wantLon: -78.451243,
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

