package gtfsmanager

import (
	"gitlab.trimet.org/transittracker/transitmon/business/data/gtfs"
	"reflect"
	"strings"
	"testing"
)

func testFloat64Pointer(f float64) *float64 {
	return &f
}

func Test_buildStopTime(t *testing.T) {

	tests := []struct {
		name       string
		csvContent string
		want       *gtfs.StopTime
		wantErr    bool
	}{
		{
			name: "stop_time parsed",
			csvContent: "trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled,timepoint,continuous_drop_off,continuous_pickup" +
				"\n10292960,06:53:02,06:53:02,10491,6,45th Ave,0,0,5543.4,0,,",
			want: &gtfs.StopTime{
				TripId:            "10292960",
				StopSequence:      6,
				StopId:            "10491",
				ArrivalTime:       (6 * 60 * 60) + (53 * 60) + 2,
				DepartureTime:     (6 * 60 * 60) + (53 * 60) + 2,
				ShapeDistTraveled: testFloat64Pointer(5543.4),
			},
			wantErr: false,
		},
		{
			name: "error on missing required field (stop_sequence)",
			csvContent: "trip_id,arrival_time,departure_time,stop_id,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled,timepoint,continuous_drop_off,continuous_pickup" +
				"\n10292960,06:53:02,06:53:02,10491,45th Ave,0,0,5543.4,0,,",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := makeGTFSFileParser(strings.NewReader(tt.csvContent), "calendar.txt")
			if err != nil {
				t.Errorf("Unable to make gtfsFileParser %s", err)
			}
			err = parser.nextLine()
			if err != nil {
				t.Errorf("Unable to move gtfsFileParser to first line %s", err)
			}
			got, err := buildStopTime(parser)
			if tt.wantErr {
				if err == nil {
					t.Errorf("%v: buildStopTime() produced no error, but we want one", tt.name)
					return
				}
				return
			} else if err != nil {
				t.Errorf("%v: buildStopTime() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildStopTime() got = %+v, want %+v", got, tt.want)
			}
		})
	}
}
