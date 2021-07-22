package gtfsmanager

import (
	"gitlab.trimet.org/transittracker/transitmon/business/data/gtfs"
	"reflect"
	"strings"
	"testing"
)

func testStringPtr(str string) *string {
	return &str
}

func Test_buildTrip(t *testing.T) {

	tests := []struct {
		name       string
		csvContent string
		want       *gtfs.Trip
		wantErr    bool
	}{
		{
			name: "trip parsed",
			csvContent: "route_id,service_id,trip_id,direction_id,block_id,shape_id,trip_type,wheelchair_accessible\n" +
				"1,W.581,10292960,0,169,460932,,1",
			want: &gtfs.Trip{
				TripId:        "10292960",
				RouteId:       "1",
				ServiceId:     "W.581",
				TripHeadsign:  nil,
				TripShortName: nil,
				BlockId:       testStringPtr("169"),
				ShapeId:       testStringPtr("460932"),
			},
			wantErr: false,
		},
		{
			name: "error on missing required field (route)",
			csvContent: "service_id,trip_id,direction_id,block_id,shape_id,trip_type,wheelchair_accessible\n" +
				"W.581,10292960,0,169,460932,,1",

			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := makeGTFSFileParser(strings.NewReader(tt.csvContent), "test.txt")
			if err != nil {
				t.Errorf("Unable to make gtfsFileParser %s", err)
			}
			err = parser.nextLine()
			if err != nil {
				t.Errorf("Unable to move gtfsFileParser to first line %s", err)
			}
			got, err := buildTrip(parser)
			if tt.wantErr {
				if err == nil {
					t.Errorf("%v: buildTrip() produced no error, but we want one", tt.name)
					return
				}
				return
			} else if err != nil {
				t.Errorf("%v: buildTrip() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildTrip() got = %+v, want %+v", got, tt.want)
			}
		})
	}
}
