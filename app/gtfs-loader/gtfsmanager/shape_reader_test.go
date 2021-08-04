package gtfsmanager

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"reflect"
	"strings"
	"testing"
)

func Test_buildShape(t *testing.T) {

	tests := []struct {
		name       string
		csvContent string
		want       *gtfs.Shape
		wantErr    bool
	}{
		{
			name: "basic shape parsed",
			csvContent: "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled\n" +
				"460931,45.522879,-122.677388,1,0.0",
			want: &gtfs.Shape{
				ShapeId:           "460931",
				ShapePtLat:        45.522879,
				ShapePtLng:        -122.677388,
				ShapePtSequence:   1,
				ShapeDistTraveled: testFloat64Pointer(0.0),
			},
			wantErr: false,
		},
		{
			name: "error on missing required field (shape_pt_sequence)",
			csvContent: "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled\n" +
				"460931,45.522879,-122.677388,,0.0",
			wantErr: true,
		},
		{
			name: "shape parsed, optional shape_dist_traveled missing",
			csvContent: "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n" +
				"462210,45.498032,-122.685442,775",
			want: &gtfs.Shape{
				ShapeId:         "462210",
				ShapePtLat:      45.498032,
				ShapePtLng:      -122.685442,
				ShapePtSequence: 775,
			},
			wantErr: false,
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
			got, err := buildShape(parser)
			if tt.wantErr {
				if err == nil {
					t.Errorf("%v: buildShape() produced no error, but we want one", tt.name)
					return
				}
				return
			} else if err != nil {
				t.Errorf("%v: buildShape() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildShape() got = %+v, want %+v", got, tt.want)
			}
		})
	}
}
