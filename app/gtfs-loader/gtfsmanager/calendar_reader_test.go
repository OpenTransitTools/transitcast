package gtfsmanager

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"reflect"
	"strings"
	"testing"
	"time"
)

func getTestDatePointer(str string) *time.Time {
	result := getTestDate(str)
	return &result
}

func Test_buildCalendar(t *testing.T) {
	tests := []struct {
		name       string
		csvContent string
		wantErr    bool
		want       *gtfs.Calendar
	}{
		{
			name: "calendar.txt no errors",
			csvContent: "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n" +
				"WKDY,1,1,1,1,1,0,0,20190211,20200210\n",
			wantErr: false,
			want: &gtfs.Calendar{
				DataSetId: 0,
				ServiceId: "WKDY",
				Monday:    1,
				Tuesday:   1,
				Wednesday: 1,
				Thursday:  1,
				Friday:    1,
				Saturday:  0,
				Sunday:    0,
				StartDate: getTestDatePointer("20190211"),
				EndDate:   getTestDatePointer("20200210"),
			},
		},
		{
			name: "calendar.txt error, missing monday value",
			csvContent: "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n" +
				"WKDY,,1,1,1,1,0,0,20190211,20200210\n",
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
			got, err := buildCalendar(parser)
			if tt.wantErr {
				if err == nil {
					t.Errorf("%v: buildCalendar() produced no error, but we want one", tt.name)
					return
				}
				return
			} else if err != nil {
				t.Errorf("%v: buildCalendar() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildCalendar() got = %+v, want %+v", got, tt.want)
			}
		})
	}
}
