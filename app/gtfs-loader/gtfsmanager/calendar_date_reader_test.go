package gtfsmanager

import (
	"gitlab.trimet.org/transittracker/transitmon/business/data/gtfs"
	"reflect"
	"strings"
	"testing"
)

func Test_buildCalendarDate(t *testing.T) {
	tests := []struct {
		name       string
		csvContent string
		wantErr    bool
		want       *gtfs.CalendarDate
	}{
		{
			name: "calendar_dates.txt no errors",
			csvContent: "service_id,date,exception_type\n" +
				"S.581,20201031,1",
			wantErr: false,
			want: &gtfs.CalendarDate{
				ServiceId:     "S.581",
				Date:          getTestDate("20201031"),
				ExceptionType: 1,
			},
		},
		{
			name: "calendar_dates.txt error, missing exception_type value",
			csvContent: "service_id,date\n" +
				"S.581,20201031",
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
			got, err := buildCalendarDate(parser)
			if tt.wantErr {
				if err == nil {
					t.Errorf("%v: buildCalendarDate() produced no error, but we want one", tt.name)
					return
				}
				return
			} else if err != nil {
				t.Errorf("%v: buildCalendarDate() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildCalendarDate() got = %+v, want %+v", got, tt.want)
			}
		})
	}
}
