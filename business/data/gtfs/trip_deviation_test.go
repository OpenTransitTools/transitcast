package gtfs

import (
	"reflect"
	"testing"
	"time"
)

func TestTripDeviation_SchedulePosition(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}
	twelvePm := time.Date(2022, 5, 22, 12, 0, 0, 0, location)
	type fields struct {
		DeviationTimestamp time.Time
		Delay              int
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{
			name: "zero delay",
			fields: fields{
				DeviationTimestamp: twelvePm,
				Delay:              0,
			},
			want: time.Date(2022, 5, 22, 12, 0, 0, 0, location),
		},
		{
			name: "one second late",
			fields: fields{
				DeviationTimestamp: twelvePm,
				Delay:              1,
			},
			want: time.Date(2022, 5, 22, 11, 59, 59, 0, location),
		},
		{
			name: "one second early",
			fields: fields{
				DeviationTimestamp: twelvePm,
				Delay:              -1,
			},
			want: time.Date(2022, 5, 22, 12, 0, 1, 0, location),
		},
		{
			name: "10 minutes early",
			fields: fields{
				DeviationTimestamp: twelvePm,
				Delay:              -600,
			},
			want: time.Date(2022, 5, 22, 12, 10, 0, 0, location),
		},
		{
			name: "10 minutes late",
			fields: fields{
				DeviationTimestamp: twelvePm,
				Delay:              600,
			},
			want: time.Date(2022, 5, 22, 11, 50, 0, 0, location),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			t := &TripDeviation{
				DeviationTimestamp: tt.fields.DeviationTimestamp,
				Delay:              tt.fields.Delay,
			}
			if got := t.SchedulePosition(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("SchedulePosition() = %v, want %v", got, tt.want)
			}
		})
	}
}
