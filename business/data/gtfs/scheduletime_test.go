package gtfs

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestMakeScheduleTime(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}
	type args struct {
		timeAt12        time.Time
		scheduleSeconds int
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{
			name: "12am",
			args: args{
				timeAt12:        time.Date(2020, 1, 9, 0, 0, 0, 0, location),
				scheduleSeconds: 0,
			},
			want: time.Date(2020, 1, 9, 0, 0, 0, 0, location),
		},
		{
			name: "12pm",
			args: args{
				timeAt12:        time.Date(2020, 1, 9, 0, 0, 0, 0, location),
				scheduleSeconds: 43200,
			},
			want: time.Date(2020, 1, 9, 12, 0, 0, 0, location),
		},
		{
			name: "12:30pm, on forward day",
			args: args{
				timeAt12:        time.Date(2018, 11, 4, 0, 0, 0, 0, location),
				scheduleSeconds: 45000,
			},
			want: time.Date(2018, 11, 4, 12, 30, 0, 0, location),
		},
		{
			name: "12:30pm, on back day",
			args: args{
				timeAt12:        time.Date(2019, 3, 10, 0, 0, 0, 0, location),
				scheduleSeconds: 45000,
			},
			want: time.Date(2019, 3, 10, 12, 30, 0, 0, location),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MakeScheduleTime(tt.args.timeAt12, tt.args.scheduleSeconds); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MakeScheduleTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetScheduleSlices(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to load \"America/Los_Angeles\" timezone: %v", err)
	}
	tests := []struct {
		giveStart time.Time
		giveEnd   time.Time
		want      []ScheduleSlice
	}{
		{
			giveStart: time.Date(2019, 11, 19, 9, 45, 0, 0, location),
			giveEnd:   time.Date(2019, 11, 19, 12, 45, 0, 0, location),
			want: []ScheduleSlice{
				{
					ServiceDate:  time.Date(2019, 11, 19, 0, 0, 0, 0, location),
					StartSeconds: ((9 * 60) + 45) * 60,
					EndSeconds:   ((12 * 60) + 45) * 60,
				},
			},
		},
		{
			giveStart: time.Date(2019, 11, 19, 9, 45, 0, 0, location),
			giveEnd:   time.Date(2019, 11, 20, 12, 45, 0, 0, location),
			want: []ScheduleSlice{
				{
					ServiceDate:  time.Date(2019, 11, 19, 0, 0, 0, 0, location),
					StartSeconds: ((9 * 60) + 45) * 60,
					EndSeconds:   MaximumScheduleSeconds, //first element should have maximum number of seconds
				},
				{
					ServiceDate:  time.Date(2019, 11, 20, 0, 0, 0, 0, location),
					StartSeconds: 0, //12am,
					EndSeconds:   ((12 * 60) + 45) * 60,
				},
			},
		},
		{
			giveStart: time.Date(2019, 11, 19, 9, 45, 0, 0, location),
			giveEnd:   time.Date(2019, 11, 20, 0, 45, 0, 0, location),
			want: []ScheduleSlice{
				{
					ServiceDate:  time.Date(2019, 11, 19, 0, 0, 0, 0, location),
					StartSeconds: ((9 * 60) + 45) * 60,
					EndSeconds:   ((24 * 60) + 45) * 60, //first element should have 45 minutes into following day
				},
				{
					ServiceDate:  time.Date(2019, 11, 20, 0, 0, 0, 0, location),
					StartSeconds: 0, //12am,
					EndSeconds:   45 * 60,
				},
			},
		},
		{
			giveStart: time.Date(2019, 11, 19, 0, 45, 0, 0, location),
			giveEnd:   time.Date(2019, 11, 19, 6, 0, 0, 0, location),
			want: []ScheduleSlice{
				{
					ServiceDate:  time.Date(2019, 11, 18, 0, 0, 0, 0, location),
					StartSeconds: (24 * 60 * 60) + (45 * 60), //first element should start at 00:45am
					EndSeconds:   (24 * 60 * 60) + (6*60)*60, //first element should end at 6am ,
				},
				{
					ServiceDate:  time.Date(2019, 11, 19, 0, 0, 0, 0, location),
					StartSeconds: 45 * 60,
					EndSeconds:   6 * 60 * 60,
				},
			},
		},
	}
	for row, tt := range tests {
		t.Run("row: "+strconv.Itoa(row), func(t *testing.T) {
			is := is.New(t)
			result := GetScheduleSlices(tt.giveStart, tt.giveEnd)
			is.Equal(len(tt.want), len(result))
			if len(tt.want) == len(result) {
				for i, wanted := range tt.want {
					got := result[i]
					is.Equal(wanted.ServiceDate, got.ServiceDate)
					is.Equal(wanted.StartSeconds, got.StartSeconds)
					is.Equal(wanted.EndSeconds, got.EndSeconds)
				}
			}
		})
	}

}

func getTestDate(str string) time.Time {
	result, _ := time.Parse("20060102", str)
	return result
}

func Test_findBestScheduleSlice(t *testing.T) {
	sliceDay1 := ScheduleSlice{
		ServiceDate:  getTestDate("20200630"),
		StartSeconds: 5000 + (60 * 60 * 24), //late in service day (wrapped around)
		EndSeconds:   10000 + (60 * 60 * 24),
	}
	sliceDay2 := ScheduleSlice{
		ServiceDate:  getTestDate("20200701"),
		StartSeconds: 5000, //early service day
		EndSeconds:   10000,
	}
	type args struct {
		slices       []ScheduleSlice
		scheduleTime int
	}
	tests := []struct {
		name string
		args args
		want *ScheduleSlice
	}{
		{
			name: "single slice found",
			args: args{
				slices: []ScheduleSlice{
					sliceDay2,
				},
				scheduleTime: 5500,
			},
			want: &sliceDay2,
		}, {
			name: "two slices found first slice",
			args: args{
				slices: []ScheduleSlice{
					sliceDay1,
					sliceDay2,
				},
				scheduleTime: 7000 + (60 * 60 * 24),
			},
			want: &sliceDay1,
		},
		{
			name: "two slices second slice",
			args: args{
				slices: []ScheduleSlice{
					sliceDay1,
					sliceDay2,
				},
				scheduleTime: 7000 + (60 * 60 * 24),
			},
			want: &sliceDay1,
		},
		{
			name: "two slices, found neither",
			args: args{
				slices: []ScheduleSlice{
					sliceDay1,
					sliceDay2,
				},
				scheduleTime: 11000,
			},
			want: nil,
		},
		{
			name: "no slices, produces error found",
			args: args{
				slices:       []ScheduleSlice{},
				scheduleTime: 11000,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findScheduleSlice(tt.args.slices, tt.args.scheduleTime)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findScheduleSlice() got = %v, want %v", got, tt.want)
			}
		})
	}
}
