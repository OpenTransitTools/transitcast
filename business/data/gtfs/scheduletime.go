package gtfs

import (
	"time"
)

// getDLSTransitionSeconds provides the number of seconds offset for a 12am date later in the day after day light saving time is done
func getDLSTransitionSeconds(timeAt12 time.Time) int {
	before := time.Date(timeAt12.Year(), timeAt12.Month(), timeAt12.Day(), 0, 0, 0, 0, timeAt12.Location())
	after := time.Date(timeAt12.Year(), timeAt12.Month(), timeAt12.Day(), 5, 0, 0, 0, timeAt12.Location())
	_, beforeOffset := before.Zone()
	_, afterOffset := after.Zone()
	return afterOffset - beforeOffset
}

// MakeScheduleTime produces a time from by adding seconds to a 12am date. Takes into account day light saving time
func MakeScheduleTime(timeAt12 time.Time, scheduleSeconds int) time.Time {
	offset := getDLSTransitionSeconds(timeAt12)
	scheduleSeconds = scheduleSeconds + (0 - offset)
	return timeAt12.Add(time.Duration(scheduleSeconds) * time.Second)
}

// ScheduleSlice contains a service date and a section of service time
type ScheduleSlice struct {
	ServiceDate  time.Time
	StartSeconds int
	EndSeconds   int
}

const (
	MaximumScheduleSeconds int = 60 * 60 * 30
)

// GetScheduleSlices produces array of schedule slices based on start and end times
func GetScheduleSlices(start time.Time, end time.Time) []ScheduleSlice {
	var result []ScheduleSlice
	//start a day behind to catch time past midnight but before MaximumScheduleSeconds
	var serviceDate = Get12AmTime(start).AddDate(0, 0, -1)
	endServiceDate := Get12AmTime(end).AddDate(0, 0, 1)
	for serviceDate.Unix() <= endServiceDate.Unix() {
		slice := ScheduleSlice{
			ServiceDate: serviceDate,
		}
		slice.StartSeconds = int(start.Unix() - serviceDate.Unix())
		if slice.StartSeconds < 0 {
			slice.StartSeconds = 0
		}
		slice.EndSeconds = int(end.Unix() - serviceDate.Unix())
		if slice.EndSeconds > MaximumScheduleSeconds {
			slice.EndSeconds = MaximumScheduleSeconds
		}
		//only include in results if the slice is within the service date's MaximumScheduleSeconds
		if slice.StartSeconds < MaximumScheduleSeconds && slice.EndSeconds > 0 {
			result = append(result, slice)
		}
		serviceDate = slice.ServiceDate.AddDate(0, 0, 1)

	}
	return result
}

func Get12AmTime(date time.Time) time.Time {
	return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())
}

// findScheduleSlice finds first ScheduleSlice for scheduleTime in slices provided or nil if non are found
func findScheduleSlice(slices []ScheduleSlice, scheduleTime int) *ScheduleSlice {
	for _, slice := range slices {
		if slice.StartSeconds <= scheduleTime && scheduleTime <= slice.EndSeconds {
			return &slice
		}
	}
	return nil
}
