package gtfs

import "time"

// PredictionSource how a prediction was made for a StopTimeUpdate
type PredictionSource int32

const (
	Undefined PredictionSource = iota
	SchedulePrediction
	StopMLPrediction
	TimepointMLPrediction
	StopStatisticsPrediction
	TimepointStatisticsPrediction
	NoFurtherPredictions
)

// TripUpdate holds a predicted Trip and its StopTimeUpdates
type TripUpdate struct {
	TripId               string           `json:"trip_id"`
	RouteId              string           `json:"route_id"`
	ScheduleRelationship string           `json:"schedule_relationship"`
	Timestamp            uint64           `json:"timestamp"`
	VehicleId            string           `json:"vehicle_id"`
	StopTimeUpdates      []StopTimeUpdate `json:"stop_time_update"`
}

// LastSchedulePosition return the last schedule position for this TripUpdate, if StopTimeUpdates is not empty
func (t *TripUpdate) LastSchedulePosition() *time.Time {
	if t == nil || len(t.StopTimeUpdates) < 1 {
		return nil
	}
	lastSchedulePosition := t.StopTimeUpdates[len(t.StopTimeUpdates)-1].PredictedArrivalTime
	return &lastSchedulePosition
}

// StopTimeUpdate predicted time for a single stop on a trip
type StopTimeUpdate struct {
	StopSequence           uint32           `json:"stop_sequence"`
	StopId                 string           `json:"stop_id"`
	ArrivalDelay           int              `json:"arrival_delay"`
	ScheduledArrivalTime   time.Time        `json:"scheduled_arrival_time"`
	PredictedArrivalTime   time.Time        `json:"predicted_arrival_time"`
	ScheduledDepartureTime *time.Time       `json:"scheduled_departure_time"`
	PredictedDepartureTime *time.Time       `json:"predicted_departure_time"`
	DepartureDelay         *int             `json:"departure_delay"`
	PredictionSource       PredictionSource `json:"prediction_source"`
}

func (stu *StopTimeUpdate) LatestPredictedTime() time.Time {
	if stu.PredictedDepartureTime != nil {
		return *stu.PredictedDepartureTime
	}
	return stu.PredictedArrivalTime
}
