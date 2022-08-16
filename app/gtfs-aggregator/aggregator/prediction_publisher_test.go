package aggregator

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	logger "log"
	"math"
	"strings"
	"sync"

	"reflect"
	"testing"
	"time"
)

func Test_buildStopUpdate(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}
	trip := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_1.json", t)
	firstStop := trip.StopTimeInstances[0]
	secondStop := trip.StopTimeInstances[1]
	thirdStop := trip.StopTimeInstances[2]
	fourthStop := trip.StopTimeInstances[3]
	type args struct {
		previousTime                time.Time
		tripDistanceTraveled        float64
		previousPredictionRemainder float64
		stopPrediction              *stopPrediction
		limitEarlyDepartureSeconds  int
	}
	tests := []struct {
		name                    string
		args                    args
		wantStopTimeUpdate      gtfs.StopTimeUpdate
		wantPredictionRemainder float64
	}{
		{
			name: "Simple no remainders",
			args: args{
				previousTime:                time.Date(2022, 5, 22, 12, 0, 0, 0, location),
				tripDistanceTraveled:        0,
				previousPredictionRemainder: 0,
				stopPrediction: &stopPrediction{
					fromStop:           firstStop,
					toStop:             secondStop,
					predictedTime:      1200,
					predictionSource:   gtfs.TimepointMLPrediction,
					predictionComplete: true,
				},
			},
			wantStopTimeUpdate: gtfs.StopTimeUpdate{
				StopSequence:         2,
				StopId:               "B",
				ArrivalDelay:         0,
				ScheduledArrivalTime: secondStop.ArrivalDateTime,
				PredictedArrivalTime: time.Date(2022, 5, 22, 12, 20, 0, 0, location),
				PredictionSource:     gtfs.TimepointMLPrediction,
			},
			wantPredictionRemainder: 0,
		},
		{
			name: "Simple 1 second late no remainders",
			args: args{
				previousTime:                time.Date(2022, 5, 22, 12, 0, 0, 0, location),
				tripDistanceTraveled:        0,
				previousPredictionRemainder: 0,
				stopPrediction: &stopPrediction{
					fromStop:           firstStop,
					toStop:             secondStop,
					predictedTime:      1201,
					predictionSource:   gtfs.TimepointMLPrediction,
					predictionComplete: true,
				},
			},
			wantStopTimeUpdate: gtfs.StopTimeUpdate{
				StopSequence:         2,
				StopId:               "B",
				ArrivalDelay:         1,
				ScheduledArrivalTime: secondStop.ArrivalDateTime,
				PredictedArrivalTime: time.Date(2022, 5, 22, 12, 20, 1, 0, location),
				PredictionSource:     gtfs.TimepointMLPrediction,
			},
			wantPredictionRemainder: 0,
		},
		{
			name: "Vehicle is half way between the stops and estimated to arrive on time",
			args: args{
				previousTime:                time.Date(2022, 5, 22, 12, 10, 0, 0, location),
				tripDistanceTraveled:        500.0,
				previousPredictionRemainder: 0,
				stopPrediction: &stopPrediction{
					fromStop:           firstStop,
					toStop:             secondStop,
					predictedTime:      1200,
					predictionSource:   gtfs.TimepointMLPrediction,
					predictionComplete: true,
				},
			},
			wantStopTimeUpdate: gtfs.StopTimeUpdate{
				StopSequence:         2,
				StopId:               "B",
				ArrivalDelay:         0,
				ScheduledArrivalTime: secondStop.ArrivalDateTime,
				PredictedArrivalTime: time.Date(2022, 5, 22, 12, 20, 0, 0, location),
				PredictionSource:     gtfs.TimepointMLPrediction,
			},
			wantPredictionRemainder: 0,
		},
		{
			name: "Vehicle is half way between the stops and estimated to arrive two minutes late",
			args: args{
				previousTime:                time.Date(2022, 5, 22, 12, 10, 0, 0, location),
				tripDistanceTraveled:        500.0,
				previousPredictionRemainder: 0,
				stopPrediction: &stopPrediction{
					fromStop:           firstStop,
					toStop:             secondStop,
					predictedTime:      1440,
					predictionSource:   gtfs.TimepointMLPrediction,
					predictionComplete: true,
				},
			},
			wantStopTimeUpdate: gtfs.StopTimeUpdate{
				StopSequence:         2,
				StopId:               "B",
				ArrivalDelay:         120,
				ScheduledArrivalTime: secondStop.ArrivalDateTime,
				PredictedArrivalTime: time.Date(2022, 5, 22, 12, 22, 0, 0, location),
				PredictionSource:     gtfs.TimepointMLPrediction,
			},
			wantPredictionRemainder: 0,
		},
		{
			name: "Vehicle is half way between the stops and estimated to arrive two minutes late with .25 remainder",
			args: args{
				previousTime:                time.Date(2022, 5, 22, 12, 10, 0, 0, location),
				tripDistanceTraveled:        500.0,
				previousPredictionRemainder: 0,
				stopPrediction: &stopPrediction{
					fromStop:           firstStop,
					toStop:             secondStop,
					predictedTime:      1440.5,
					predictionSource:   gtfs.TimepointMLPrediction,
					predictionComplete: true,
				},
			},
			wantStopTimeUpdate: gtfs.StopTimeUpdate{
				StopSequence:         2,
				StopId:               "B",
				ArrivalDelay:         120,
				ScheduledArrivalTime: secondStop.ArrivalDateTime,
				PredictedArrivalTime: time.Date(2022, 5, 22, 12, 22, 0, 0, location),
				PredictionSource:     gtfs.TimepointMLPrediction,
			},
			wantPredictionRemainder: .25,
		},
		{
			name: "Vehicle located prior the stops and estimated to arrive two minutes early with .3 remainder",
			args: args{
				previousTime:                time.Date(2022, 5, 22, 12, 20, 0, 0, location),
				tripDistanceTraveled:        500.0,
				previousPredictionRemainder: 0,
				stopPrediction: &stopPrediction{
					fromStop:           secondStop,
					toStop:             thirdStop,
					predictedTime:      1080.3,
					predictionSource:   gtfs.StopMLPrediction,
					predictionComplete: true,
				},
			},
			wantStopTimeUpdate: gtfs.StopTimeUpdate{
				StopSequence:         3,
				StopId:               "C",
				ArrivalDelay:         -120,
				ScheduledArrivalTime: thirdStop.ArrivalDateTime,
				PredictedArrivalTime: time.Date(2022, 5, 22, 12, 38, 0, 0, location),
				PredictionSource:     gtfs.StopMLPrediction,
			},
			wantPredictionRemainder: .3,
		},
		{
			name: "Vehicle located prior the stops and estimated to arrive two minutes early, with previousPrediction remainder and returning .1 remainder",
			args: args{
				previousTime:                time.Date(2022, 5, 22, 12, 20, 0, 0, location),
				tripDistanceTraveled:        500.0,
				previousPredictionRemainder: .6,
				stopPrediction: &stopPrediction{
					fromStop:           secondStop,
					toStop:             thirdStop,
					predictedTime:      1080.5,
					predictionSource:   gtfs.StopMLPrediction,
					predictionComplete: true,
				},
			},
			wantStopTimeUpdate: gtfs.StopTimeUpdate{
				StopSequence:         3,
				StopId:               "C",
				ArrivalDelay:         -119,
				ScheduledArrivalTime: thirdStop.ArrivalDateTime,
				PredictedArrivalTime: time.Date(2022, 5, 22, 12, 38, 1, 0, location),
				PredictionSource:     gtfs.StopMLPrediction,
			},
			wantPredictionRemainder: .1,
		},
		{
			name: "vehicle 5 minutes early at timepoint, next stop should not be earlier than limitEarlyDepartureSeconds",
			args: args{
				previousTime:                time.Date(2022, 5, 22, 12, 35, 0, 0, location),
				tripDistanceTraveled:        0,
				previousPredictionRemainder: 0,
				stopPrediction: &stopPrediction{
					fromStop:           thirdStop,
					toStop:             fourthStop,
					predictedTime:      1200,
					predictionSource:   gtfs.TimepointMLPrediction,
					predictionComplete: true,
				},
				limitEarlyDepartureSeconds: 60,
			},
			wantStopTimeUpdate: gtfs.StopTimeUpdate{
				StopSequence:         4,
				StopId:               "D",
				ArrivalDelay:         -60,
				ScheduledArrivalTime: fourthStop.ArrivalDateTime,
				PredictedArrivalTime: time.Date(2022, 5, 22, 12, 59, 0, 0, location),
				PredictionSource:     gtfs.TimepointMLPrediction,
			},
			wantPredictionRemainder: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testLog := makeTestLogWriter()
			gotStopTimeUpdate, gotPredictionRemainder := buildStopUpdate(testLog.log, tt.args.previousTime,
				tt.args.tripDistanceTraveled, tt.args.previousPredictionRemainder, tt.args.stopPrediction,
				tt.args.limitEarlyDepartureSeconds)
			if !reflect.DeepEqual(gotStopTimeUpdate, tt.wantStopTimeUpdate) {
				t.Errorf("buildStopUpdate() produced unexpected StopTimeUpdate\ngot= %+v\nwant=%+v", gotStopTimeUpdate, tt.wantStopTimeUpdate)
			}
			if math.Abs(gotPredictionRemainder-tt.wantPredictionRemainder) > .01 {
				t.Errorf("buildStopUpdate() gotPredictionRemainder = %v, want %v", gotPredictionRemainder, tt.wantPredictionRemainder)
			}
		})
	}
}

type testLogWriter struct {
	logLines []string
	log      *logger.Logger
}

func makeTestLogWriter() *testLogWriter {
	logWriter := testLogWriter{
		logLines: make([]string, 0),
	}
	log := logger.New(&logWriter, "TEST_GTFS_AGGREGATOR : ", logger.LstdFlags|logger.Lmicroseconds|logger.Lshortfile)
	logWriter.log = log
	return &logWriter
}

func (t *testLogWriter) Write(p []byte) (n int, err error) {
	t.logLines = append(t.logLines, string(p))
	return len(p), nil
}

func Test_buildTripUpdate(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}

	trip1 := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_1.json", t)
	firstStop := trip1.StopTimeInstances[0]
	secondStop := trip1.StopTimeInstances[1]
	thirdStop := trip1.StopTimeInstances[2]
	fourthStop := trip1.StopTimeInstances[3]
	fifthStop := trip1.StopTimeInstances[4]
	sixthStop := trip1.StopTimeInstances[5]
	seventhStop := trip1.StopTimeInstances[6]
	twelvePm := time.Date(2022, 5, 22, 12, 0, 0, 0, location)
	twelve20Pm := time.Date(2022, 5, 22, 12, 20, 0, 0, location)
	twelve38Pm := time.Date(2022, 5, 22, 12, 38, 0, 0, location)
	twelve58Pm := time.Date(2022, 5, 22, 12, 58, 0, 0, location)
	type args struct {
		previousSchedulePositionTime time.Time
		prediction                   *tripPrediction
		limitEarlyDepartureSeconds   int
	}
	tests := []struct {
		name string
		args args
		want *gtfs.TripUpdate
	}{
		{
			name: "Simple, on time, at start of trip",
			args: args{
				previousSchedulePositionTime: twelvePm,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          twelvePm,
						DeviationTimestamp: twelvePm,
						TripProgress:       0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
						Delay:              0,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           firstStop,
							toStop:             secondStop,
							predictedTime:      1200,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(twelvePm.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					{
						StopSequence:         1,
						StopId:               "A",
						ArrivalDelay:         0,
						ScheduledArrivalTime: firstStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 0, 0, 0, location),
						PredictionSource:     gtfs.SchedulePrediction,
					},
					{
						StopSequence:         2,
						StopId:               "B",
						ArrivalDelay:         0,
						ScheduledArrivalTime: secondStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 20, 0, 0, location),
						PredictionSource:     gtfs.StopMLPrediction,
					},
				},
			},
		},
		{
			name: "Simple, prior to trip by one minute, at first stop",
			args: args{
				previousSchedulePositionTime: twelvePm.Add(-time.Minute),
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          twelvePm,
						DeviationTimestamp: twelvePm,
						TripProgress:       0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
						Delay:              0,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           firstStop,
							toStop:             secondStop,
							predictedTime:      1200,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(twelvePm.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					{
						StopSequence:         1,
						StopId:               "A",
						ArrivalDelay:         0,
						ScheduledArrivalTime: firstStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 0, 0, 0, location),
						PredictionSource:     gtfs.SchedulePrediction,
					},
					{
						StopSequence:         2,
						StopId:               "B",
						ArrivalDelay:         0,
						ScheduledArrivalTime: secondStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 20, 0, 0, location),
						PredictionSource:     gtfs.StopMLPrediction,
					},
				},
			},
		},
		{
			name: "One minute late, at first stop",
			args: args{
				previousSchedulePositionTime: twelvePm.Add(time.Minute),
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          twelvePm,
						DeviationTimestamp: twelvePm,
						TripProgress:       0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
						Delay:              0,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           firstStop,
							toStop:             secondStop,
							predictedTime:      1200,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(twelvePm.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					{
						StopSequence:         1,
						StopId:               "A",
						ArrivalDelay:         60,
						ScheduledArrivalTime: firstStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 1, 0, 0, location),
						PredictionSource:     gtfs.SchedulePrediction,
					},
					{
						StopSequence:         2,
						StopId:               "B",
						ArrivalDelay:         60,
						ScheduledArrivalTime: secondStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 21, 0, 0, location),
						PredictionSource:     gtfs.StopMLPrediction,
					},
				},
			},
		},
		{
			name: "10 minutes late, between first amd second stop",
			args: args{
				previousSchedulePositionTime: twelve20Pm,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          twelve20Pm,
						DeviationTimestamp: twelve20Pm,
						TripProgress:       500.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           firstStop,
							toStop:             secondStop,
							predictedTime:      1200,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
						{
							fromStop:           secondStop,
							toStop:             thirdStop,
							predictedTime:      1200,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(twelve20Pm.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					{
						StopSequence:         1,
						StopId:               "A",
						ArrivalDelay:         0,
						ScheduledArrivalTime: firstStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 0, 0, 0, location),
						PredictionSource:     gtfs.SchedulePrediction,
					},
					{
						StopSequence:         2,
						StopId:               "B",
						ArrivalDelay:         600,
						ScheduledArrivalTime: secondStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 30, 0, 0, location),
						PredictionSource:     gtfs.StopMLPrediction,
					},
					{
						StopSequence:         3,
						StopId:               "C",
						ArrivalDelay:         600,
						ScheduledArrivalTime: thirdStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 50, 0, 0, location),
						PredictionSource:     gtfs.StopMLPrediction,
					},
				},
			},
		},
		{
			name: "2 minutes early, just left third stop",
			args: args{
				previousSchedulePositionTime: twelve38Pm,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          twelve38Pm,
						DeviationTimestamp: twelve38Pm,
						TripProgress:       2100.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           thirdStop,
							toStop:             fourthStop,
							predictedTime:      1200,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(twelve38Pm.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					{
						StopSequence:         1,
						StopId:               "A",
						ArrivalDelay:         0,
						ScheduledArrivalTime: firstStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 0, 0, 0, location),
						PredictionSource:     gtfs.SchedulePrediction,
					},
					{
						StopSequence:         3,
						StopId:               "C",
						ArrivalDelay:         -180, //three minutes before timestamp on TripDeviations
						ScheduledArrivalTime: thirdStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 37, 0, 0, location),
						PredictionSource:     gtfs.SchedulePrediction,
					},
					{
						StopSequence:         4,
						StopId:               "D",
						ArrivalDelay:         -240,
						ScheduledArrivalTime: fourthStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 56, 0, 0, location),
						PredictionSource:     gtfs.StopMLPrediction,
					},
				},
			},
		},
		{
			name: "2 minutes early, at fourth stop, predictions to end of trip, early adjusted by first timepoint",
			args: args{
				previousSchedulePositionTime: twelve58Pm,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          twelve58Pm,
						DeviationTimestamp: twelve58Pm,
						TripProgress:       3000.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           fourthStop,
							toStop:             fifthStop,
							predictedTime:      1200,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
						{
							fromStop:           fifthStop,
							toStop:             sixthStop,
							predictedTime:      600,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
						{
							fromStop:           sixthStop,
							toStop:             seventhStop,
							predictedTime:      700,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(twelve58Pm.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					{
						StopSequence:         1,
						StopId:               "A",
						ArrivalDelay:         0,
						ScheduledArrivalTime: firstStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 0, 0, 0, location),
						PredictionSource:     gtfs.SchedulePrediction,
					},
					{
						StopSequence:         4,
						StopId:               "D",
						ArrivalDelay:         -180, //three minutes before timestamp on TripDeviations
						ScheduledArrivalTime: fourthStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 12, 57, 0, 0, location),
						PredictionSource:     gtfs.SchedulePrediction,
					},
					{
						StopSequence:         5,
						StopId:               "E",
						ArrivalDelay:         -120,
						ScheduledArrivalTime: fifthStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 13, 18, 0, 0, location),
						PredictionSource:     gtfs.StopMLPrediction,
					},
					{
						StopSequence:         6,
						StopId:               "F",
						ArrivalDelay:         -60,
						ScheduledArrivalTime: sixthStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 13, 29, 0, 0, location),
						PredictionSource:     gtfs.StopMLPrediction,
					},
					{
						StopSequence:         7,
						StopId:               "G",
						ArrivalDelay:         -60,
						ScheduledArrivalTime: seventhStop.ArrivalDateTime,
						PredictedArrivalTime: time.Date(2022, 5, 22, 13, 40, 40, 0, location),
						PredictionSource:     gtfs.StopMLPrediction,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testLog := makeTestLogWriter()
			got := buildTripUpdate(testLog.log, tt.args.previousSchedulePositionTime, tt.args.prediction,
				tt.args.limitEarlyDepartureSeconds)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildTripUpdate() produced unexpected StopTimeUpdate\ngot= %v\nwant=%v",
					sprintTripUpdate(got), sprintTripUpdate(tt.want))
			}
		})
	}
}

func sprintTripUpdates(updates []*gtfs.TripUpdate) string {
	var parts []string
	for _, update := range updates {
		parts = append(parts, sprintTripUpdate(update))
	}
	return strings.Join(parts, "\n")
}

func sprintTripUpdate(update *gtfs.TripUpdate) string {
	parts := []string{fmt.Sprintf("{TripId:%s RouteId:%s ScheduleRelationship:%s Timestamp:%d, VehicleId:%s",
		update.TripId, update.RouteId, update.ScheduleRelationship, update.Timestamp, update.VehicleId)}
	for _, su := range update.StopTimeUpdates {
		parts = append(parts,
			fmt.Sprintf("{StopSequence:%d StopId:%s ArrivalDelay:%d ScheduledArrivalTime:%v PredictedArrivalTime:%v PredictionSource:%d}",
				su.StopSequence, su.StopId, su.ArrivalDelay, su.ScheduledArrivalTime, su.PredictedArrivalTime, su.PredictionSource))
	}
	return strings.Join(parts, "\n")
}

func Test_makeTripUpdates(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}

	trip2 := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_2.json", t)
	trip3 := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_3.json", t)
	trip3OneMinuteEarlier := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_3.json", t)
	trip3OneMinuteEarlier.StopTimeInstances[0].ArrivalDateTime = trip3OneMinuteEarlier.StopTimeInstances[0].ArrivalDateTime.Add(time.Minute)
	trip3OneMinuteEarlier.StopTimeInstances[0].DepartureDateTime = trip3OneMinuteEarlier.StopTimeInstances[0].ArrivalDateTime.Add(time.Minute)
	trip3OneMinuteEarlier.StopTimeInstances[0].ArrivalTime += 60
	trip3OneMinuteEarlier.StopTimeInstances[0].DepartureTime += 60
	oneFortyThree := time.Date(2022, 5, 22, 13, 43, 0, 0, location)
	oneFiftyThree := time.Date(2022, 5, 22, 13, 53, 0, 0, location)

	tests := []struct {
		name                       string
		orderedPredictions         []*tripPrediction
		limitEarlyDepartureSeconds int
		want                       []*gtfs.TripUpdate
	}{
		{
			name: "On time prediction at start of trip",
			orderedPredictions: []*tripPrediction{
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          oneFortyThree,
						DeviationTimestamp: oneFortyThree,
						TripProgress:       0.0,
						TripId:             trip2.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           trip2.StopTimeInstances[0],
							toStop:             trip2.StopTimeInstances[1],
							predictedTime:      180,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
						{
							fromStop:           trip2.StopTimeInstances[1],
							toStop:             trip2.StopTimeInstances[2],
							predictedTime:      180,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance:       trip2,
					pendingPredictions: 0,
				},
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          oneFortyThree,
						DeviationTimestamp: oneFortyThree,
						TripProgress:       -2000.0,
						TripId:             trip3.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           trip3.StopTimeInstances[0],
							toStop:             trip3.StopTimeInstances[1],
							predictedTime:      360,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
						{
							fromStop:           trip3.StopTimeInstances[1],
							toStop:             trip3.StopTimeInstances[2],
							predictedTime:      180,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance:       trip3,
					pendingPredictions: 0,
				},
			},
			want: []*gtfs.TripUpdate{
				{
					TripId:               trip2.TripId,
					RouteId:              trip2.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(oneFortyThree.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						{
							StopSequence:         trip2.StopTimeInstances[0].StopSequence,
							StopId:               trip2.StopTimeInstances[0].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip2.StopTimeInstances[0].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 43, 0, 0, location),
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip2.StopTimeInstances[1].StopSequence,
							StopId:               trip2.StopTimeInstances[1].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip2.StopTimeInstances[1].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 46, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
						{
							StopSequence:         trip2.StopTimeInstances[2].StopSequence,
							StopId:               trip2.StopTimeInstances[2].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip2.StopTimeInstances[2].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 49, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
					},
				},
				{
					TripId:               trip3.TripId,
					RouteId:              trip3.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(oneFortyThree.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						{
							StopSequence:         trip3.StopTimeInstances[0].StopSequence,
							StopId:               trip3.StopTimeInstances[0].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip3.StopTimeInstances[0].ArrivalDateTime,
							PredictedArrivalTime: trip3.StopTimeInstances[0].ArrivalDateTime,
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip3.StopTimeInstances[1].StopSequence,
							StopId:               trip3.StopTimeInstances[1].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip3.StopTimeInstances[1].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 55, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
						{
							StopSequence:         trip3.StopTimeInstances[2].StopSequence,
							StopId:               trip3.StopTimeInstances[2].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip3.StopTimeInstances[2].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 58, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
					},
				},
			},
		},
		{
			name: "Early prediction in middle of first trip",
			orderedPredictions: []*tripPrediction{
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          oneFortyThree,
						DeviationTimestamp: oneFortyThree,
						TripProgress:       1000.0,
						TripId:             trip2.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           trip2.StopTimeInstances[1],
							toStop:             trip2.StopTimeInstances[2],
							predictedTime:      180,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance:       trip2,
					pendingPredictions: 0,
				},
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          oneFortyThree,
						DeviationTimestamp: oneFortyThree,
						TripProgress:       -1000.0,
						TripId:             trip3.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           trip3.StopTimeInstances[0],
							toStop:             trip3.StopTimeInstances[1],
							predictedTime:      360,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
						{
							fromStop:           trip3.StopTimeInstances[1],
							toStop:             trip3.StopTimeInstances[2],
							predictedTime:      180,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance:       trip3,
					pendingPredictions: 0,
				},
			},
			want: []*gtfs.TripUpdate{
				{
					TripId:               trip2.TripId,
					RouteId:              trip2.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(oneFortyThree.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						{
							StopSequence:         trip2.StopTimeInstances[0].StopSequence,
							StopId:               trip2.StopTimeInstances[0].StopId,
							ArrivalDelay:         -60,
							ScheduledArrivalTime: trip2.StopTimeInstances[0].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 42, 0, 0, location),
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip2.StopTimeInstances[1].StopSequence,
							StopId:               trip2.StopTimeInstances[1].StopId,
							ArrivalDelay:         -240,
							ScheduledArrivalTime: trip2.StopTimeInstances[1].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 42, 0, 0, location),
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip2.StopTimeInstances[2].StopSequence,
							StopId:               trip2.StopTimeInstances[2].StopId,
							ArrivalDelay:         -180,
							ScheduledArrivalTime: trip2.StopTimeInstances[2].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 46, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
					},
				},
				{
					TripId:               trip3.TripId,
					RouteId:              trip3.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(oneFortyThree.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						{
							StopSequence:         trip3.StopTimeInstances[0].StopSequence,
							StopId:               trip3.StopTimeInstances[0].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip3.StopTimeInstances[0].ArrivalDateTime,
							PredictedArrivalTime: trip3.StopTimeInstances[0].ArrivalDateTime,
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip3.StopTimeInstances[1].StopSequence,
							StopId:               trip3.StopTimeInstances[1].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip3.StopTimeInstances[1].ArrivalDateTime,
							PredictedArrivalTime: trip3.StopTimeInstances[1].ArrivalDateTime,
							PredictionSource:     gtfs.StopMLPrediction,
						},
						{
							StopSequence:         trip3.StopTimeInstances[2].StopSequence,
							StopId:               trip3.StopTimeInstances[2].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip3.StopTimeInstances[2].ArrivalDateTime,
							PredictedArrivalTime: trip3.StopTimeInstances[2].ArrivalDateTime,
							PredictionSource:     gtfs.StopMLPrediction,
						},
					},
				},
			},
		},
		{
			name: "Late prediction in middle of first trip",
			orderedPredictions: []*tripPrediction{
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          oneFiftyThree,
						DeviationTimestamp: oneFiftyThree,
						TripProgress:       1000.0,
						TripId:             trip2.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           trip2.StopTimeInstances[1],
							toStop:             trip2.StopTimeInstances[2],
							predictedTime:      180,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance:       trip2,
					pendingPredictions: 0,
				},
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          oneFiftyThree,
						DeviationTimestamp: oneFiftyThree,
						TripProgress:       -1000.0,
						TripId:             trip3.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           trip3.StopTimeInstances[0],
							toStop:             trip3.StopTimeInstances[1],
							predictedTime:      180,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
						{
							fromStop:           trip3.StopTimeInstances[1],
							toStop:             trip3.StopTimeInstances[2],
							predictedTime:      180,
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance:       trip3,
					pendingPredictions: 0,
				},
			},
			want: []*gtfs.TripUpdate{
				{
					TripId:               trip2.TripId,
					RouteId:              trip2.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(oneFiftyThree.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						{
							StopSequence:         trip2.StopTimeInstances[0].StopSequence,
							StopId:               trip2.StopTimeInstances[0].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip2.StopTimeInstances[0].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 43, 0, 0, location),
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip2.StopTimeInstances[1].StopSequence,
							StopId:               trip2.StopTimeInstances[1].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip2.StopTimeInstances[1].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 46, 0, 0, location),
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip2.StopTimeInstances[2].StopSequence,
							StopId:               trip2.StopTimeInstances[2].StopId,
							ArrivalDelay:         420,
							ScheduledArrivalTime: trip2.StopTimeInstances[2].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 56, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
					},
				},
				{
					TripId:               trip3.TripId,
					RouteId:              trip3.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(oneFiftyThree.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						{
							StopSequence:         trip3.StopTimeInstances[0].StopSequence,
							StopId:               trip3.StopTimeInstances[0].StopId,
							ArrivalDelay:         240,
							ScheduledArrivalTime: trip3.StopTimeInstances[0].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 53, 0, 0, location),
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip3.StopTimeInstances[1].StopSequence,
							StopId:               trip3.StopTimeInstances[1].StopId,
							ArrivalDelay:         240,
							ScheduledArrivalTime: trip3.StopTimeInstances[1].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 59, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
						{
							StopSequence:         trip3.StopTimeInstances[2].StopSequence,
							StopId:               trip3.StopTimeInstances[2].StopId,
							ArrivalDelay:         240,
							ScheduledArrivalTime: trip3.StopTimeInstances[2].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 14, 2, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
					},
				},
			},
		},
		{
			name: "Late prediction in middle of first trip, various predictions along the way, trip3 modified to start a minute later",
			orderedPredictions: []*tripPrediction{
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          oneFiftyThree,
						DeviationTimestamp: oneFiftyThree,
						TripProgress:       1000.0,
						TripId:             trip2.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           trip2.StopTimeInstances[1],
							toStop:             trip2.StopTimeInstances[2],
							predictedTime:      120, //60 seconds faster than scheduled
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance:       trip2,
					pendingPredictions: 0,
				},
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          oneFiftyThree,
						DeviationTimestamp: oneFiftyThree,
						TripProgress:       -1000.0,
						TripId:             trip3OneMinuteEarlier.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						{
							fromStop:           trip3OneMinuteEarlier.StopTimeInstances[0],
							toStop:             trip3OneMinuteEarlier.StopTimeInstances[1],
							predictedTime:      60, //120 seconds faster than scheduled
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
						{
							fromStop:           trip3OneMinuteEarlier.StopTimeInstances[1],
							toStop:             trip3OneMinuteEarlier.StopTimeInstances[2],
							predictedTime:      240, //60 seconds longer than usual
							predictionSource:   gtfs.StopMLPrediction,
							predictionComplete: true,
						},
					},
					tripInstance:       trip3OneMinuteEarlier,
					pendingPredictions: 0,
				},
			},
			want: []*gtfs.TripUpdate{
				{
					TripId:               trip2.TripId,
					RouteId:              trip2.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(oneFiftyThree.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						{
							StopSequence:         trip2.StopTimeInstances[0].StopSequence,
							StopId:               trip2.StopTimeInstances[0].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip2.StopTimeInstances[0].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 43, 0, 0, location),
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip2.StopTimeInstances[1].StopSequence,
							StopId:               trip2.StopTimeInstances[1].StopId,
							ArrivalDelay:         0,
							ScheduledArrivalTime: trip2.StopTimeInstances[1].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 46, 0, 0, location),
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip2.StopTimeInstances[2].StopSequence,
							StopId:               trip2.StopTimeInstances[2].StopId,
							ArrivalDelay:         360,
							ScheduledArrivalTime: trip2.StopTimeInstances[2].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 55, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
					},
				},
				{
					TripId:               trip3.TripId,
					RouteId:              trip3.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(oneFiftyThree.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						{
							StopSequence:         trip3OneMinuteEarlier.StopTimeInstances[0].StopSequence,
							StopId:               trip3OneMinuteEarlier.StopTimeInstances[0].StopId,
							ArrivalDelay:         240,
							ScheduledArrivalTime: trip3OneMinuteEarlier.StopTimeInstances[0].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 54, 0, 0, location),
							PredictionSource:     gtfs.SchedulePrediction,
						},
						{
							StopSequence:         trip3OneMinuteEarlier.StopTimeInstances[1].StopSequence,
							StopId:               trip3OneMinuteEarlier.StopTimeInstances[1].StopId,
							ArrivalDelay:         60,
							ScheduledArrivalTime: trip3OneMinuteEarlier.StopTimeInstances[1].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 13, 56, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
						{
							StopSequence:         trip3OneMinuteEarlier.StopTimeInstances[2].StopSequence,
							StopId:               trip3OneMinuteEarlier.StopTimeInstances[2].StopId,
							ArrivalDelay:         120,
							ScheduledArrivalTime: trip3OneMinuteEarlier.StopTimeInstances[2].ArrivalDateTime,
							PredictedArrivalTime: time.Date(2022, 5, 22, 14, 0, 0, 0, location),
							PredictionSource:     gtfs.StopMLPrediction,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testLog := makeTestLogWriter()
			got := makeTripUpdates(testLog.log, tt.orderedPredictions, tt.limitEarlyDepartureSeconds)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("makeTripUpdates() \ngot =\n%v\nwant=\n%v", sprintTripUpdates(got), sprintTripUpdates(tt.want))
			}
		})
	}
}

func Test_buildStopUpdateForFirstStop(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}

	trip := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_3.json", t)
	firstStop := trip.StopTimeInstances[0]

	twoMinutesBeforeScheduledArrival := time.Date(2022, 5, 22, 13, 47, 0, 0, location)
	oneSecondAfterDeparture := time.Date(2022, 5, 22, 13, 52, 1, 0, location)
	oneSecondAfterArrival := time.Date(2022, 5, 22, 13, 49, 1, 0, location)

	fiveMinutesAfterDeparture := time.Date(2022, 5, 22, 13, 57, 0, 0, location)
	fiveMinutesAfterArrival := time.Date(2022, 5, 22, 13, 54, 0, 0, location)
	type args struct {
		at       time.Time
		stopTime *gtfs.StopTimeInstance
	}
	tests := []struct {
		name string
		args args
		want gtfs.StopTimeUpdate
	}{
		{
			name: "Time exactly at arrive time of stop",
			args: args{
				at:       firstStop.ArrivalDateTime,
				stopTime: firstStop,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         1,
				StopId:               "A3",
				ArrivalDelay:         0,
				ScheduledArrivalTime: firstStop.ArrivalDateTime,
				PredictedArrivalTime: firstStop.ArrivalDateTime,
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "Time is before arrive time of stop",
			args: args{
				at:       twoMinutesBeforeScheduledArrival,
				stopTime: firstStop,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         1,
				StopId:               "A3",
				ArrivalDelay:         0,
				ScheduledArrivalTime: firstStop.ArrivalDateTime,
				PredictedArrivalTime: firstStop.ArrivalDateTime,
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "Time is at depart time of stop",
			args: args{
				at:       firstStop.DepartureDateTime,
				stopTime: firstStop,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         1,
				StopId:               "A3",
				ArrivalDelay:         0,
				ScheduledArrivalTime: firstStop.ArrivalDateTime,
				PredictedArrivalTime: firstStop.ArrivalDateTime,
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "Time is one second after depart time",
			args: args{
				at:       oneSecondAfterDeparture,
				stopTime: firstStop,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         1,
				StopId:               "A3",
				ArrivalDelay:         1,
				ScheduledArrivalTime: firstStop.ArrivalDateTime,
				PredictedArrivalTime: oneSecondAfterArrival,
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "Time is five minutes after depart time",
			args: args{
				at:       fiveMinutesAfterDeparture,
				stopTime: firstStop,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         1,
				StopId:               "A3",
				ArrivalDelay:         300,
				ScheduledArrivalTime: firstStop.ArrivalDateTime,
				PredictedArrivalTime: fiveMinutesAfterArrival,
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildStopUpdateForFirstStop(tt.args.at, tt.args.stopTime); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildStopUpdateForFirstStop() = \n%+v, want=\n%+v", got, tt.want)
			}
		})
	}
}
