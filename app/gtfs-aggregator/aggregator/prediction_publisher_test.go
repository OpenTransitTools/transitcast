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
	firstStop := trip.StopTimeInstances[0]  //dist: 0.0 		arrive 12:00:00 depart 12:00:00
	secondStop := trip.StopTimeInstances[1] //dist: 1000.0		arrive 12:20:00 depart 12:20:00
	thirdStop := trip.StopTimeInstances[2]  //dist: 2000.0		arrive 12:40:00 depart 12:40:00
	fourthStop := trip.StopTimeInstances[3] //dist: 3000.0 	arrive 13:00:00 depart 13:00:00

	type args struct {
		predictedPositionInTime     time.Time
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
				predictedPositionInTime:     time.Date(2022, 5, 22, 12, 0, 0, 0, location),
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
				predictedPositionInTime:     time.Date(2022, 5, 22, 12, 0, 0, 0, location),
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
				predictedPositionInTime:     time.Date(2022, 5, 22, 12, 10, 0, 0, location),
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
				predictedPositionInTime:     time.Date(2022, 5, 22, 12, 10, 0, 0, location),
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
				predictedPositionInTime:     time.Date(2022, 5, 22, 12, 10, 0, 0, location),
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
				predictedPositionInTime:     time.Date(2022, 5, 22, 12, 20, 0, 0, location),
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
				predictedPositionInTime:     time.Date(2022, 5, 22, 12, 20, 0, 0, location),
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
				predictedPositionInTime:     time.Date(2022, 5, 22, 12, 35, 0, 0, location),
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
		{
			name: "vehicle very late, before stops",
			args: args{
				predictedPositionInTime:     time.Date(2022, 5, 22, 13, 10, 0, 0, location),
				tripDistanceTraveled:        -2500,
				previousPredictionRemainder: 0,
				stopPrediction:              buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
				limitEarlyDepartureSeconds:  60,
			},
			wantStopTimeUpdate: gtfs.StopTimeUpdate{
				StopSequence:         4,
				StopId:               "D",
				ArrivalDelay:         1800,
				ScheduledArrivalTime: fourthStop.ArrivalDateTime,
				PredictedArrivalTime: time.Date(2022, 5, 22, 13, 30, 0, 0, location),
				PredictionSource:     gtfs.TimepointMLPrediction,
			},
			wantPredictionRemainder: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testLog := makeTestLogWriter()
			gotStopTimeUpdate, gotPredictionRemainder := buildStopUpdate(testLog.log, tt.args.predictedPositionInTime,
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

func buildTestPrediction(from *gtfs.StopTimeInstance,
	to *gtfs.StopTimeInstance,
	additionalTime float64,
	predictionSource gtfs.PredictionSource,
	stopUpdateDisposition stopUpdateDisposition,
) *stopPrediction {
	return &stopPrediction{
		fromStop:              from,
		toStop:                to,
		predictedTime:         float64(to.ArrivalTime-from.ArrivalTime) + additionalTime,
		predictionSource:      predictionSource,
		stopUpdateDisposition: stopUpdateDisposition,
		predictionComplete:    true,
	}
}

func buildTestStopUpdateWithDeparture(s *gtfs.StopTimeInstance,
	arrivalDelay int,
	departureDelay int,
	predictionSource gtfs.PredictionSource) gtfs.StopTimeUpdate {
	predictedDepartureTime := s.DepartureDateTime.Add(time.Duration(departureDelay) * time.Second)
	return gtfs.StopTimeUpdate{
		StopSequence:           s.StopSequence,
		StopId:                 s.StopId,
		ArrivalDelay:           arrivalDelay,
		ScheduledArrivalTime:   s.ArrivalDateTime,
		PredictedArrivalTime:   s.ArrivalDateTime.Add(time.Duration(arrivalDelay) * time.Second),
		ScheduledDepartureTime: &s.DepartureDateTime,
		PredictedDepartureTime: &predictedDepartureTime,
		DepartureDelay:         &departureDelay,
		PredictionSource:       predictionSource,
	}
}

func buildTestStopUpdate(s *gtfs.StopTimeInstance,
	arrivalDelay int,
	predictionSource gtfs.PredictionSource) gtfs.StopTimeUpdate {
	return gtfs.StopTimeUpdate{
		StopSequence:         s.StopSequence,
		StopId:               s.StopId,
		ArrivalDelay:         arrivalDelay,
		ScheduledArrivalTime: s.ArrivalDateTime,
		PredictedArrivalTime: s.ArrivalDateTime.Add(time.Duration(arrivalDelay) * time.Second),
		PredictionSource:     predictionSource,
	}
}

func Test_buildTripUpdate(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}

	trip1 := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_1.json", t)
	firstStop := trip1.StopTimeInstances[0]   //dist: 0.0 		arrive 12:00:00 depart 12:00:00
	secondStop := trip1.StopTimeInstances[1]  //dist: 1000.0	arrive 12:20:00 depart 12:20:00
	thirdStop := trip1.StopTimeInstances[2]   //dist: 2000.0	arrive 12:40:00 depart 12:40:00
	fourthStop := trip1.StopTimeInstances[3]  //dist: 3000.0 	arrive 13:00:00 depart 13:00:00
	fifthStop := trip1.StopTimeInstances[4]   //dist: 4000.0    arrive 13:20:00 depart 13:20:00
	sixthStop := trip1.StopTimeInstances[5]   //dist: 5000.0 arrive 13:30:00 depart 13:31:40 -- timepoint, dwells
	seventhStop := trip1.StopTimeInstances[6] //dist: 6000.0 arrive 13:41:40 depart 13:41:40

	eleven50Am := time.Date(2022, 5, 22, 11, 50, 0, 0, location)
	eleven59Am := time.Date(2022, 5, 22, 11, 59, 0, 0, location)
	twelvePm := time.Date(2022, 5, 22, 12, 0, 0, 0, location)
	twelve10Pm := time.Date(2022, 5, 22, 12, 10, 0, 0, location)
	twelve20Pm := time.Date(2022, 5, 22, 12, 20, 0, 0, location)
	twelve40Pm := time.Date(2022, 5, 22, 12, 40, 0, 0, location)
	twelve58Pm := time.Date(2022, 5, 22, 12, 58, 0, 0, location)

	timeAt1302 := time.Date(2022, 5, 22, 13, 02, 0, 0, location)
	timeAt1310 := time.Date(2022, 5, 22, 13, 10, 0, 0, location)
	timeAt1320 := time.Date(2022, 5, 22, 13, 20, 0, 0, location)
	timeAt1330 := time.Date(2022, 5, 22, 13, 30, 0, 0, location)

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
				limitEarlyDepartureSeconds:   60,
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
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 0.0, gtfs.StopMLPrediction, FutureStop),
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
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(secondStop, 0, gtfs.StopMLPrediction),
					buildTestStopUpdate(thirdStop, 0, gtfs.StopMLPrediction),
					buildTestStopUpdate(fourthStop, 0, gtfs.StopMLPrediction),
					buildTestStopUpdate(fifthStop, 0, gtfs.StopMLPrediction),
					buildTestStopUpdate(sixthStop, 0, gtfs.StopMLPrediction),
					buildTestStopUpdate(seventhStop, 0, gtfs.StopMLPrediction),
				},
			},
		},
		{
			name: "Simple, prior to trip by one minute, at first stop",
			args: args{
				previousSchedulePositionTime: eleven59Am,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          eleven59Am,
						DeviationTimestamp: eleven59Am,
						TripProgress:       -500,
						TripId:             trip1.TripId,
						VehicleId:          "1",
						Delay:              -60,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 0.0, gtfs.StopMLPrediction, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(eleven59Am.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(secondStop, 0, gtfs.StopMLPrediction),
					buildTestStopUpdate(thirdStop, 0, gtfs.StopMLPrediction),
					buildTestStopUpdate(fourthStop, 0, gtfs.StopMLPrediction),
					buildTestStopUpdate(fifthStop, 0, gtfs.StopMLPrediction),
					buildTestStopUpdate(sixthStop, 0, gtfs.StopMLPrediction),
					buildTestStopUpdate(seventhStop, 0, gtfs.StopMLPrediction),
				},
			},
		},
		{
			name: "One minute late, at first stop",
			args: args{
				previousSchedulePositionTime: twelvePm.Add(time.Minute),
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          eleven59Am,
						DeviationTimestamp: eleven59Am,
						TripProgress:       0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
						Delay:              -60,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 0.0, gtfs.StopMLPrediction, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(eleven59Am.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdateWithDeparture(firstStop, 60, 60, gtfs.SchedulePrediction),
					buildTestStopUpdate(secondStop, 60, gtfs.StopMLPrediction),
					buildTestStopUpdate(thirdStop, 60, gtfs.StopMLPrediction),
					buildTestStopUpdate(fourthStop, 60, gtfs.StopMLPrediction),
					buildTestStopUpdate(fifthStop, 60, gtfs.StopMLPrediction),
					buildTestStopUpdate(sixthStop, 60, gtfs.StopMLPrediction),
					buildTestStopUpdate(seventhStop, 60, gtfs.StopMLPrediction),
				},
			},
		},
		{
			name: "10 minutes late, between first amd second stop",
			args: args{
				previousSchedulePositionTime: twelve20Pm,
				limitEarlyDepartureSeconds:   60,
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
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 0.0, gtfs.StopMLPrediction, FutureStop),
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
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(secondStop, 600, gtfs.StopMLPrediction),
					buildTestStopUpdate(thirdStop, 600, gtfs.StopMLPrediction),
					buildTestStopUpdate(fourthStop, 600, gtfs.StopMLPrediction),
					buildTestStopUpdate(fifthStop, 600, gtfs.StopMLPrediction),
					buildTestStopUpdate(sixthStop, 600, gtfs.StopMLPrediction),
					buildTestStopUpdate(seventhStop, 600, gtfs.StopMLPrediction),
				},
			},
		},
		{
			name: "2 minutes early, just left third stop",
			args: args{
				previousSchedulePositionTime: twelve40Pm,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          twelve40Pm,
						DeviationTimestamp: twelve40Pm,
						TripProgress:       2100.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 0.0, gtfs.StopMLPrediction, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(twelve40Pm.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(thirdStop, -60, gtfs.SchedulePrediction), //past this stop
					buildTestStopUpdate(fourthStop, -120, gtfs.StopMLPrediction),
					buildTestStopUpdate(fifthStop, -120, gtfs.StopMLPrediction),
					buildTestStopUpdate(sixthStop, -60, gtfs.StopMLPrediction),
					buildTestStopUpdate(seventhStop, -60, gtfs.StopMLPrediction),
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
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.StopMLPrediction, AtStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 0.0, gtfs.StopMLPrediction, FutureStop),
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
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(thirdStop, 0, gtfs.SchedulePrediction),     //last past stop
					buildTestStopUpdate(fourthStop, -120, gtfs.SchedulePrediction), //at this stop
					buildTestStopUpdate(fifthStop, -120, gtfs.StopMLPrediction),
					buildTestStopUpdate(sixthStop, -60, gtfs.StopMLPrediction),
					buildTestStopUpdate(seventhStop, -60, gtfs.StopMLPrediction),
				},
			},
		},
		{
			name: "late, half way between fifth and sixth stop, estimate extra time to get to stop seven",
			args: args{
				previousSchedulePositionTime: timeAt1330,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1330,
						DeviationTimestamp: timeAt1330,
						TripProgress:       4500.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 200.0, gtfs.StopMLPrediction, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(timeAt1330.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(fifthStop, 0, gtfs.SchedulePrediction), //last past stop
					buildTestStopUpdate(sixthStop, 300, gtfs.StopMLPrediction),
					buildTestStopUpdate(seventhStop, 500, gtfs.StopMLPrediction),
				},
			},
		},
		{
			name: "early, half way between fifth and sixth stop, extra time estimated between stop six and seven",
			args: args{
				previousSchedulePositionTime: timeAt1320,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1320,
						DeviationTimestamp: timeAt1320,
						TripProgress:       4500.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 200.0, gtfs.StopMLPrediction, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(timeAt1320.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(fifthStop, -60, gtfs.SchedulePrediction), //last past stop
					buildTestStopUpdate(sixthStop, -300, gtfs.StopMLPrediction),
					buildTestStopUpdate(seventhStop, -60, gtfs.StopMLPrediction),
				},
			},
		},
		{
			name: "late, half way between fifth and sixth stop, timepoint stop transitions before trip progress",
			args: args{
				previousSchedulePositionTime: timeAt1330,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1330,
						DeviationTimestamp: timeAt1330,
						TripProgress:       4500.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.TimepointMLPrediction, PastStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.TimepointMLPrediction, PastStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 200.0, gtfs.TimepointMLPrediction, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(timeAt1330.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(fifthStop, 0, gtfs.SchedulePrediction), //last past stop
					buildTestStopUpdate(sixthStop, 300, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(seventhStop, 500, gtfs.TimepointMLPrediction),
				},
			},
		},
		{
			name: "late, half way between fifth and sixth stop, timepoint stop transitions from beginning of trip",
			args: args{
				previousSchedulePositionTime: timeAt1330,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1330,
						DeviationTimestamp: timeAt1330,
						TripProgress:       4500.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.TimepointMLPrediction, PastStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.TimepointMLPrediction, PastStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.TimepointMLPrediction, PastStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.TimepointMLPrediction, PastStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 200.0, gtfs.TimepointMLPrediction, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(timeAt1330.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(fifthStop, 0, gtfs.SchedulePrediction), //last past stop
					buildTestStopUpdate(sixthStop, 300, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(seventhStop, 500, gtfs.TimepointMLPrediction),
				},
			},
		},
		{
			name: "2 minutes late, at fourth stop , predictions to end of trip, timepoint predictions prior to location",
			args: args{
				previousSchedulePositionTime: timeAt1302,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1302,
						DeviationTimestamp: timeAt1302,
						TripProgress:       3000.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.TimepointMLPrediction, PastStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.TimepointMLPrediction, PastStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.TimepointMLPrediction, AtStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(timeAt1302.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(thirdStop, 0, gtfs.SchedulePrediction),    //last past stop
					buildTestStopUpdate(fourthStop, 120, gtfs.SchedulePrediction), //at stop
					buildTestStopUpdate(fifthStop, 120, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(sixthStop, 120, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(seventhStop, 120, gtfs.TimepointMLPrediction),
				},
			},
		},
		{
			name: "10 minutes late, between 3rd and fourth stop , predictions to end of trip, timepoint predictions prior to location",
			args: args{
				previousSchedulePositionTime: timeAt1310,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1310,
						DeviationTimestamp: timeAt1310,
						TripProgress:       2500.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.TimepointMLPrediction, PastStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.TimepointMLPrediction, PastStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 200.0, gtfs.TimepointMLPrediction, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(timeAt1310.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(thirdStop, 0, gtfs.SchedulePrediction), //last past stop
					buildTestStopUpdate(fourthStop, 1200, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(fifthStop, 1200, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(sixthStop, 1200, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(seventhStop, 1400, gtfs.TimepointMLPrediction),
				},
			},
		},
		{
			name: "10 minutes late (so far), before this trip, predictions to last stop",
			args: args{
				previousSchedulePositionTime: twelve10Pm,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          eleven59Am,
						DeviationTimestamp: eleven59Am,
						TripProgress:       -2500.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(sixthStop, seventhStop, 200.0, gtfs.NoFurtherPredictions, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(eleven59Am.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdateWithDeparture(firstStop, 600, 600, gtfs.SchedulePrediction),
					buildTestStopUpdate(secondStop, 600, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(thirdStop, 600, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(fourthStop, 600, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(fifthStop, 600, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(sixthStop, 600, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(seventhStop, 800, gtfs.NoFurtherPredictions),
				},
			},
		},
		{
			name: "10 minutes early, before this trip, predictions to sixth stop",
			args: args{
				previousSchedulePositionTime: eleven50Am,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          eleven50Am,
						DeviationTimestamp: eleven50Am,
						TripProgress:       -500.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.NoFurtherPredictions, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(eleven50Am.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(secondStop, 0, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(thirdStop, 0, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(fourthStop, 0, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(fifthStop, 0, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(sixthStop, 0, gtfs.NoFurtherPredictions),
				},
			},
		},
		{
			name: "10 minutes early, at first stop of this trip, predictions to sixth stop",
			args: args{
				previousSchedulePositionTime: twelvePm,
				limitEarlyDepartureSeconds:   60,
				prediction: &tripPrediction{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          eleven50Am,
						DeviationTimestamp: eleven50Am,
						TripProgress:       0.0,
						TripId:             trip1.TripId,
						VehicleId:          "1",
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(firstStop, secondStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(secondStop, thirdStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(thirdStop, fourthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(fourthStop, fifthStop, 0.0, gtfs.TimepointMLPrediction, FutureStop),
						buildTestPrediction(fifthStop, sixthStop, 0.0, gtfs.NoFurtherPredictions, FutureStop),
					},
					tripInstance: trip1,
				},
			},
			want: &gtfs.TripUpdate{
				TripId:               trip1.TripId,
				RouteId:              trip1.RouteId,
				ScheduleRelationship: "SCHEDULED",
				Timestamp:            uint64(eleven50Am.Unix()),
				VehicleId:            "1",
				StopTimeUpdates: []gtfs.StopTimeUpdate{
					buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
					buildTestStopUpdate(secondStop, 0, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(thirdStop, 0, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(fourthStop, 0, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(fifthStop, 0, gtfs.TimepointMLPrediction),
					buildTestStopUpdate(sixthStop, 0, gtfs.NoFurtherPredictions),
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

func Test_makeTripUpdates(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}

	trip2 := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_2.json", t)
	stop1Trip2 := trip2.StopTimeInstances[0] //arrive & depart: 13:43:00
	stop2Trip2 := trip2.StopTimeInstances[1] //arrive & depart: 13:46:00
	stop3Trip2 := trip2.StopTimeInstances[2] //arrive & depart: 13:49:00

	trip3 := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_3.json", t)
	stop1Trip3 := trip3.StopTimeInstances[0] //arrive: 13:49:00 depart: 13:52:00
	stop2Trip3 := trip3.StopTimeInstances[1] //arrive & depart: 13:55:00
	stop3Trip3 := trip3.StopTimeInstances[2] //arrive & depart: 13:58:00

	trip4 := getTestTrip(time.Date(2022, 5, 22, 0, 0, 0, 0, location),
		"trip_instance_3.json", t)
	stop1Trip4 := trip4.StopTimeInstances[0] //arrive: 13:50:00 depart: 13:53:00
	stop2Trip4 := trip4.StopTimeInstances[1] //arrive & depart: 13:55:00
	stop3Trip4 := trip4.StopTimeInstances[2] //arrive & depart: 13:58:00

	timeAt1343 := time.Date(2022, 5, 22, 13, 43, 0, 0, location)
	timeAt1348 := time.Date(2022, 5, 22, 13, 48, 0, 0, location)
	timeAt1353 := time.Date(2022, 5, 22, 13, 53, 0, 0, location)
	timeAt140730 := time.Date(2022, 5, 22, 14, 7, 30, 0, location)

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
					tripInstance: trip2,
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1343,
						DeviationTimestamp: timeAt1343,
						TripProgress:       0.0,
						TripId:             trip2.TripId,
						VehicleId:          "1",
						Delay:              0,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(stop1Trip2, stop2Trip2, 0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(stop2Trip2, stop3Trip2, 0, gtfs.StopMLPrediction, FutureStop),
					},
				},
				{
					tripInstance: trip3,
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1343,
						DeviationTimestamp: timeAt1343,
						TripProgress:       -2000.0,
						TripId:             trip3.TripId,
						VehicleId:          "1",
						Delay:              0,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(stop1Trip3, stop2Trip3, 0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(stop2Trip3, stop3Trip3, 0, gtfs.StopMLPrediction, FutureStop),
					},
				},
			},
			want: []*gtfs.TripUpdate{
				{
					TripId:               trip2.TripId,
					RouteId:              trip2.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(timeAt1343.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						buildTestStopUpdate(stop1Trip2, 0, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop2Trip2, 0, gtfs.StopMLPrediction),
						buildTestStopUpdate(stop3Trip2, 0, gtfs.StopMLPrediction),
					},
				},
				{
					TripId:               trip3.TripId,
					RouteId:              trip3.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(timeAt1343.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						buildTestStopUpdate(stop1Trip3, 0, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop2Trip3, 0, gtfs.StopMLPrediction),
						buildTestStopUpdate(stop3Trip3, 0, gtfs.StopMLPrediction),
					},
				},
			},
		},
		{
			name: "Early in middle of first trip",
			orderedPredictions: []*tripPrediction{
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1343,
						DeviationTimestamp: timeAt1343,
						TripProgress:       1000.0,
						TripId:             trip2.TripId,
						VehicleId:          "1",
						Delay:              -180,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(stop1Trip2, stop2Trip2, 0, gtfs.StopMLPrediction, AtStop),
						buildTestPrediction(stop2Trip2, stop3Trip2, 0, gtfs.StopMLPrediction, FutureStop),
					},
					tripInstance:       trip2,
					pendingPredictions: 0,
				},
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1343,
						DeviationTimestamp: timeAt1343,
						TripProgress:       -1000.0,
						TripId:             trip3.TripId,
						VehicleId:          "1",
						Delay:              -180,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(stop1Trip3, stop2Trip3, 0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(stop2Trip3, stop3Trip3, 0, gtfs.StopMLPrediction, FutureStop),
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
					Timestamp:            uint64(timeAt1343.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						buildTestStopUpdate(stop1Trip2, -180, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop2Trip2, -180, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop3Trip2, -180, gtfs.StopMLPrediction),
					},
				},
				{
					TripId:               trip3.TripId,
					RouteId:              trip3.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(timeAt1343.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						buildTestStopUpdate(stop1Trip3, 0, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop2Trip3, 0, gtfs.StopMLPrediction),
						buildTestStopUpdate(stop3Trip3, 0, gtfs.StopMLPrediction),
					},
				},
			},
		},
		{
			name: "Late prediction in middle of first trip, depart second trip on time",
			orderedPredictions: []*tripPrediction{
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1348,
						DeviationTimestamp: timeAt1348,
						TripProgress:       1000.0,
						TripId:             trip2.TripId,
						VehicleId:          "1",
						Delay:              120,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(stop1Trip2, stop2Trip2, 0, gtfs.StopMLPrediction, AtStop),
						buildTestPrediction(stop2Trip2, stop3Trip2, 0, gtfs.StopMLPrediction, FutureStop),
					},
					tripInstance:       trip2,
					pendingPredictions: 0,
				},
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1348,
						DeviationTimestamp: timeAt1348,
						TripProgress:       -1000.0,
						TripId:             trip3.TripId,
						VehicleId:          "1",
						Delay:              120,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(stop1Trip3, stop2Trip3, 0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(stop2Trip3, stop3Trip3, 0, gtfs.StopMLPrediction, FutureStop),
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
					Timestamp:            uint64(timeAt1348.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						buildTestStopUpdate(stop1Trip2, 120, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop2Trip2, 120, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop3Trip2, 120, gtfs.StopMLPrediction),
					},
				},
				{
					TripId:               trip3.TripId,
					RouteId:              trip3.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(timeAt1348.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						buildTestStopUpdate(stop1Trip3, 0, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop2Trip3, 0, gtfs.StopMLPrediction),
						buildTestStopUpdate(stop3Trip3, 0, gtfs.StopMLPrediction),
					},
				},
			},
		},
		{
			name: "Late prediction in middle of first trip, trip4 starts a minute later",
			orderedPredictions: []*tripPrediction{
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1353,
						DeviationTimestamp: timeAt1353,
						TripProgress:       1000.0,
						TripId:             trip2.TripId,
						VehicleId:          "1",
						Delay:              420,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(stop1Trip2, stop2Trip2, 0, gtfs.StopMLPrediction, AtStop),
						buildTestPrediction(stop2Trip2, stop3Trip2, -120, gtfs.StopMLPrediction, FutureStop), //two minutes faster than scheduled
					},
					tripInstance: trip2,
				},
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt1353,
						DeviationTimestamp: timeAt1353,
						TripProgress:       -1000.0,
						TripId:             trip4.TripId,
						VehicleId:          "1",
						Delay:              420,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(stop1Trip4, stop2Trip4, -120, gtfs.StopMLPrediction, FutureStop), //120 seconds faster
						buildTestPrediction(stop2Trip4, stop3Trip4, 60, gtfs.StopMLPrediction, FutureStop),   //60 seconds slower
					},
					tripInstance:       trip4,
					pendingPredictions: 0,
				},
			},
			want: []*gtfs.TripUpdate{
				{
					TripId:               trip2.TripId,
					RouteId:              trip2.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(timeAt1353.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						buildTestStopUpdate(stop1Trip2, 420, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop2Trip2, 420, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop3Trip2, 300, gtfs.StopMLPrediction),
					},
				},
				{
					TripId:               trip4.TripId,
					RouteId:              trip4.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(timeAt1353.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						buildTestStopUpdate(stop1Trip4, 300, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop2Trip4, 180, gtfs.StopMLPrediction), //makes up 2 minutes
						buildTestStopUpdate(stop3Trip4, 240, gtfs.StopMLPrediction), //looses a minute
					},
				},
			},
		},
		{
			name: "Late prediction in middle of trip2",
			orderedPredictions: []*tripPrediction{
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt140730,
						DeviationTimestamp: timeAt140730,
						TripProgress:       1500.0,
						TripId:             trip2.TripId,
						VehicleId:          "1",
						Delay:              1200,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(stop1Trip2, stop2Trip2, 0, gtfs.StopMLPrediction, PastStop),
						buildTestPrediction(stop2Trip2, stop3Trip2, 0, gtfs.StopMLPrediction, FutureStop),
					},
					tripInstance:       trip2,
					pendingPredictions: 0,
				},
				{
					tripDeviation: &gtfs.TripDeviation{
						CreatedAt:          timeAt140730,
						DeviationTimestamp: timeAt140730,
						TripProgress:       -500.0,
						TripId:             trip3.TripId,
						VehicleId:          "1",
						Delay:              1200,
					},
					mu: sync.Mutex{},
					stopPredictions: []*stopPrediction{
						buildTestPrediction(stop1Trip3, stop2Trip3, 0, gtfs.StopMLPrediction, FutureStop),
						buildTestPrediction(stop2Trip3, stop3Trip3, 0, gtfs.StopMLPrediction, FutureStop),
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
					Timestamp:            uint64(timeAt140730.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						buildTestStopUpdate(stop1Trip2, 1200, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop2Trip2, 1200, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop3Trip2, 1200, gtfs.StopMLPrediction),
					},
				},
				{
					TripId:               trip3.TripId,
					RouteId:              trip3.RouteId,
					ScheduleRelationship: "SCHEDULED",
					Timestamp:            uint64(timeAt140730.Unix()),
					VehicleId:            "1",
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						buildTestStopUpdate(stop1Trip3, 1200, gtfs.SchedulePrediction),
						buildTestStopUpdate(stop2Trip3, 1200, gtfs.StopMLPrediction),
						buildTestStopUpdate(stop3Trip3, 1200, gtfs.StopMLPrediction),
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

	firstStop := &gtfs.StopTimeInstance{
		StopTime: gtfs.StopTime{
			StopSequence: 1,
			StopId:       "A3",
			Timepoint:    1,
		},
		FirstStop:         true,
		ArrivalDateTime:   time.Date(2022, 5, 22, 13, 49, 0, 0, location),
		DepartureDateTime: time.Date(2022, 5, 22, 13, 52, 0, 0, location),
	}

	timeAt1339 := time.Date(2022, 5, 22, 13, 39, 0, 0, location)
	timeAt1344 := time.Date(2022, 5, 22, 13, 44, 0, 0, location)
	timeAt1346 := time.Date(2022, 5, 22, 13, 46, 0, 0, location)
	timeAt1347 := time.Date(2022, 5, 22, 13, 47, 0, 0, location)
	timeAt1350 := time.Date(2022, 5, 22, 13, 50, 0, 0, location)
	timeAt135201 := time.Date(2022, 5, 22, 13, 52, 1, 0, location)
	timeAt1353 := time.Date(2022, 5, 22, 13, 53, 0, 0, location)
	timeAt1356 := time.Date(2022, 5, 22, 13, 56, 0, 0, location)
	timeAt1357 := time.Date(2022, 5, 22, 13, 57, 0, 0, location)
	timeAt1402 := time.Date(2022, 5, 22, 14, 2, 0, 0, location)

	type args struct {
		predictedPositionInTime time.Time
		positionInSchedule      time.Time
		positionTimestamp       time.Time
		stopTime                *gtfs.StopTimeInstance
		delay                   int
	}
	tests := []struct {
		name string
		args args
		want gtfs.StopTimeUpdate
	}{
		{
			name: "Time exactly at arrival time of stop",
			args: args{
				predictedPositionInTime: firstStop.ArrivalDateTime,
				positionInSchedule:      firstStop.ArrivalDateTime,
				positionTimestamp:       firstStop.ArrivalDateTime,
				stopTime:                firstStop,
				delay:                   0,
			},
			want: buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
		},
		{
			name: "Time is before arrive time of stop, on time",
			args: args{
				predictedPositionInTime: timeAt1347,
				positionInSchedule:      timeAt1347,
				positionTimestamp:       timeAt1347,
				stopTime:                firstStop,
				delay:                   0,
			},
			want: buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
		},
		{
			name: "Time is one second after depart time",
			args: args{
				predictedPositionInTime: timeAt135201,
				positionInSchedule:      firstStop.ArrivalDateTime,
				positionTimestamp:       timeAt135201,
				stopTime:                firstStop,
				delay:                   181,
			},
			want: buildTestStopUpdate(firstStop, 181, gtfs.SchedulePrediction),
		},
		{
			name: "Time is one minute after arrive time but before depart time",
			args: args{
				predictedPositionInTime: timeAt1350,
				positionInSchedule:      firstStop.ArrivalDateTime,
				positionTimestamp:       timeAt1350,
				stopTime:                firstStop,
				delay:                   0,
			},
			want: buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
		},
		{
			name: "Time is one minute after arrive time but before depart time, predictedPositionInTime is one minute after depart time",
			args: args{
				predictedPositionInTime: timeAt1353,
				positionInSchedule:      timeAt1344,
				positionTimestamp:       timeAt1353,
				stopTime:                firstStop,
				delay:                   240,
			},
			want: buildTestStopUpdate(firstStop, 240, gtfs.SchedulePrediction),
		},
		{
			name: "Time is five minutes after depart time, at the stop",
			args: args{
				predictedPositionInTime: timeAt1357,
				positionInSchedule:      firstStop.ArrivalDateTime,
				positionTimestamp:       timeAt1357,
				stopTime:                firstStop,
				delay:                   480,
			},
			want: buildTestStopUpdate(firstStop, 480, gtfs.SchedulePrediction),
		},
		{
			name: "five minutes before depart time, position is before stop, predictedPositionInTime is after stop",
			args: args{
				predictedPositionInTime: timeAt1357,
				positionInSchedule:      timeAt1344,
				positionTimestamp:       timeAt1344,
				stopTime:                firstStop,
				delay:                   480,
			},
			want: buildTestStopUpdateWithDeparture(firstStop, 480, 300, gtfs.SchedulePrediction),
		},
		{
			name: "Started trip, and are five minutes early",
			args: args{
				predictedPositionInTime: timeAt1402,
				positionInSchedule:      timeAt1402,
				positionTimestamp:       timeAt1344,
				stopTime:                firstStop,
				delay:                   -1080,
			},
			want: buildTestStopUpdate(firstStop, -1080, gtfs.SchedulePrediction),
		},
		{
			name: "Started trip, and five minutes late",
			args: args{
				predictedPositionInTime: timeAt1357,
				positionInSchedule:      timeAt1357,
				positionTimestamp:       timeAt1402,
				stopTime:                firstStop,
				delay:                   0,
			},
			want: buildTestStopUpdate(firstStop, 0, gtfs.SchedulePrediction),
		},
		{
			name: "Previous trip, and seven minutes late",
			args: args{
				predictedPositionInTime: timeAt1356,
				positionInSchedule:      timeAt1339,
				positionTimestamp:       timeAt1356,
				stopTime:                firstStop,
				delay:                   420,
			},
			want: buildTestStopUpdate(firstStop, 420, gtfs.SchedulePrediction),
		},
		{
			name: "Previous trip, and four minutes after departure minutes, timestamp before location",
			args: args{
				predictedPositionInTime: timeAt1356,
				positionInSchedule:      timeAt1346,
				positionTimestamp:       timeAt1353,
				stopTime:                firstStop,
				delay:                   420,
			},
			want: buildTestStopUpdate(firstStop, 420, gtfs.SchedulePrediction),
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			got := buildStopUpdateForFirstStop(tt.args.predictedPositionInTime, tt.args.positionInSchedule,
				tt.args.positionTimestamp, time.Duration(tt.args.delay)*time.Second, tt.args.stopTime)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildStopUpdateForFirstStop() = \n%s, \nwant=\n%s",
					sprintStopUpdate(got), sprintStopUpdate(tt.want))
			}
		})
	}
}

func Test_buildStopUpdateForAtStop(t *testing.T) {

	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}

	stop1 := &gtfs.StopTimeInstance{
		StopTime: gtfs.StopTime{
			StopSequence: 3,
			StopId:       "A",
			Timepoint:    0,
		},
		FirstStop:         false,
		ArrivalDateTime:   time.Date(2022, 5, 22, 10, 0, 0, 0, location),
		DepartureDateTime: time.Date(2022, 5, 22, 10, 0, 0, 0, location),
	}
	timepointStop1 := &gtfs.StopTimeInstance{
		StopTime: gtfs.StopTime{
			StopSequence: 3,
			StopId:       "A",
			Timepoint:    1,
		},
		FirstStop:         false,
		ArrivalDateTime:   time.Date(2022, 5, 22, 10, 0, 0, 0, location),
		DepartureDateTime: time.Date(2022, 5, 22, 10, 3, 0, 0, location),
	}
	type args struct {
		at                         time.Time
		stopTime                   *gtfs.StopTimeInstance
		limitEarlyDepartureSeconds int
	}
	tests := []struct {
		name string
		args args
		want gtfs.StopTimeUpdate
	}{
		{
			name: "on schedule at stop",
			args: args{
				at:                         stop1.ArrivalDateTime,
				stopTime:                   stop1,
				limitEarlyDepartureSeconds: 60,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         stop1.StopSequence,
				StopId:               stop1.StopId,
				ArrivalDelay:         0,
				ScheduledArrivalTime: stop1.ArrivalDateTime,
				PredictedArrivalTime: stop1.ArrivalDateTime,
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "early at stop, not timepoint",
			args: args{
				at:                         stop1.ArrivalDateTime.Add(time.Duration(-30) * time.Second),
				stopTime:                   stop1,
				limitEarlyDepartureSeconds: 60,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         stop1.StopSequence,
				StopId:               stop1.StopId,
				ArrivalDelay:         -30,
				ScheduledArrivalTime: stop1.ArrivalDateTime,
				PredictedArrivalTime: stop1.ArrivalDateTime.Add(time.Duration(-30) * time.Second),
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "early at timepoint, under limitEarlyDepartureSeconds",
			args: args{
				at:                         timepointStop1.ArrivalDateTime.Add(time.Duration(-30) * time.Second),
				stopTime:                   stop1,
				limitEarlyDepartureSeconds: 60,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         timepointStop1.StopSequence,
				StopId:               timepointStop1.StopId,
				ArrivalDelay:         -30,
				ScheduledArrivalTime: timepointStop1.ArrivalDateTime,
				PredictedArrivalTime: timepointStop1.ArrivalDateTime.Add(time.Duration(-30) * time.Second),
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "early at timepoint, over limitEarlyDepartureSeconds",
			args: args{
				at:                         timepointStop1.ArrivalDateTime.Add(time.Duration(-90) * time.Second),
				stopTime:                   timepointStop1,
				limitEarlyDepartureSeconds: 60,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         timepointStop1.StopSequence,
				StopId:               timepointStop1.StopId,
				ArrivalDelay:         -60,
				ScheduledArrivalTime: timepointStop1.ArrivalDateTime,
				PredictedArrivalTime: timepointStop1.ArrivalDateTime.Add(time.Duration(-60) * time.Second),
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "late at stop",
			args: args{
				at:                         stop1.ArrivalDateTime.Add(time.Duration(90) * time.Second),
				stopTime:                   stop1,
				limitEarlyDepartureSeconds: 60,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         stop1.StopSequence,
				StopId:               stop1.StopId,
				ArrivalDelay:         90,
				ScheduledArrivalTime: stop1.ArrivalDateTime,
				PredictedArrivalTime: stop1.ArrivalDateTime.Add(time.Duration(90) * time.Second),
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "late at timepoint",
			args: args{
				at:                         timepointStop1.ArrivalDateTime.Add(time.Duration(90) * time.Second),
				stopTime:                   timepointStop1,
				limitEarlyDepartureSeconds: 60,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:           timepointStop1.StopSequence,
				StopId:                 timepointStop1.StopId,
				ArrivalDelay:           90,
				ScheduledArrivalTime:   timepointStop1.ArrivalDateTime,
				PredictedArrivalTime:   timepointStop1.ArrivalDateTime.Add(time.Duration(90) * time.Second),
				ScheduledDepartureTime: nil,
				PredictionSource:       gtfs.SchedulePrediction,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildStopUpdateForAtStop(tt.args.at, tt.args.stopTime, tt.args.limitEarlyDepartureSeconds); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildStopUpdateForAtStop() got=\n%s,\nwant=\n%s", sprintStopUpdate(got), sprintStopUpdate(tt.want))
			}
		})
	}
}

func Test_buildStopUpdateForPassedStop(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}

	stop1 := &gtfs.StopTimeInstance{
		StopTime: gtfs.StopTime{
			StopSequence: 3,
			StopId:       "A",
			Timepoint:    0,
		},
		FirstStop:         false,
		ArrivalDateTime:   time.Date(2022, 5, 22, 10, 0, 0, 0, location),
		DepartureDateTime: time.Date(2022, 5, 22, 10, 0, 0, 0, location),
	}
	dwellingStop := &gtfs.StopTimeInstance{
		StopTime: gtfs.StopTime{
			StopSequence: 3,
			StopId:       "A",
			Timepoint:    1,
		},
		FirstStop:         false,
		ArrivalDateTime:   time.Date(2022, 5, 22, 10, 0, 0, 0, location),
		DepartureDateTime: time.Date(2022, 5, 22, 10, 3, 0, 0, location),
	}

	type args struct {
		at       time.Time
		stopTime *gtfs.StopTimeInstance
		delay    int
	}
	tests := []struct {
		name string
		args args
		want gtfs.StopTimeUpdate
	}{
		{
			name: "Passed stop at its arrival time",
			args: args{
				at:       stop1.ArrivalDateTime,
				stopTime: stop1,
				delay:    -60,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         stop1.StopSequence,
				StopId:               stop1.StopId,
				ArrivalDelay:         -60,
				ScheduledArrivalTime: stop1.ArrivalDateTime,
				PredictedArrivalTime: stop1.ArrivalDateTime.Add(time.Duration(-60) * time.Second),
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "Passed stop five minutes after its arrival time",
			args: args{
				at:       stop1.ArrivalDateTime.Add(time.Duration(300) * time.Second),
				stopTime: stop1,
				delay:    0,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         stop1.StopSequence,
				StopId:               stop1.StopId,
				ArrivalDelay:         0,
				ScheduledArrivalTime: stop1.ArrivalDateTime,
				PredictedArrivalTime: stop1.ArrivalDateTime,
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "Passed stop five minutes before its arrival time",
			args: args{
				at:       stop1.ArrivalDateTime.Add(time.Duration(-300) * time.Second),
				stopTime: stop1,
				delay:    -360,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         stop1.StopSequence,
				StopId:               stop1.StopId,
				ArrivalDelay:         -360, //how early it is plus a minute
				ScheduledArrivalTime: stop1.ArrivalDateTime,
				PredictedArrivalTime: stop1.ArrivalDateTime.Add(time.Duration(-360) * time.Second),
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "Passed stop at its arrival time, but not before its departure",
			args: args{
				at:       dwellingStop.ArrivalDateTime,
				stopTime: dwellingStop,
				delay:    -300,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         dwellingStop.StopSequence,
				StopId:               dwellingStop.StopId,
				ArrivalDelay:         -300, //how early it departs plus a minute (departs at 9:59, scheduled for 10:03)
				ScheduledArrivalTime: dwellingStop.ArrivalDateTime,
				PredictedArrivalTime: dwellingStop.ArrivalDateTime.Add(time.Duration(-300) * time.Second),
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
		{
			name: "Passed stop 60 seconds before its departure time",
			args: args{
				at:       dwellingStop.DepartureDateTime.Add(-1 * time.Minute),
				stopTime: dwellingStop,
				delay:    -180,
			},
			want: gtfs.StopTimeUpdate{
				StopSequence:         dwellingStop.StopSequence,
				StopId:               dwellingStop.StopId,
				ArrivalDelay:         -180, //how early it departs plus a minute (departed at 10:02, scheduled for 10:03)
				ScheduledArrivalTime: dwellingStop.ArrivalDateTime,
				PredictedArrivalTime: dwellingStop.ArrivalDateTime.Add(time.Duration(-180) * time.Second),
				PredictionSource:     gtfs.SchedulePrediction,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildStopUpdateForPassedStop(tt.args.at, tt.args.stopTime, time.Duration(tt.args.delay)*time.Second); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildStopUpdateForPassedStop()\ngot=\n%s,\nwant=\n%s",
					sprintStopUpdate(got), sprintStopUpdate(tt.want))
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
		parts = append(parts, sprintStopUpdate(su))
	}
	return strings.Join(parts, "\n")
}

func sprintStopUpdate(su gtfs.StopTimeUpdate) string {
	departurePart := ""
	if su.PredictedDepartureTime != nil {
		departurePart = fmt.Sprintf(" DepartureDelay:%v, ScheduledDepartureTime:%v PredictedDepartureTime:%v",
			*su.DepartureDelay, *su.ScheduledDepartureTime, *su.PredictedDepartureTime)
	}
	return fmt.Sprintf("{StopSequence:%d StopId:%s ArrivalDelay:%d ScheduledArrivalTime:%v PredictedArrivalTime:%v PredictionSource:%d%s}",
		su.StopSequence, su.StopId, su.ArrivalDelay, su.ScheduledArrivalTime, su.PredictedArrivalTime, su.PredictionSource, departurePart)
}
