package monitor

import (
	"encoding/json"
	"fmt"
	"gitlab.trimet.org/transittracker/transitmon/business/data/gtfs"
	"io/ioutil"
	"reflect"

	"strings"
	"testing"
	"time"
)

func makeVehiclePositionStopId(id string, label string, tripId string, stopSequence uint32,
	stopPosition VehicleStopStatus, timeStamp int64, stopId string) vehiclePosition {
	return vehiclePosition{
		Id:                id,
		Label:             label,
		Timestamp:         timeStamp,
		TripId:            &tripId,
		VehicleStopStatus: stopPosition,
		StopSequence:      &stopSequence,
		StopId:            &stopId,
	}
}

var spacedStopSequenceTrip = &gtfs.TripInstance{
	Trip: gtfs.Trip{
		TripId:        "1000",
		RouteId:       "100",
		ServiceId:     "A",
		BlockId:       strPtr("9020"),
		TripHeadsign:  strPtr("Cleveland Ave MAX Station"),
		TripShortName: strPtr("Hatfield Government Center"),
	},

	StopTimeInstances: []*gtfs.StopTimeInstance{
		{
			StopTime: gtfs.StopTime{
				TripId:            "9529801",
				StopSequence:      10,
				ArrivalTime:       32350,
				DepartureTime:     32350,
				ShapeDistTraveled: float64Ptr(0),
				StopId:            "9848",
			},
			FirstStop: true,
		},
		{
			StopTime: gtfs.StopTime{
				TripId:            "9529801",
				StopSequence:      20,
				ArrivalTime:       32455,
				DepartureTime:     32480,
				ShapeDistTraveled: float64Ptr(1830.1),
				StopId:            "9846",
			},
			FirstStop: false,
		},
		{
			StopTime: gtfs.StopTime{
				TripId:            "9529801",
				StopSequence:      30,
				ArrivalTime:       32550,
				DepartureTime:     32570,
				ShapeDistTraveled: float64Ptr(3601.3),
				StopId:            "9843",
			},
			FirstStop: false,
		},
		{
			StopTime: gtfs.StopTime{
				TripId:            "9529801",
				StopSequence:      40,
				ArrivalTime:       32655,
				DepartureTime:     32680,
				ShapeDistTraveled: float64Ptr(5876.7),
				StopId:            "9841",
			},
			FirstStop: false,
		},
	},
}

func TestVehicleMonitor_NewPosition(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	expireSeconds := int64(15 * 60)

	testTrips := getTestTrips(time.Date(2019, 12, 11, 0, 0, 0, 0, location), t)

	trip10856058 := getFirstTestTripFromJson("trip_10856058_2021_07_13.json", t)

	testTrips = append(testTrips, trip10856058)

	if err != nil {
		t.Errorf("Unable to get testing time zone location")
		return
	}
	type args struct {
		Positions []vehiclePosition
	}
	type want struct {
		stopTimes []gtfs.ObservedStopTime
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "Initial position",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(1), StoppedAt,
						time.Date(2019, 12, 11, 8, 59, 25, 0, location).Unix(), "9848"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{},
			},
		},
		{
			name: "Revert to unknown position",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(1), StoppedAt, 1576083565, "9848"),
					{Id: "102", Label: "", Timestamp: 1576083575},
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{},
			},
		},
		{
			name: "Have not moved to next stop",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(1), StoppedAt,
						time.Date(2019, 12, 11, 8, 59, 25, 0, location).Unix(), "9848"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo, 1576083596, "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo,
						time.Date(2019, 12, 11, 9, 0, 27, 0, location).Unix(), "9846"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{},
			},
		},
		{
			name: "Moved to next stop",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(1), StoppedAt, 1576083565, "9848"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo, 1576083596, "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo, 1576083627, "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 0, 58, 0, location).Unix(), "9846"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "100",
						StopId:             "9848",
						ObservedAtStop:     true,
						NextStopId:         "9846",
						ObservedAtNextStop: true,
						ObservedTime:       time.Unix(int64(1576083658), 0),
						TravelSeconds:      int64(1576083658 - 1576083565),
						ScheduledSeconds:   int64Ptr(105),
						VehicleId:          "102",
						TripId:             "9529801",
					},
				},
			},
		},
		{
			name: "Don't update from an older position",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo,
						time.Date(2019, 12, 11, 9, 1, 10, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 1, 17, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 0, 58, 0, location).Unix(), "9846"), //older position
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{},
			},
		},
		{
			name: "Start tracking movement between stops, produce stop time when between another two stops",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(5), InTransitTo, 1576083922, "9838"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(5), InTransitTo, 1576083953, "9838"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(5), InTransitTo, 1576083983, "9838"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(6), InTransitTo, 1576084075, "9839"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(7), InTransitTo,
						time.Date(2019, 12, 11, 9, 9, 27, 0, location).Unix(), "9835"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "100",
						StopId:             "9838",
						ObservedAtStop:     false,
						NextStopId:         "9839",
						ObservedAtNextStop: false,
						ObservedTime:       time.Unix(int64(1576084167), 0),
						TravelSeconds:      int64(1576084167 - 1576084075),
						ScheduledSeconds:   int64Ptr(115),
						VehicleId:          "102",
						TripId:             "9529801",
					},
				},
			},
		},
		{
			name: "Start tracking movement near end of trip, next position at beginning of next trip",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(46), StoppedAt, 1576089931, "8357"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo, 1576089962, "8359"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo, 1576089993, "8359"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo, 1576090024, "8359"),
					makeVehiclePositionStopId("102", "Blue to Hillsboro", "9530573", uint32(1), StoppedAt,
						time.Date(2019, 12, 11, 10, 47, 34, 0, location).Unix(), "8359"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "100",
						StopId:             "8357",
						ObservedAtStop:     true,
						NextStopId:         "8359",
						ObservedAtNextStop: false,
						ObservedTime:       time.Unix(int64(1576090054), 0),
						TravelSeconds:      int64(1576090054 - 1576089931),
						ScheduledSeconds:   int64Ptr(135),
						VehicleId:          "102",
						TripId:             "9529801",
					},
				},
			},
		},
		{
			name: "Start tracking movement near end of trip, next position at second stop of next trip",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(46), StoppedAt,
						time.Date(2019, 12, 11, 10, 45, 31, 0, location).Unix(), "8357"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo,
						time.Date(2019, 12, 11, 10, 46, 2, 0, location).Unix(), "8359"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo,
						time.Date(2019, 12, 11, 10, 46, 33, 0, location).Unix(), "8359"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo,
						time.Date(2019, 12, 11, 10, 47, 4, 0, location).Unix(), "8359"),
					makeVehiclePositionStopId("102", "Blue to Hillsboro", "9530573", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 11, 0, 23, 0, location).Unix(), "8360"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "100",
						StopId:             "8357",
						ObservedAtStop:     true,
						NextStopId:         "8359",
						ObservedAtNextStop: false,
						ObservedTime:       time.Date(2019, 12, 11, 10, 59, 33, 0, location),
						TravelSeconds:      int64(71), //twice scheduled time due to delay
						ScheduledSeconds:   int64Ptr(135),
						VehicleId:          "102",
						TripId:             "9529801",
					},
					{
						RouteId:            "100",
						StopId:             "8359",
						ObservedAtStop:     false,
						NextStopId:         "8360",
						ObservedAtNextStop: true,
						ObservedTime:       time.Date(2019, 12, 11, 11, 0, 23, 0, location),
						TravelSeconds:      int64(50), //twice scheduled time due to delay
						ScheduledSeconds:   int64Ptr(90),
						VehicleId:          "102",
						TripId:             "9530573",
					},
				},
			},
		},
		{
			name: "Second STOPPED_AT position doesn't generate another ObservedStopTime",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(1), StoppedAt, 1576083565, "9848"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo, 1576083596, "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo, 1576083627, "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 0, 58, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 1, 17, 0, location).Unix(), "9846"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{},
			},
		},
		{
			name: "Erroneous trip movement doesn't produce observed stop times",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(46), StoppedAt, 1576089931, "8357"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo, 1576089962, "8359"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo, 1576089993, "8359"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo,
						time.Date(2019, 12, 11, 10, 47, 4, 0, location).Unix(), "8359"),
					makeVehiclePositionStopId("102", "Blue to Hillsboro2", "9530573", uint32(9), StoppedAt,
						time.Date(2019, 12, 11, 10, 50, 0, 0, location).Unix(), "8366"), //too far down the line
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{},
			},
		},
		{
			name: "Do not generate arrivalDelay at stop last stop of trip",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(46), StoppedAt, 1576089941, "8357"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo, 1576089962, "8359"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), StoppedAt,
						time.Date(2019, 12, 11, 10, 47, 4, 0, location).Unix(), "8359"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "100",
						StopId:             "8357",
						ObservedAtStop:     true,
						NextStopId:         "8359",
						ObservedAtNextStop: true,
						ObservedTime:       time.Date(2019, 12, 11, 10, 47, 4, 0, location),
						TravelSeconds:      int64(83),
						ScheduledSeconds:   int64Ptr(135),
						VehicleId:          "102",
						TripId:             "9529801",
					},
				},
			},
		},
		{
			name: "Don't update depart time when at stop in middle of trip",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(46), StoppedAt, 1576089931, "8357"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(46), StoppedAt, 1576089941, "8357"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), InTransitTo, 1576089962, "8359"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(47), StoppedAt,
						time.Date(2019, 12, 11, 10, 47, 4, 0, location).Unix(), "8359"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "100",
						StopId:             "8357",
						ObservedAtStop:     true,
						NextStopId:         "8359",
						ObservedAtNextStop: true,
						ObservedTime:       time.Date(2019, 12, 11, 10, 47, 4, 0, location),
						TravelSeconds:      int64(93),
						ScheduledSeconds:   int64Ptr(135),
						VehicleId:          "102",
						TripId:             "9529801",
					},
				},
			},
		},
		{
			name: "Do update depart time when at stop at beginning of trip",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(1), StoppedAt, 1576083565, "9848"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(1), StoppedAt, 1576083596, "9848"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo, 1576083627, "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt, 1576083658, "9846"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "100",
						StopId:             "9848",
						ObservedAtStop:     true,
						NextStopId:         "9846",
						ObservedAtNextStop: true,
						ObservedTime:       time.Unix(int64(1576083658), 0),
						TravelSeconds:      int64(62),
						ScheduledSeconds:   int64Ptr(105),
						VehicleId:          "102",
						TripId:             "9529801",
					},
				},
			},
		},
		{
			name: "At first stop mark observed stop time as traveling at the scheduled travel time when its arrived on time",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(1), StoppedAt,
						time.Date(2019, 12, 11, 8, 50, 0, 0, location).Unix(), "9848"), //last seen at stop about 11 minutes earlier
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo,
						time.Date(2019, 12, 11, 8, 58, 12, 0, location).Unix(), "9846"), //scheduled depart time is 8:58:10
					makeVehiclePositionStopId("102", "Blue to Gresham2", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 0, 55, 0, location).Unix(), "9846"), //seen at stop at scheduled arrive time
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "100",
						StopId:             "9848",
						ObservedAtStop:     true,
						NextStopId:         "9846",
						ObservedAtNextStop: true,
						ObservedTime:       time.Date(2019, 12, 11, 9, 0, 55, 0, location),
						TravelSeconds:      int64(105),
						ScheduledSeconds:   int64Ptr(105),
						VehicleId:          "102",
						TripId:             "9529801",
					},
				},
			},
		},
		{
			name: "At first stop mark observed stop time as traveling at the nearer the scheduled travel time when its almost on time",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(1), StoppedAt,
						time.Date(2019, 12, 11, 8, 50, 0, 0, location).Unix(), "9848"), //last seen at stop about 11 minutes earlier
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo,
						time.Date(2019, 12, 11, 8, 58, 12, 0, location).Unix(), "9846"), //scheduled depart time is 8:58:10
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 1, 0, 0, location).Unix(), "9846"), //seen at stop 5 seconds after the scheduled time
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "100",
						StopId:             "9848",
						ObservedAtStop:     true,
						NextStopId:         "9846",
						ObservedAtNextStop: true,
						ObservedTime:       time.Date(2019, 12, 11, 9, 1, 0, 0, location),
						TravelSeconds:      int64(110),
						ScheduledSeconds:   int64Ptr(105),
						VehicleId:          "102",
						TripId:             "9529801",
					},
				},
			},
		},
		{
			name: "When traversing two stops from start of trip mark observed stop time as traveling nearer the scheduled travel time when its almost on time",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(1), StoppedAt,
						time.Date(2019, 12, 11, 8, 50, 0, 0, location).Unix(), "9848"), //last seen at stop about 12 minutes earlier
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), InTransitTo,
						time.Date(2019, 12, 11, 8, 58, 12, 0, location).Unix(), "9846"), //scheduled depart time is 8:58:10
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(3), StoppedAt,
						time.Date(2019, 12, 11, 9, 2, 30, 0, location).Unix(), "9843"), //seen at stop at the scheduled time
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "100",
						StopId:             "9848",
						ObservedAtStop:     true,
						NextStopId:         "9846",
						ObservedAtNextStop: false,
						ObservedTime:       time.Date(2019, 12, 11, 9, 0, 55, 0, location),
						TravelSeconds:      int64(105),
						ScheduledSeconds:   int64Ptr(105),
						VehicleId:          "102",
						TripId:             "9529801",
					},
					{
						RouteId:            "100",
						StopId:             "9846",
						ObservedAtStop:     false,
						NextStopId:         "9843",
						ObservedAtNextStop: true,
						ObservedTime:       time.Date(2019, 12, 11, 9, 2, 30, 0, location),
						TravelSeconds:      int64(95),
						ScheduledSeconds:   int64Ptr(95),
						VehicleId:          "102",
						TripId:             "9529801",
					},
				},
			},
		},
		{
			name: "arrivalDelay remains as its getting later",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 8, 50, 0, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 8, 58, 12, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 2, 30, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 3, 30, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 4, 30, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 5, 30, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 6, 30, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 7, 30, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham2", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 8, 30, 0, location).Unix(), "9846"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{},
			},
		},
		{
			name: "arrivalDelay remains after less then the expiration time with just one update",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("102", "Blue to Gresham", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 8, 50, 0, 0, location).Unix(), "9846"),
					makeVehiclePositionStopId("102", "Blue to Gresham2", "9529801", uint32(2), StoppedAt,
						time.Date(2019, 12, 11, 9, 4, 30, 0, location).Unix(), "9846"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{},
			},
		},
		{
			name: "Transitioning from stop 7970 to 8059",
			args: args{
				Positions: []vehiclePosition{
					makeVehiclePositionStopId("3934", "72 Swan Island", "10856058", uint32(13), InTransitTo,
						time.Date(2021, 7, 13, 23, 44, 59, 0, location).Unix(), "7962"),
					makeVehiclePositionStopId("3934", "72 Swan Island", "10856058", uint32(15), InTransitTo,
						time.Date(2021, 7, 13, 23, 45, 29, 0, location).Unix(), "7970"),
					makeVehiclePositionStopId("3934", "72 Swan Island", "10856058", uint32(16), InTransitTo,
						time.Date(2021, 7, 13, 23, 45, 59, 0, location).Unix(), "7960"),
					makeVehiclePositionStopId("3934", "72 Swan Island", "10856058", uint32(18), InTransitTo,
						time.Date(2021, 7, 13, 23, 46, 59, 0, location).Unix(), "8059"),
				},
			},
			want: want{
				stopTimes: []gtfs.ObservedStopTime{
					{
						RouteId:            "72",
						StopId:             "7970",
						ObservedAtStop:     false,
						NextStopId:         "7960",
						ObservedAtNextStop: false,
						ObservedTime:       time.Date(2021, 7, 13, 23, 45, 59, 0, location),
						TravelSeconds:      int64(110),
						ScheduledSeconds:   int64Ptr(30),
						VehicleId:          "3934",
						TripId:             "10856058",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testLog := makeTestLogWriter()

			vm := makeVehicleMonitor(tt.args.Positions[0].Id, .4, expireSeconds)
			var result []gtfs.ObservedStopTime
			//iterate over positions
			for _, lastPosition := range tt.args.Positions {

				trip := getTestTrip(testTrips, lastPosition.TripId, t)
				result = vm.newPosition(testLog.log, &lastPosition, trip)

			}
			same, discrepancyDescription := observedStopTimesSame(result, tt.want.stopTimes)
			if !same {
				t.Errorf("ObservedStopTimes don't match = %v, \n got = \n%v\n, want= \n%v", discrepancyDescription,
					printObservedStopTimesRows(result), printObservedStopTimesRows(tt.want.stopTimes))
			}

		})
	}
}

func observedStopTimesSame(got []gtfs.ObservedStopTime, want []gtfs.ObservedStopTime) (bool, string) {
	if len(got) != len(want) {
		return false, fmt.Sprintf("len(got) = %d != len(*want) %d", len(got), len(want))
	}
	for i, s1 := range got {
		s2 := (want)[i]
		if s1.RouteId != s2.RouteId {
			return false, fmt.Sprintf("row %v, routeId %v != %v", i, s1.RouteId, s2.RouteId)
		}
		if s1.StopId != s2.StopId {
			return false, fmt.Sprintf("row %v, stopId %v != %v", i, s1.StopId, s2.StopId)
		}
		if s1.ObservedAtStop != s2.ObservedAtStop {
			return false, fmt.Sprintf("row %v, ObservedAtStop %v != %v", i, s1.ObservedAtStop, s2.ObservedAtStop)
		}
		if s1.NextStopId != s2.NextStopId {
			return false, fmt.Sprintf("row %v, NextStopId %v != %v", i, s1.NextStopId, s2.NextStopId)
		}
		if s1.ObservedAtNextStop != s2.ObservedAtNextStop {
			return false, fmt.Sprintf("row %v, ObservedAtNextStop %v != %v", i, s1.ObservedAtNextStop, s2.ObservedAtNextStop)
		}
		if !s1.ObservedTime.Equal(s2.ObservedTime) {
			return false, fmt.Sprintf("row %v, ObservedTime %v != %v", i, s1.ObservedTime, s2.ObservedTime)
		}
		if s1.TravelSeconds != s2.TravelSeconds {
			return false, fmt.Sprintf("row %v, TravelSeconds %v != %v", i, s1.TravelSeconds, s2.TravelSeconds)
		}
		if s1.VehicleId != s2.VehicleId {
			return false, fmt.Sprintf("row %v, vehicleId %v != %v", i, s1.VehicleId, s2.VehicleId)
		}
		if s1.TripId != s2.TripId {
			return false, fmt.Sprintf("row %v, TripId %v != %v", i, s1.TripId, s2.TripId)
		}
		if !reflect.DeepEqual(s1.ScheduledSeconds, s2.ScheduledSeconds) {
			return false, fmt.Sprintf("row %v, ScheduledSeconds %v != %v", i, s1.ScheduledSeconds, s2.ScheduledSeconds)
		}

	}
	return true, ""
}

//printObservedStopTimesRows format errors in a way that is easy to scan
func printObservedStopTimesRows(stopTimes []gtfs.ObservedStopTime) string {
	if stopTimes == nil {
		return "nil"
	}
	all := make([]string, 0)

	for i, st := range stopTimes {
		scheduleSeconds := "<nil>"
		if st.ScheduledSeconds != nil {
			scheduleSeconds = fmt.Sprintf("%d", *st.ScheduledSeconds)
		}
		row := fmt.Sprintf("row:%d ObservedTime:%s, RouteId:%v StopId:%v ObservedAtStop:%v NextStopId:%v "+
			"ObservedAtNextStop:%v TravelSeconds:%d ScheduledSeconds:%s VehicleId:%s, TripId:%s",
			i,
			st.ObservedTime.Format("2006-01-02 15:04:05"),
			st.RouteId,
			st.StopId,
			st.ObservedAtStop,
			st.NextStopId,
			st.ObservedAtNextStop,
			st.TravelSeconds,
			scheduleSeconds,
			st.VehicleId,
			st.TripId)
		all = append(all, row)

	}
	return strings.Join(all, "\n")
}

func Test_shouldUseToMoveForward(t *testing.T) {

	//only need the tripId populated in tripInstance for shouldUseToMoveForward
	testTripOne := &gtfs.TripInstance{
		Trip: gtfs.Trip{
			TripId: "1",
		},
	}

	testTripTwo := &gtfs.TripInstance{
		Trip: gtfs.Trip{
			TripId: "2",
		},
	}

	type args struct {
		previousTripStopPosition *tripStopPosition
		newPosition              *tripStopPosition
	}

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Update when moved passed stop",
			args: args{
				previousTripStopPosition: &tripStopPosition{
					stopId:                "2",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
				newPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          3,
					nextStopSequence:      4,
				},
			},
			want: true,
		},
		{
			name: "Update when arrived at stop",
			args: args{
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
				newPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            true,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
			},
			want: true,
		},
		{
			name: "Update when arrived at stop",
			args: args{
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
				newPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            true,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
			},
			want: true,
		},
		{
			name: "Don't update when at stop and new position between previous stop and next stop",
			args: args{
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            true,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
				newPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: true,
					tripInstance:          testTripOne,
					stopSequence:          3,
					nextStopSequence:      4,
				},
			},
			want: false,
		},
		{
			name: "Do update when at stop and new position at the next stop",
			args: args{
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            true,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
				newPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            true,
					witnessedPreviousStop: true,
					tripInstance:          testTripOne,
					stopSequence:          3,
					nextStopSequence:      4,
				},
			},
			want: true,
		},
		{
			name: "Don't update when at stop and new position at the same stop",
			args: args{
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            true,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
				newPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            true,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
			},
			want: false,
		},
		{
			name: "Dont update when two positions between same stops",
			args: args{
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
				newPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
			},
			want: false,
		},
		{
			name: "Do update when moving between stops",
			args: args{
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
				newPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          3,
					nextStopSequence:      4,
				},
			},
			want: true,
		},
		{
			name: "Do update when different trip",
			args: args{
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          12,
					nextStopSequence:      33,
				},
				newPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripTwo,
					stopSequence:          1,
					nextStopSequence:      2,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldUseToMoveForward(tt.args.previousTripStopPosition, tt.args.newPosition); got != tt.want {
				t.Errorf("shouldUseToMoveForward() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getStopTransition(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to load \"America/Los_Angeles\" timezone: %v", err)
	}
	testTrips := getTestTrips(time.Date(2019, 12, 11, 16, 0, 0, 0, location), t)
	testTrip := getTestTrip(testTrips, strPtr("9529801"), t)

	type args struct {
		trip                     *gtfs.TripInstance
		previousTripStopPosition *tripStopPosition
		stopSequence             uint32
		status                   VehicleStopStatus
	}
	tests := []struct {
		name string
		args args
		want *tripStopPosition
	}{
		{
			name: "at first stop of trip",
			args: args{
				trip:                     testTrips[0],
				previousTripStopPosition: nil,
				stopSequence:             1,
				status:                   StoppedAt,
			},
			want: &tripStopPosition{
				stopId:                "9848",
				seenAtStop:            true,
				witnessedPreviousStop: true,
				tripInstance:          testTrip,
				stopSequence:          1,
				nextStopSequence:      2,
				isFirstStop:           true,
			},
		},
		{
			name: "previously at same first stop of trip",
			args: args{
				trip: testTrips[0],
				previousTripStopPosition: &tripStopPosition{
					stopId:                "9848",
					seenAtStop:            true,
					witnessedPreviousStop: true,
					tripInstance:          testTrip,
					stopSequence:          1,
					nextStopSequence:      2,
				},
				stopSequence: 1,
				status:       StoppedAt,
			},
			want: &tripStopPosition{
				stopId:                "9848",
				seenAtStop:            true,
				witnessedPreviousStop: true,
				tripInstance:          testTrip,
				stopSequence:          1,
				nextStopSequence:      2,
				isFirstStop:           true,
			},
		},
		{
			name: "Moved from being at first stop",
			args: args{
				trip: testTrips[0],
				previousTripStopPosition: &tripStopPosition{
					stopId:                "9848",
					seenAtStop:            true,
					witnessedPreviousStop: true,
					tripInstance:          testTrip,
					stopSequence:          1,
					nextStopSequence:      2,
				},
				stopSequence: 2,
				status:       InTransitTo,
			},
			want: &tripStopPosition{
				stopId:                "9846",
				seenAtStop:            false,
				witnessedPreviousStop: true,
				tripInstance:          testTrip,
				stopSequence:          2,
				nextStopSequence:      3,
			},
		},
		{
			name: "Seen at second stop of trip, no previous position",
			args: args{
				trip:                     testTrips[0],
				previousTripStopPosition: nil,
				stopSequence:             2,
				status:                   StoppedAt,
			},
			want: &tripStopPosition{
				stopId:                "9846",
				seenAtStop:            true,
				witnessedPreviousStop: true,
				tripInstance:          testTrip,
				stopSequence:          2,
				nextStopSequence:      3,
			},
		},
		{
			name: "Seen between stops no previous position",
			args: args{
				trip:                     testTrips[0],
				previousTripStopPosition: nil,
				stopSequence:             2,
				status:                   InTransitTo,
			},
			want: &tripStopPosition{
				stopId:                "9846",
				seenAtStop:            false,
				witnessedPreviousStop: false,
				tripInstance:          testTrip,
				stopSequence:          2,
				nextStopSequence:      3,
			},
		},
		{
			name: "Between stop 3 and 4, see between stop 2 and 3",
			args: args{
				trip: testTrips[0],
				previousTripStopPosition: &tripStopPosition{
					stopId:                "9846",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTrip,
					stopSequence:          2,
					nextStopSequence:      3,
				},
				stopSequence: 4,
				status:       InTransitTo,
			},
			want: &tripStopPosition{
				stopId:                "9841",
				seenAtStop:            false,
				witnessedPreviousStop: true,
				tripInstance:          testTrip,
				stopSequence:          4,
				nextStopSequence:      5,
			},
		},
		{
			name: "Seen at last stop of trip, no previous position",
			args: args{
				trip:                     testTrips[0],
				previousTripStopPosition: nil,
				stopSequence:             47,
				status:                   StoppedAt,
			},
			want: &tripStopPosition{
				stopId:                "8359",
				seenAtStop:            true,
				witnessedPreviousStop: true,
				tripInstance:          testTrip,
				stopSequence:          47,
				nextStopSequence:      47, //same stop sequence because this is the last stop
			},
		},
		{
			name: "Seen at last stop of trip, previous position between previous stop",
			args: args{
				trip: testTrips[0],
				previousTripStopPosition: &tripStopPosition{
					stopId:                "8357",
					seenAtStop:            false,
					witnessedPreviousStop: true,
					tripInstance:          testTrip,
					stopSequence:          46,
					nextStopSequence:      47,
				},
				stopSequence: 47,
				status:       StoppedAt,
			},
			want: &tripStopPosition{
				stopId:                "8359",
				seenAtStop:            true,
				witnessedPreviousStop: true,
				tripInstance:          testTrip,
				stopSequence:          47,
				nextStopSequence:      47, //same stop sequence because this is the last stop
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			position := vehiclePosition{
				VehicleStopStatus: tt.args.status,
				StopSequence:      &tt.args.stopSequence,
			}
			got, _ := getTripStopPosition(tt.args.trip, tt.args.previousTripStopPosition, &position)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getTripStopPosition() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func Test_witnessedPreviousStop(t *testing.T) {

	//only need the tripId populated in tripInstance for witnessedPreviousStop
	testTripOne := &gtfs.TripInstance{
		Trip: gtfs.Trip{
			TripId: "1",
		},
	}

	type args struct {
		tripId                   string
		stopSequence             uint32
		previousTripStopPosition *tripStopPosition
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nil previous stop position",
			args: args{
				tripId:                   "1",
				stopSequence:             3,
				previousTripStopPosition: nil,
			},
			want: false,
		},
		{
			name: "previous stop position same stop",
			args: args{
				tripId:       "1",
				stopSequence: 3,
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          3,
					nextStopSequence:      4,
				},
			},
			want: false,
		},
		{
			name: "previously at earlier stop",
			args: args{
				tripId:       "1",
				stopSequence: 3,
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          2,
					nextStopSequence:      3,
				},
			},
			want: true,
		},
		{
			name: "previously at later stop",
			args: args{
				tripId:       "1",
				stopSequence: 3,
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            true,
					witnessedPreviousStop: true,
					tripInstance:          testTripOne,
					stopSequence:          4,
					nextStopSequence:      5,
				},
			},
			want: false,
		},
		{
			name: "previously at same stop",
			args: args{
				tripId:       "1",
				stopSequence: 3,
				previousTripStopPosition: &tripStopPosition{
					stopId:                "1",
					seenAtStop:            true,
					witnessedPreviousStop: false,
					tripInstance:          testTripOne,
					stopSequence:          3,
					nextStopSequence:      4,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := witnessedPreviousStop(tt.args.tripId, tt.args.stopSequence, tt.args.previousTripStopPosition); got != tt.want {
				t.Errorf("witnessedPreviousStop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getStopPairsBetweenPositions(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to load \"America/Los_Angeles\" timezone: %v", err)
	}
	testTrips := getTestTrips(time.Date(2019, 12, 11, 16, 0, 0, 0, location), t)

	firstTrip := getTestTrip(testTrips, strPtr("9529801"), t)
	secondTrip := getTestTrip(testTrips, strPtr("9530573"), t)
	trip10856058 := getFirstTestTripFromJson("trip_10856058_2021_07_13.json", t)

	type args struct {
		lastPosition    *tripStopPosition
		currentPosition *tripStopPosition
	}
	tests := []struct {
		name    string
		args    args
		want    []StopTimePair
		wantErr bool
	}{
		{
			name: "Still at first stop",
			args: args{
				lastPosition: &tripStopPosition{
					seenAtStop:            true,
					witnessedPreviousStop: false,
					tripInstance:          firstTrip,
					stopSequence:          1,
					nextStopSequence:      2,
				},
				currentPosition: &tripStopPosition{
					seenAtStop:            true,
					witnessedPreviousStop: false,
					tripInstance:          firstTrip,
					stopSequence:          1,
					nextStopSequence:      2,
				},
			},
			want:    []StopTimePair{},
			wantErr: false,
		},
		{
			name: "At first stop then at second stop",
			args: args{
				lastPosition: &tripStopPosition{
					seenAtStop:            true,
					witnessedPreviousStop: true,
					tripInstance:          firstTrip,
					stopSequence:          1,
					nextStopSequence:      2,
				},
				currentPosition: &tripStopPosition{
					seenAtStop:            true,
					witnessedPreviousStop: true,
					tripInstance:          firstTrip,
					stopSequence:          2,
					nextStopSequence:      3,
				},
			},
			want: []StopTimePair{
				{
					*testTrips[0].StopTimeInstances[0],
					*testTrips[0].StopTimeInstances[1],
					testTrips[0],
				},
			},
			wantErr: false,
		},
		{
			name: "At first stop, then between second and third stop",
			args: args{
				lastPosition: &tripStopPosition{
					seenAtStop:            true,
					witnessedPreviousStop: true,
					tripInstance:          firstTrip,
					stopSequence:          1,
					nextStopSequence:      2,
				},
				currentPosition: &tripStopPosition{
					seenAtStop:            false,
					witnessedPreviousStop: true,
					tripInstance:          firstTrip,
					stopSequence:          3,
					nextStopSequence:      4,
				},
			},
			want: []StopTimePair{
				{
					*testTrips[0].StopTimeInstances[0],
					*testTrips[0].StopTimeInstances[1],
					testTrips[0],
				},
			},
			wantErr: false,
		},
		{
			name: "between first and second (without being seen at stop), then between second and third stop",
			args: args{
				lastPosition: &tripStopPosition{
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          firstTrip,
					stopSequence:          1,
					nextStopSequence:      2,
				},
				currentPosition: &tripStopPosition{
					seenAtStop:            false,
					witnessedPreviousStop: true,
					tripInstance:          firstTrip,
					stopSequence:          3,
					nextStopSequence:      4,
				},
			},
			want: []StopTimePair{
				{
					*testTrips[0].StopTimeInstances[0],
					*testTrips[0].StopTimeInstances[1],
					testTrips[0],
				},
			},
			wantErr: false,
		},
		{
			name: "Near end of first trip, into second trip",
			args: args{
				lastPosition: &tripStopPosition{
					seenAtStop:            false,
					witnessedPreviousStop: true,
					tripInstance:          firstTrip,
					stopSequence:          45,
					nextStopSequence:      46,
				},
				currentPosition: &tripStopPosition{
					seenAtStop:            false,
					witnessedPreviousStop: false,
					tripInstance:          secondTrip,
					stopSequence:          3,
					nextStopSequence:      4,
				},
			},
			want: []StopTimePair{
				{
					*testTrips[0].StopTimeInstances[44],
					*testTrips[0].StopTimeInstances[45],
					testTrips[0],
				},
				{
					*testTrips[0].StopTimeInstances[45],
					*testTrips[0].StopTimeInstances[46],
					testTrips[0],
				},
				{
					*testTrips[1].StopTimeInstances[0],
					*testTrips[1].StopTimeInstances[1],
					testTrips[1],
				},
			},
			wantErr: false,
		},
		{
			name: "Witnessed at previous stop now two stops beyond it",
			args: args{
				lastPosition: &tripStopPosition{
					seenAtStop:            false,
					witnessedPreviousStop: true,
					tripInstance:          firstTrip,
					stopSequence:          6,
					nextStopSequence:      7,
				},
				currentPosition: &tripStopPosition{
					seenAtStop:            false,
					witnessedPreviousStop: true,
					tripInstance:          firstTrip,
					stopSequence:          9,
					nextStopSequence:      10,
				},
			},
			want: []StopTimePair{
				{
					*testTrips[0].StopTimeInstances[5],
					*testTrips[0].StopTimeInstances[6],
					testTrips[0],
				},
				{
					*testTrips[0].StopTimeInstances[6],
					*testTrips[0].StopTimeInstances[7],
					testTrips[0],
				},
			},
			wantErr: false,
		},
		{
			name: "At second to last stop, then at first stop of next trip",
			args: args{
				lastPosition: &tripStopPosition{
					seenAtStop:            true,
					witnessedPreviousStop: true,
					tripInstance:          firstTrip,
					stopSequence:          46,
					nextStopSequence:      47,
				},
				currentPosition: &tripStopPosition{
					seenAtStop:            true,
					witnessedPreviousStop: true,
					tripInstance:          secondTrip,
					stopSequence:          1,
					nextStopSequence:      2,
				},
			},
			want: []StopTimePair{
				{
					*testTrips[0].StopTimeInstances[45],
					*testTrips[0].StopTimeInstances[46],
					testTrips[0],
				},
			},
			wantErr: false,
		},
		{
			name: "Seen before previous stop, now moved past next stop",
			args: args{
				lastPosition: &tripStopPosition{
					seenAtStop:            false,
					witnessedPreviousStop: true,
					tripInstance:          trip10856058,
					stopSequence:          15,
					nextStopSequence:      16,
				},
				currentPosition: &tripStopPosition{
					seenAtStop:            false,
					witnessedPreviousStop: true,
					tripInstance:          trip10856058,
					stopSequence:          16,
					nextStopSequence:      17,
				},
			},
			want: []StopTimePair{
				{
					*trip10856058.StopTimeInstances[15],
					*trip10856058.StopTimeInstances[16],
					trip10856058,
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getStopPairsBetweenPositions(tt.args.lastPosition, tt.args.currentPosition)
			if (err != nil) != tt.wantErr {
				t.Errorf("getStopPairsBetweenPositions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getStopPairsBetweenPositions() got = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func Test_getStopPairsBetweenSequences(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to load \"America/Los_Angeles\" timezone: %v", err)
	}
	testTrips := getTestTrips(time.Date(2019, 12, 11, 16, 0, 0, 0, location), t)
	type args struct {
		trip             *gtfs.TripInstance
		fromStopSequence uint32
		toStopSequence   uint32
	}
	tests := []struct {
		name string
		args args
		want []StopTimePair
	}{
		{
			name: "single stop time pair",
			args: args{
				trip:             testTrips[0],
				fromStopSequence: 1,
				toStopSequence:   2,
			},
			want: []StopTimePair{
				{
					*testTrips[0].StopTimeInstances[0],
					*testTrips[0].StopTimeInstances[1],
					testTrips[0],
				},
			},
		},
		{
			name: "no stop time pairs",
			args: args{
				trip:             testTrips[0],
				fromStopSequence: 1,
				toStopSequence:   1,
			},
			want: []StopTimePair{},
		},
		{
			name: "3 stop time pairs",
			args: args{
				trip:             testTrips[0],
				fromStopSequence: 4,
				toStopSequence:   7,
			},
			want: []StopTimePair{
				{
					*testTrips[0].StopTimeInstances[3],
					*testTrips[0].StopTimeInstances[4],
					testTrips[0],
				},
				{
					*testTrips[0].StopTimeInstances[4],
					*testTrips[0].StopTimeInstances[5],
					testTrips[0],
				},
				{
					*testTrips[0].StopTimeInstances[5],
					*testTrips[0].StopTimeInstances[6],
					testTrips[0],
				},
			},
		},
		{
			name: "5 stop times",
			args: args{
				trip:             testTrips[0],
				fromStopSequence: 4,
				toStopSequence:   8,
			},
			want: []StopTimePair{
				{
					*testTrips[0].StopTimeInstances[3],
					*testTrips[0].StopTimeInstances[4],
					testTrips[0],
				},
				{
					*testTrips[0].StopTimeInstances[4],
					*testTrips[0].StopTimeInstances[5],
					testTrips[0],
				},
				{
					*testTrips[0].StopTimeInstances[5],
					*testTrips[0].StopTimeInstances[6],
					testTrips[0],
				},
				{
					*testTrips[0].StopTimeInstances[6],
					*testTrips[0].StopTimeInstances[7],
					testTrips[0],
				},
			},
		},
		{
			name: "Stops where to sequence is between stop sequences",
			args: args{
				trip:             spacedStopSequenceTrip,
				fromStopSequence: 10,
				toStopSequence:   29,
			},
			want: []StopTimePair{
				{
					*spacedStopSequenceTrip.StopTimeInstances[0],
					*spacedStopSequenceTrip.StopTimeInstances[1],
					spacedStopSequenceTrip,
				},
			},
		},
		{
			name: "Stops where from sequence is between stop sequences",
			args: args{
				trip:             spacedStopSequenceTrip,
				fromStopSequence: 15,
				toStopSequence:   30,
			},
			want: []StopTimePair{
				{
					*spacedStopSequenceTrip.StopTimeInstances[1],
					*spacedStopSequenceTrip.StopTimeInstances[2],
					spacedStopSequenceTrip,
				},
			},
		},
		{
			name: "Stops where from and to sequences are between stop sequences",
			args: args{
				trip:             spacedStopSequenceTrip,
				fromStopSequence: 15,
				toStopSequence:   49,
			},
			want: []StopTimePair{
				{
					*spacedStopSequenceTrip.StopTimeInstances[1],
					*spacedStopSequenceTrip.StopTimeInstances[2],
					spacedStopSequenceTrip,
				},
				{
					*spacedStopSequenceTrip.StopTimeInstances[2],
					*spacedStopSequenceTrip.StopTimeInstances[3],
					spacedStopSequenceTrip,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getStopPairsBetweenSequences(tt.args.trip, tt.args.fromStopSequence, tt.args.toStopSequence)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getStopPairsBetweenSequences() got = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func Test_TestVehicleMonitor_NewPositionGetsEveryStopPairOnce(t *testing.T) {
	testPositions := getTestVehiclePositions(t)

	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to load \"America/Los_Angeles\" timezone: %v", err)
	}
	testTrips := getTestTrips(time.Date(2019, 12, 11, 16, 0, 0, 0, location), t)

	vm := makeVehicleMonitor("102", .2, 15*60)
	t.Run("newPosition produces every stop pair once", func(t *testing.T) {

		testLog := makeTestLogWriter()

		transitionMap := make(map[string]gtfs.ObservedStopTime)
		//iterate over positions
		for i, lastPosition := range testPositions {

			trip := getTestTrip(testTrips, lastPosition.TripId, t)

			results := vm.newPosition(testLog.log, &lastPosition, trip)
			if results == nil {
				continue
			}
			for _, observedStopTime := range results {
				key := fmt.Sprint(observedStopTime.TripId, ":", observedStopTime.StopId, "-", observedStopTime.NextStopId)
				if duplicate, present := transitionMap[key]; present {
					t.Errorf("found duplicate stop transition: %v after position %v, previous value %+v, new value %+v",
						key, i, duplicate, observedStopTime)
				} else {
					transitionMap[key] = observedStopTime
				}
			}
		}
		//iterate over trips and ensure every stop pair is present
		for _, trip := range testTrips {
			stopTimeInstances := trip.StopTimeInstances
			numberStopTimeInstances := len(stopTimeInstances)
			for i := 0; i+1 < numberStopTimeInstances; i++ {
				s1 := stopTimeInstances[i]
				s2 := stopTimeInstances[i+1]
				key := fmt.Sprint(trip.TripId, ":", s1.StopId, "-", s2.StopId)
				if _, present := transitionMap[key]; !present {
					t.Errorf("Missing ObservedStopTime transition: %v, from stop %+v, to %+v", key, s1, s2)
				}

			}
		}

	})
}

func getTestVehiclePositions(t *testing.T) []vehiclePosition {
	file, err := ioutil.ReadFile("testdata/vehicle_102_vehicle_positions.json")
	if err != nil {
		t.Errorf("unable to read test file: %v", err)
	}
	vehiclePositions := make([]vehiclePosition, 0)
	err = json.Unmarshal(file, &vehiclePositions)
	if err != nil {
		t.Errorf("unable to read test vehiclePositions file: %v", err)
	}
	return vehiclePositions
}

func Test_getSegmentTravelPostulate(t *testing.T) {
	type args struct {
		totalTravelSeconds    int64
		totalScheduledLength  int64
		segmentScheduleLength int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "50 percent",
			args: args{
				totalTravelSeconds:    200,
				totalScheduledLength:  100,
				segmentScheduleLength: 50,
			},
			want: 100,
		},
		{
			name: "20 percent",
			args: args{
				totalTravelSeconds:    200,
				totalScheduledLength:  100,
				segmentScheduleLength: 20,
			},
			want: 40,
		},
		{
			name: "0 percent",
			args: args{
				totalTravelSeconds:    0,
				totalScheduledLength:  100,
				segmentScheduleLength: 10,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getSegmentTravelPortion(tt.args.totalTravelSeconds, tt.args.totalScheduledLength, tt.args.segmentScheduleLength); got != tt.want {
				t.Errorf("getSegmentTravelPortion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isMovementBelievable(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Errorf("Unable to load \"America/Los_Angeles\" timezone: %v", err)
	}
	type args struct {
		stopTimePairs  []StopTimePair
		fromTimestamp  int64
		toTimestamp    int64
		earlyTolerance float64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "no movement",
			args: args{
				stopTimePairs:  []StopTimePair{},
				fromTimestamp:  0,
				toTimestamp:    0,
				earlyTolerance: 0.3,
			},
			want: true,
		},
		{
			name: "backward movement is invalid",
			args: args{
				stopTimePairs: []StopTimePair{
					{
						from: gtfs.StopTimeInstance{
							ArrivalDateTime:   time.Date(2020, 1, 12, 12, 0, 0, 0, location),
							DepartureDateTime: time.Date(2020, 1, 12, 12, 0, 0, 0, location),
						},
						to: gtfs.StopTimeInstance{
							ArrivalDateTime:   time.Date(2020, 1, 12, 11, 0, 0, 0, location),
							DepartureDateTime: time.Date(2020, 1, 12, 11, 0, 0, 0, location),
						},
						trip: nil,
					},
				},
				fromTimestamp:  0,
				toTimestamp:    0,
				earlyTolerance: 0.3,
			},
			want: false,
		},
		{
			name: "zero travel time is invalid",
			args: args{
				stopTimePairs: []StopTimePair{
					{
						from: gtfs.StopTimeInstance{
							ArrivalDateTime:   time.Date(2020, 1, 12, 12, 0, 0, 0, location),
							DepartureDateTime: time.Date(2020, 1, 12, 12, 0, 0, 0, location),
						},
						to: gtfs.StopTimeInstance{
							ArrivalDateTime:   time.Date(2020, 1, 12, 12, 1, 0, 0, location),
							DepartureDateTime: time.Date(2020, 1, 12, 12, 1, 0, 0, location),
						},
						trip: nil,
					},
				},
				fromTimestamp:  time.Date(2020, 1, 12, 12, 1, 0, 0, location).Unix(),
				toTimestamp:    time.Date(2020, 1, 12, 12, 1, 0, 0, location).Unix(),
				earlyTolerance: 0.3,
			},
			want: false,
		},
		{
			name: "30 percent travel time is invalid when set at 0.4",
			args: args{
				stopTimePairs: []StopTimePair{
					{
						from: gtfs.StopTimeInstance{
							ArrivalDateTime:   time.Date(2020, 1, 12, 12, 0, 0, 0, location),
							DepartureDateTime: time.Date(2020, 1, 12, 12, 0, 0, 0, location),
						},
						to: gtfs.StopTimeInstance{
							ArrivalDateTime:   time.Date(2020, 1, 12, 12, 1, 40, 0, location),
							DepartureDateTime: time.Date(2020, 1, 12, 12, 1, 40, 0, location),
						},
						trip: nil,
					},
				},
				fromTimestamp:  time.Date(2020, 1, 12, 12, 0, 0, 0, location).Unix(),
				toTimestamp:    time.Date(2020, 1, 12, 12, 0, 30, 0, location).Unix(),
				earlyTolerance: 0.4,
			},
			want: false,
		},
		{
			name: "normal travel time is valid",
			args: args{
				stopTimePairs: []StopTimePair{
					{
						from: gtfs.StopTimeInstance{
							ArrivalDateTime:   time.Date(2020, 1, 12, 12, 0, 0, 0, location),
							DepartureDateTime: time.Date(2020, 1, 12, 12, 0, 0, 0, location),
						},
						to: gtfs.StopTimeInstance{
							ArrivalDateTime:   time.Date(2020, 1, 12, 12, 1, 40, 0, location),
							DepartureDateTime: time.Date(2020, 1, 12, 12, 1, 40, 0, location),
						},
						trip: nil,
					},
				},
				fromTimestamp:  time.Date(2020, 1, 12, 12, 0, 0, 0, location).Unix(),
				toTimestamp:    time.Date(2020, 1, 12, 12, 1, 40, 0, location).Unix(),
				earlyTolerance: 0.3,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, _ := isMovementBelievable(tt.args.stopTimePairs, tt.args.fromTimestamp, tt.args.toTimestamp, tt.args.earlyTolerance)
			if got != tt.want {
				t.Errorf("isMovementBelievable() = %v, want %v", got, tt.want)
			}
		})
	}
}
