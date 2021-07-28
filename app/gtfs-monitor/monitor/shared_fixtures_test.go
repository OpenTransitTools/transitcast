package monitor

import (
	"encoding/json"
	"gitlab.trimet.org/transittracker/transitmon/business/data/gtfs"
	"io/ioutil"
	"log"
	"path/filepath"
	"testing"
	"time"
)

type testLogWriter struct {
	logLines []string
	log      *log.Logger
}

func makeTestLogWriter() *testLogWriter {
	logWriter := testLogWriter{
		logLines: make([]string, 0),
	}
	logger := log.New(&logWriter, "GTFS_MONITOR : ", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	logWriter.log = logger
	return &logWriter
}

func intPtr(i int) *int {
	return &i
}

func strPtr(s string) *string {
	return &s
}
func float32Ptr(f float32) *float32 {
	return &f
}

func float64Ptr(f float64) *float64 {
	return &f
}

func (t *testLogWriter) Write(p []byte) (n int, err error) {
	t.logLines = append(t.logLines, string(p))
	return len(p), nil
}

func getTestTrip(trips []*gtfs.TripInstance, tripId *string, t *testing.T) *gtfs.TripInstance {
	if tripId == nil {
		return nil
	}
	for _, trip := range trips {
		if trip.TripId == *tripId {
			return trip
		}
	}
	t.Errorf("unable to find test tripId %s", *tripId)
	return nil
}

func getTestTrips(serviceDate time.Time, t *testing.T) []*gtfs.TripInstance {
	var result []*gtfs.TripInstance
	file, err := ioutil.ReadFile("testdata/test_trips.json")
	if err != nil {
		t.Errorf("unable to read test trips file: %v", err)
	}
	err = json.Unmarshal(file, &result)
	if err != nil {
		t.Errorf("unable to read test trips file: %v", err)
	}
	for _, trip := range result {
		for _, s := range trip.StopTimeInstances {
			s.ArrivalDateTime = gtfs.MakeScheduleTime(serviceDate, s.ArrivalTime)
			s.DepartureDateTime = gtfs.MakeScheduleTime(serviceDate, s.DepartureTime)
		}
	}
	return result
}

func getTestTripsFromJson(fileName string, t *testing.T) []*gtfs.TripInstance {
	var result []*gtfs.TripInstance
	file, err := ioutil.ReadFile(filepath.Join("testdata", fileName))
	if err != nil {
		t.Errorf("unable to read test trips file: %v", err)
	}
	err = json.Unmarshal(file, &result)
	if err != nil {
		t.Errorf("unable to read test trips file: %v", err)
	}
	return result
}

func getFirstTestTripFromJson(fileName string, t *testing.T) *gtfs.TripInstance {
	trips := getTestTripsFromJson(fileName, t)
	if len(trips) < 1 {
		t.Errorf("failed to load test trip from file %s", fileName)
		return nil
	}
	return trips[0]
}

func testDate(dateString string) time.Time {
	t, _ := time.Parse("2006-01-02T15:04:05-07:00", dateString)
	return t
}
