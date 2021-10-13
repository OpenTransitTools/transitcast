package gtfsmanager

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
)

const batchedStopTimeCount = 250

//tripStartEnds stores start times, end times and maximum distances for a trip for later use while loading trips
type tripStartEnds struct {
	startTime    int
	endTime      int
	tripDistance float64
}

// stopTimeRowReader implements gtfsRowReader interface for gtfs.StopTime
// batches inserts
type stopTimeRowReader struct {
	batchedStopTimes []*gtfs.StopTime
	tripStartEndMap  map[string]*tripStartEnds
}

func newStopTimeRowReader() *stopTimeRowReader {
	return &stopTimeRowReader{
		tripStartEndMap: make(map[string]*tripStartEnds),
	}
}

func (s *stopTimeRowReader) addRow(parser *gtfsFileParser, dsTx *gtfs.DataSetTransaction) error {
	stopTime, err := buildStopTime(parser)
	if err != nil {
		return err
	}
	s.batchedStopTimes = append(s.batchedStopTimes, stopTime)
	s.addEndStartTime(stopTime)

	//check if it's time to save the batch
	if len(s.batchedStopTimes) == batchedStopTimeCount {
		return s.flush(dsTx)
	}
	return nil
}

// addEndStartTime updates tripStartEnds with gtfs.StopTime for later use
func (s *stopTimeRowReader) addEndStartTime(stopTime *gtfs.StopTime) {
	trip := s.tripStartEndMap[stopTime.TripId]
	if trip == nil {
		trip = &tripStartEnds{
			startTime:    stopTime.ArrivalTime,
			endTime:      stopTime.DepartureTime,
			tripDistance: stopTime.ShapeDistTraveled,
		}
		s.tripStartEndMap[stopTime.TripId] = trip
		return
	}
	if stopTime.ArrivalTime < trip.startTime {
		trip.startTime = stopTime.ArrivalTime
	}
	if stopTime.DepartureTime > trip.startTime {
		trip.endTime = stopTime.DepartureTime
	}
	if trip.tripDistance < stopTime.ShapeDistTraveled {
		trip.tripDistance = stopTime.ShapeDistTraveled
	}

}

func (s *stopTimeRowReader) flush(dsTx *gtfs.DataSetTransaction) error {
	//check if there's something to do
	if len(s.batchedStopTimes) == 0 {

		return nil
	}

	err := gtfs.RecordStopTimes(s.batchedStopTimes, dsTx)
	if err != nil {
		return err
	}

	// truncate the batch
	s.batchedStopTimes = make([]*gtfs.StopTime, 0)
	return nil
}

func buildStopTime(parser *gtfsFileParser) (*gtfs.StopTime, error) {
	stopTime := gtfs.StopTime{}
	stopTime.TripId = parser.getString("trip_id", false)
	stopTime.StopId = parser.getString("stop_id", false)
	stopTime.StopSequence = uint32(parser.getInt("stop_sequence", false))
	stopTime.ArrivalTime = parser.getGTFSTime("arrival_time", false)
	stopTime.DepartureTime = parser.getGTFSTime("departure_time", false)
	stopTime.ShapeDistTraveled = parser.getFloat64("shape_dist_traveled", false)
	stopTime.Timepoint = parser.getInt("timepoint", true)
	return &stopTime, parser.getError()
}
