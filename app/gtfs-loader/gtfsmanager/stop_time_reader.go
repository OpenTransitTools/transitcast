package gtfsmanager

import (
	"gitlab.trimet.org/transittracker/transitmon/business/data/gtfs"
)

const batchedStopTimeCount = 250

// stopTimeRowReader implements gtfsRowReader interface for gtfs.StopTime
// batches inserts
type stopTimeRowReader struct {
	batchedStopTimes []*gtfs.StopTime
}

func (s *stopTimeRowReader) addRow(parser *gtfsFileParser, dsTx *gtfs.DataSetTransaction) error {
	stopTime, err := buildStopTime(parser)
	if err != nil {
		return err
	}
	s.batchedStopTimes = append(s.batchedStopTimes, stopTime)

	//check if its time to save the batch
	if len(s.batchedStopTimes) == batchedStopTimeCount {
		return s.flush(dsTx)
	}
	return nil
}

func (s *stopTimeRowReader) flush(dsTx *gtfs.DataSetTransaction) error {
	//check if there's something to do
	if len(s.batchedStopTimes) == 0 {

		return nil
	}

	err := gtfs.RecordStopTime(s.batchedStopTimes, dsTx)
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
	stopTime.StopSequence = parser.getInt("stop_sequence", false)
	stopTime.ArrivalTime = parser.getGTFSTime("arrival_time", false)
	stopTime.DepartureTime = parser.getGTFSTime("departure_time", false)
	stopTime.ShapeDistTraveled = parser.getFloat64Pointer("shape_dist_traveled", true)
	return &stopTime, parser.getError()
}
