package gtfsmanager

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
)

const batchedTripCount = 250

// tripRowReader implements gtfsRowReader interface for gtfs.Trip
// batches inserts
type tripRowReader struct {
	batchedTrips []*gtfs.Trip
	stopRR       *stopTimeRowReader
	shapeRR      *shapeRowReader
}

func newTripRowReader(stopRR *stopTimeRowReader, shapeRR *shapeRowReader) *tripRowReader {
	return &tripRowReader{
		stopRR:  stopRR,
		shapeRR: shapeRR,
	}
}

func (r *tripRowReader) addRow(parser *gtfsFileParser, dsTx *gtfs.DataSetTransaction) error {
	trip, err := buildTrip(parser)
	if err != nil {
		return err
	}
	err = r.populateColumnsFromChildren(trip)
	if err != nil {
		return err
	}

	r.batchedTrips = append(r.batchedTrips, trip)

	//check if it's time to save the batch
	if len(r.batchedTrips) == batchedTripCount {
		return r.flush(dsTx)
	}
	return nil
}

//populateColumnsFromChildren loads StartTime, EndTime and TripDistance from stopRowReader and ShapeRowReader
func (r *tripRowReader) populateColumnsFromChildren(trip *gtfs.Trip) error {
	tripStopEnds, present := r.stopRR.tripStartEndMap[trip.TripId]
	if !present {
		return fmt.Errorf("found no stops for tripId:%s", trip.TripId)
	}
	trip.StartTime = tripStopEnds.startTime
	trip.EndTime = tripStopEnds.endTime
	trip.TripDistance = tripStopEnds.tripDistance

	shapeDistance, present := r.shapeRR.shapeMaxDistMap[trip.ShapeId]
	if !present {
		return fmt.Errorf("found no shapes for tripId:%s, shapeId:%s",
			trip.TripId, trip.ShapeId)
	}
	if shapeDistance > trip.TripDistance {
		trip.TripDistance = shapeDistance
	}
	return nil
}

func (r *tripRowReader) flush(dsTx *gtfs.DataSetTransaction) error {
	//check if there's something to do
	if len(r.batchedTrips) == 0 {
		return nil
	}

	err := gtfs.RecordTrips(r.batchedTrips, dsTx)
	if err != nil {
		return err
	}
	//truncate batch
	r.batchedTrips = make([]*gtfs.Trip, 0)
	return nil
}

func buildTrip(parser *gtfsFileParser) (*gtfs.Trip, error) {
	trip := gtfs.Trip{
		TripId:        parser.getString("trip_id", false),
		RouteId:       parser.getString("route_id", false),
		ServiceId:     parser.getString("service_id", false),
		TripHeadsign:  parser.getStringPointer("trip_headsign", true),
		TripShortName: parser.getStringPointer("trip_short_name", true),
		BlockId:       parser.getStringPointer("block_id", true),
		ShapeId:       parser.getString("shape_id", false),
	}
	return &trip, parser.getError()
}
