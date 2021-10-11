package gtfsmanager

import "github.com/OpenTransitTools/transitcast/business/data/gtfs"

const batchedShapeCount = 250

// shapeRowReader implements gtfsRowReader interface for gtfs.Shape
// batches inserts
type shapeRowReader struct {
	batchedShapeRows []*gtfs.Shape
	shapeMaxDistMap  map[string]float64
}

func newShapeRowReader() *shapeRowReader {
	return &shapeRowReader{
		shapeMaxDistMap: make(map[string]float64),
	}
}

func (s *shapeRowReader) addRow(parser *gtfsFileParser, dsTx *gtfs.DataSetTransaction) error {
	shape, err := buildShape(parser)
	if err != nil {
		return err
	}
	s.batchedShapeRows = append(s.batchedShapeRows, shape)
	s.addMaxShapeDistance(shape)

	//check if its time to save the batch
	if len(s.batchedShapeRows) == batchedShapeCount {
		return s.flush(dsTx)
	}
	return nil
}

//addMaxShapeDistance saves the furthest distance seen on this shapeId in shapeMaxDistMap for later use
func (s *shapeRowReader) addMaxShapeDistance(shape *gtfs.Shape) {
	if shape.ShapeDistTraveled == nil {
		return
	}
	furthest := s.shapeMaxDistMap[shape.ShapeId]
	if *shape.ShapeDistTraveled > furthest {
		s.shapeMaxDistMap[shape.ShapeId] = *shape.ShapeDistTraveled
	}
}

func (s *shapeRowReader) flush(dsTx *gtfs.DataSetTransaction) error {
	//check if there's something to do
	if len(s.batchedShapeRows) == 0 {

		return nil
	}

	err := gtfs.RecordShapes(s.batchedShapeRows, dsTx)
	if err != nil {
		return err
	}

	// truncate the batch
	s.batchedShapeRows = make([]*gtfs.Shape, 0)
	return nil
}

func buildShape(parser *gtfsFileParser) (*gtfs.Shape, error) {
	shape := gtfs.Shape{}
	shape.ShapeId = parser.getString("shape_id", false)
	shape.ShapePtLat = parser.getFloat64("shape_pt_lat", false)
	shape.ShapePtLng = parser.getFloat64("shape_pt_lon", false)
	shape.ShapePtSequence = parser.getInt("shape_pt_sequence", false)
	shape.ShapeDistTraveled = parser.getFloat64Pointer("shape_dist_traveled", true)
	return &shape, parser.getError()
}
