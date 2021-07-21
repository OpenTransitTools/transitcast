package gtfs

/*
Shape contains rows from the GTFS shapes.txt file
*/
type Shape struct {
	DataSetId         int64    `db:"data_set_id" json:"data_set_id"`
	ShapeId           string   `db:"shape_id" json:"shape_id"`
	ShapePtLat        float64  `db:"shape_pt_lat" json:"shape_pt_lat"`
	ShapePtLng        float64  `db:"shape_pt_lon" json:"shape_pt_lon"`
	ShapePtSequence   int      `db:"shape_pt_sequence" json:"shape_pt_sequence"`
	ShapeDistTraveled *float64 `db:"shape_dist_traveled" json:"shape_dist_traveled"`
}

// RecordShapes saves shapes to database in a batch
func RecordShapes(shapes []*Shape, dsTx *DataSetTransaction) error {
	for _, shape := range shapes {
		shape.DataSetId = dsTx.DS.Id
	}

	statementString := "insert into shape ( " +
		"data_set_id, " +
		"shape_id, " +
		"shape_pt_lat, " +
		"shape_pt_lon, " +
		"shape_pt_sequence, " +
		"shape_dist_traveled) " +
		"values (" +
		":data_set_id, " +
		":shape_id, " +
		":shape_pt_lat, " +
		":shape_pt_lon, " +
		":shape_pt_sequence, " +
		":shape_dist_traveled)"
	statementString = dsTx.Tx.Rebind(statementString)
	_, err := dsTx.Tx.NamedExec(statementString, shapes)
	return err
}
