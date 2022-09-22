package gtfs

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/foundation/database"
	"github.com/jmoiron/sqlx"
)

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

// GetShapes collects Shape collections and returns results in ShapePtSequence order inside a map
// returns:
//		map with results keyed by shapeIds,
//		slice of missing shapeIds (where no Shape records could be found)
func GetShapes(db *sqlx.DB,
	dataSetId int64,
	shapeIds []string) (map[string][]*Shape, []string, error) {

	results := make(map[string][]*Shape)
	seenShapeIds := make(map[string]bool, 0)
	missingShapeIds := make([]string, 0)

	if len(shapeIds) < 1 {
		return results, missingShapeIds, nil
	}

	statementString := "select * from shape where data_set_id = :data_set_id and shape_id in (:shape_ids)" +
		"order by shape_id, shape_pt_sequence"
	rows, err := database.PrepareNamedQueryRowsFromMap(statementString, db, map[string]interface{}{
		"data_set_id": dataSetId,
		"shape_ids":   shapeIds,
	})
	defer func() {
		if rows != nil {
			_ = rows.Close()
		}
	}()
	if err != nil {
		return nil, missingShapeIds, fmt.Errorf("unable to retrieve shapeIds %v, error: %w", shapeIds, err)
	}

	currentShapeId := ""
	currentShapes := make([]*Shape, 0)
	for rows.Next() {
		shape := Shape{}
		err = rows.StructScan(&shape)
		if err != nil {
			return nil, missingShapeIds, err
		}

		// check if the current row is the start of a new Shape
		if currentShapeId != shape.ShapeId {

			//if there are items in currentShapes add them to the results map for the currentShapeId
			if len(currentShapes) > 0 {
				results[currentShapeId] = currentShapes
				// create new currentShapes
				currentShapes = make([]*Shape, 0)

			}

			//set the new currentShapeId being iterated over
			currentShapeId = shape.ShapeId

			//add to list of currentShapeId that have been seen
			seenShapeIds[currentShapeId] = true

		}
		currentShapes = append(currentShapes, &shape)

	}
	//take care of last list of shapes
	if len(currentShapes) > 0 {
		//put the currentShapes times into the results
		results[currentShapeId] = currentShapes
	}

	//find shapeIds that were not found
	for _, shapeId := range shapeIds {
		//check in map of seen shapeIds
		if _, shapeIdPresent := seenShapeIds[shapeId]; !shapeIdPresent {
			missingShapeIds = append(missingShapeIds, shapeId)
		}
	}

	return results, missingShapeIds, err
}
