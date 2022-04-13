package monitor

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/jmoiron/sqlx"
	"log"
)

//rtRecorder interface takes observations made by vehicle monitor and sends them to their
// destinations (such as database and/or nats )
type rtRecorder interface {
	//recordTripStopPosition saves tripStopPosition, tripCache is used to find trips relevant to downstream
	//tripInstances for the block
	recordTripStopPosition(tripCache map[string]*gtfs.TripInstance, tsp *tripStopPosition)

	//recordObservedStopTimePositions saves gtfs.ObservedStopTimes
	recordObservedStopTimePositions(observations []gtfs.ObservedStopTime)
}

//dbRecorder implements rtRecorder interface for saving records to database
type dbRecorder struct {
	log *log.Logger
	db  *sqlx.DB
}

//makeDBRecorder creates dbRecorder
func makeDBRecorder(log *log.Logger, db *sqlx.DB) *dbRecorder {
	return &dbRecorder{
		log: log,
		db:  db,
	}
}

func (d dbRecorder) recordTripStopPosition(tripCache map[string]*gtfs.TripInstance, tsp *tripStopPosition) {
	newTripDeviations := collectBlockDeviations(tripCache, tsp)
	err := gtfs.RecordTripDeviation(newTripDeviations, d.db)
	if err != nil {
		log.Printf("failed to record %d trip deviations, error:%v", len(newTripDeviations), err)
		return
	}
}

func (d *dbRecorder) recordObservedStopTimePositions(observations []gtfs.ObservedStopTime) {
	//record each observation
	for _, observation := range observations {

		d.log.Printf("Vehicle %s on route %s moved from %s to %s in %d\n", observation.VehicleId,
			observation.RouteId, observation.StopId, observation.NextStopId, observation.TravelSeconds)
		err := gtfs.RecordObservedStopTime(&observation, d.db)
		if err != nil {
			d.log.Printf("Error saving stop time observation %+v. error: %v", observation, err)
		}
	}
}
