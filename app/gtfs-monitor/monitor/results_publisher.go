package monitor

import (
	"encoding/json"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/jmoiron/sqlx"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

//vehicleMonitorResultsPublisher takes observations made by vehicle monitor and sends them to their
// destinations (such as database and nats )
type vehicleMonitorResultsPublisher struct {
	log              *log.Logger
	db               *sqlx.DB
	natsConnection   *nats.Conn
	recordToDatabase bool
	publishOverNats  bool
}

//makeVehicleMonitorResultsPublisher creates vehicleMonitorResultsPublisher
func makeVehicleMonitorResultsPublisher(log *log.Logger,
	db *sqlx.DB,
	natsConnection *nats.Conn,
	recordToDatabase bool,
	publishOverNats bool) *vehicleMonitorResultsPublisher {
	return &vehicleMonitorResultsPublisher{
		log:              log,
		db:               db,
		natsConnection:   natsConnection,
		recordToDatabase: recordToDatabase,
		publishOverNats:  publishOverNats,
	}
}

//publish sends gtfs.VehicleMonitorResults over NATS and records them to the database according to
//publishOverNats and recordToDatabase
func (v *vehicleMonitorResultsPublisher) publish(results *gtfs.VehicleMonitorResults) {
	now := time.Now()
	//set created at on all observations and log
	for _, observation := range results.ObservedStopTimes {
		observation.CreatedAt = now
		v.log.Printf("Vehicle %s on route %s moved from %s to %s in %d\n", observation.VehicleId,
			observation.RouteId, observation.StopId, observation.NextStopId, observation.TravelSeconds)
	}
	//set created at on all tripDeviations
	for _, tripDeviation := range results.TripDeviations {
		tripDeviation.CreatedAt = now
	}
	if v.publishOverNats {
		v.sendOverNats(results)
	}
	if v.recordToDatabase {
		v.record(results)
	}

}

func (v *vehicleMonitorResultsPublisher) sendOverNats(results *gtfs.VehicleMonitorResults) {
	jsonData, err := json.Marshal(results)
	if err != nil {
		v.log.Printf("failed to marshal VehicleMonitorResults to in "+
			"vehicleMonitorResultsPublisher.sendOverNats, error:%v", err)
		return
	}
	err = v.natsConnection.Publish("vehicle-monitor-results", jsonData)
	if err != nil {
		v.log.Printf("failed to send VehicleMonitorResults in "+
			"vehicleMonitorResultsPublisher.sendOverNats, error:%v", err)
	}
}

func (v *vehicleMonitorResultsPublisher) record(results *gtfs.VehicleMonitorResults) {
	for _, observation := range results.ObservedStopTimes {
		err := gtfs.RecordObservedStopTime(observation, v.db)
		if err != nil {
			v.log.Printf("Error saving stop time observation %+v. error: %v", observation, err)
		}
	}
	err := gtfs.RecordTripDeviation(results.TripDeviations, v.db)
	if err != nil {
		v.log.Printf("failed to record %d trip deviations, error:%v", len(results.TripDeviations), err)
		return
	}

}
