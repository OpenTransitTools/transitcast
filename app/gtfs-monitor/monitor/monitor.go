// Package monitor monitors gtfs a vehicle feed
package monitor

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/jmoiron/sqlx"
	"log"
	"os"
	"time"
)

//RunVehicleMonitorLoop starts loop that monitors gtfs-rt feed and records results for use in ML processing.
func RunVehicleMonitorLoop(log *log.Logger,
	db *sqlx.DB,
	url string,
	loopEverySeconds int,
	earlyTolerance float64,
	expirePositionSeconds int,
	shutdownSignal chan os.Signal) error {

	loopDuration := time.Duration(loopEverySeconds) * time.Second

	sleepChan := make(chan bool)
	sleep := time.Duration(0) //sleep for zero seconds the first time

	relevantTripCache := makeTripCache(time.Now())
	monitorCollection := newVehicleMonitorCollection(earlyTolerance, expirePositionSeconds)

	positionRecorder := makeDBRecorder(log, db)

	for {

		go func() {
			time.Sleep(sleep)
			sleepChan <- true
		}()

		select {
		case <-shutdownSignal:
			log.Printf("Exiting on shutdown signal")
			return nil
		case <-sleepChan:
			break
		}

		//set default sleep for next loop in the event of an error after continue statements
		sleep = loopDuration

		// mark the time we start working
		start := time.Now()

		vehiclePositions, err := getVehiclePositions(log, url)

		if err != nil {
			log.Printf("error retrieving vehicle positions. error:%v\n", err)
			continue
		}

		log.Printf("loaded %d vehicle positions\n", len(vehiclePositions))

		//load required trips
		loadedTrips, err := relevantTripCache.loadRelevantTrips(log, db, start, vehiclePositions)

		if err != nil {
			log.Printf("error attempting to get required trip for vehicle positions. error:%v\n", err)
			continue
		}

		//update vehicle positions and retrieve new positions for recording to TripDeviation
		updateVehiclePositions(log, positionRecorder, vehiclePositions, loadedTrips, &monitorCollection)

		// attempt to run the loop every loopEverySeconds by subtracting the time it took to perform the work
		workTook := time.Now().Sub(start)

		log.Printf("work took %s\n", fmtDuration(workTook))

		// if the work took longer than loopEverySeconds don't sleep at all on the next loop
		if workTook >= loopDuration {
			sleep = time.Duration(0)
		} else {
			sleep = loopDuration - workTook
		}

	}
}

//updateVehiclePositions runs vehiclePositions through vehicleMonitors and saves results to database
//returns map of new tripStopPositions by blockId
func updateVehiclePositions(log *log.Logger,
	recorder rtRecorder,
	positions []vehiclePosition,
	loadedTripInstancesByTripId map[string]*gtfs.TripInstance,
	monitorCollection *vehicleMonitorCollection) {

	countNewTripStopPositions := 0
	countNewObservations := 0

	for _, position := range positions {
		vm := monitorCollection.getOrMakeVehicle(position.Id)
		var trip *gtfs.TripInstance
		if position.TripId != nil {
			trip = loadedTripInstancesByTripId[*position.TripId]
		}

		newPosition, observations := vm.newPosition(log, position, trip)

		if newPosition != nil {
			recorder.recordTripStopPosition(loadedTripInstancesByTripId, newPosition)
			countNewTripStopPositions++
		}

		recorder.recordObservedStopTimePositions(observations)

		countNewObservations += len(observations)

	}

	if countNewObservations > 0 {
		log.Printf("Made %d new stop time observations", countNewObservations)
	}

	if countNewTripStopPositions > 0 {
		log.Printf("Made %d new trip stop positions", countNewObservations)
	}

}

//fmtDuration returns a string presentation of time.Duration for logging
func fmtDuration(d time.Duration) string {
	d = d.Round(time.Millisecond)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	mill := d / time.Millisecond
	return fmt.Sprintf("%02d:%02d.%d", h, m, mill)
}
