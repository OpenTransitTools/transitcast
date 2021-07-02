// Package monitor monitors gtfs a vehicle feed
package monitor

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"gitlab.trimet.org/transittracker/transitmon/business/data/gtfs"
	"log"
	"os"
	"time"
)

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
	loadedTrips := make(map[string]*gtfs.TripInstance)
	monitorCollection := newVehicleMonitorCollection(earlyTolerance, expirePositionSeconds)
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

		// mark the time we start working
		start := time.Now()

		vehiclePositions, err := getVehiclePositions(log, url)

		if err != nil {
			log.Printf("error attempting to get vehicle positions. error:%v\n", err)
			continue
		}

		log.Printf("loaded %d vehicle positions\n", len(vehiclePositions))

		loadedTrips, err = collectRequiredTrips(log, db, vehiclePositions, time.Now(), loadedTrips)

		if err != nil {
			log.Printf("error attempting to get required trip for vehicle positions. error:%v\n", err)
			continue
		}

		updateVehiclePositions(log, db, vehiclePositions, loadedTrips, &monitorCollection)

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

//collectRequiredTrips loads all trips that are required for processing list of vehiclePositions and returns as a map by tripId
//only trips not present in loadedTripInstances are retrieved
//any trips in loadedTripInstances that are no longer needed will not be included in the return map.
func collectRequiredTrips(log *log.Logger,
	db *sqlx.DB,
	vehiclePositions []vehiclePosition,
	now time.Time,
	loadedTripInstancesByTripId map[string]*gtfs.TripInstance) (map[string]*gtfs.TripInstance, error) {

	requiredTrips := make(map[string]*gtfs.TripInstance)
	tripIdsNeeded := make([]string, 0)
	uniqTripIdsNeeded := make(map[string]bool)

	for _, position := range vehiclePositions {
		if position.TripId != nil {
			tripId := *position.TripId
			if trip, present := loadedTripInstancesByTripId[tripId]; present {
				requiredTrips[tripId] = trip
			} else {
				//only add to list if not already present
				if _, present = uniqTripIdsNeeded[tripId]; !present {
					uniqTripIdsNeeded[tripId] = true
					tripIdsNeeded = append(tripIdsNeeded, tripId)
				}

			}

		}
	}

	log.Printf("%d trips loaded, need %d new trips\n", len(requiredTrips), len(tripIdsNeeded))
	if len(tripIdsNeeded) == 0 {
		return requiredTrips, nil
	}

	startTime, endTime := getStartEndTimeToSearchForTrips(now)
	batchResult, err := gtfs.GetTripInstances(db, now, startTime, endTime, tripIdsNeeded)
	if err != nil {
		return requiredTrips, err
	}
	log.Printf("loaded of %d of %d new trips\n", len(batchResult.TripInstancesByTripId), len(tripIdsNeeded))
	if len(batchResult.MissingTripIds) > 0 {
		log.Printf("unable to find tripIds %+v\n", batchResult.MissingTripIds)
	}
	if len(batchResult.ScheduleSliceOutOfRange) > 0 {
		log.Printf("unable to find matching schedule slices for tripIds %+v\n", batchResult.ScheduleSliceOutOfRange)
	}

	// add all the trips loaded into the requiredTrips result
	for _, trip := range batchResult.TripInstancesByTripId {
		requiredTrips[trip.TripId] = trip
	}

	return requiredTrips, nil
}

//getStartEndTimeToSearchForTrips produces very wide range of time to search for valid trip schedules at a point in time
//but still shouldn't overlap
func getStartEndTimeToSearchForTrips(now time.Time) (start time.Time, end time.Time) {
	const tripSearchRangeSeconds = 60 * 60 * 8
	start = now.Add(time.Duration(-tripSearchRangeSeconds) * time.Second)
	end = now.Add(time.Duration(tripSearchRangeSeconds) * time.Second)
	return
}

//updateVehiclePositions runs vehiclePositions through vehicleMonitors and saves results to database
func updateVehiclePositions(log *log.Logger,
	db *sqlx.DB,
	positions []vehiclePosition,
	loadedTripInstancesByTripId map[string]*gtfs.TripInstance,
	monitorCollection *vehicleMonitorCollection) {

	for _, position := range positions {
		vm := monitorCollection.getOrMakeVehicle(position.Id)
		var trip *gtfs.TripInstance
		if position.TripId != nil {
			trip = loadedTripInstancesByTripId[*position.TripId]
		}
		observations := vm.newPosition(log, &position, trip)

		// for now we just log it until the database is ready
		for _, observation := range observations {
			log.Printf("Vehicle %s on route %s moved from %s to %s in %d\n", observation.VehicleId,
				observation.RouteId, observation.StopId, observation.NextStopId, observation.TravelSeconds)
		}
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
