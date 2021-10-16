package monitor

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/jmoiron/sqlx"
	"log"
	"time"
)

// tripCache keeping trips that are currently in service or near to service loaded
type tripCache struct {
	lastLoadedTrips        time.Time
	loadTripsEveryDuration time.Duration
	relevantTripDuration   time.Duration
	//keys are tripIds, holds true values for every trip that is relevant
	requiredTripMap map[string]bool
	loadedTrips     map[string]*gtfs.TripInstance
}

// makeTripCache generates new tripCache
func makeTripCache(now time.Time) *tripCache {
	return &tripCache{
		lastLoadedTrips:        now.Add(-1 * time.Hour),
		loadTripsEveryDuration: 5 * time.Minute,
		relevantTripDuration:   time.Hour,
		requiredTripMap:        make(map[string]bool),
		loadedTrips:            make(map[string]*gtfs.TripInstance),
	}
}

// loadRelevantTrips finds all trips that scheduled in the near future or are currently present in vehiclePositions slice
func (r *tripCache) loadRelevantTrips(
	log *log.Logger,
	db *sqlx.DB,
	now time.Time,
	vehiclePositions []vehiclePosition) (map[string]*gtfs.TripInstance, error) {
	//Only load scheduled trips every so often
	if now.After(r.lastLoadedTrips.Add(r.loadTripsEveryDuration)) {
		// load an hours worth plus how long we wait to reload
		loadTripsUntil := r.loadTripsEveryDuration + r.relevantTripDuration
		requiredTripMap, err := gtfs.GetScheduledTripIds(db, now, now, now.Add(loadTripsUntil))
		if err != nil {
			log.Printf("error retrieving scheduled trip_ids. error:%s\n", err)
			return nil, err
		}
		r.requiredTripMap = requiredTripMap
		r.lastLoadedTrips = now
	}

	requiredTripMap := addVehiclePositionTripIds(r.requiredTripMap, vehiclePositions)

	loadedTrips, err := collectRequiredTrips(log, db, requiredTripMap, time.Now(), r.loadedTrips)
	if err != nil {
		return nil, err
	}
	r.loadedTrips = loadedTrips
	return r.loadedTrips, nil
}

// addVehiclePositionTripIds combine trips from tripIdMap and vehiclePositions into new map
func addVehiclePositionTripIds(tripIdMap map[string]bool, vehiclePositions []vehiclePosition) map[string]bool {
	result := make(map[string]bool)
	for k, v := range tripIdMap {
		result[k] = v
	}
	for _, position := range vehiclePositions {
		if position.TripId != nil {
			result[*position.TripId] = true
		}
	}
	return result
}

//collectRequiredTrips loads all trips that are required for processing list of vehiclePositions and returns as a map by tripId
//only trips not present in loadedTripInstances are retrieved
//any trips in loadedTripInstances that are no longer needed will not be included in the return map.
func collectRequiredTrips(log *log.Logger,
	db *sqlx.DB,
	currentTripIdMap map[string]bool,
	now time.Time,
	loadedTripInstancesByTripId map[string]*gtfs.TripInstance) (map[string]*gtfs.TripInstance, error) {

	requiredTrips := make(map[string]*gtfs.TripInstance)
	tripIdsNeeded := make([]string, 0)
	uniqTripIdsNeeded := make(map[string]bool)

	for tripId := range currentTripIdMap {

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
//but still shouldn't overlap to other schedule days
func getStartEndTimeToSearchForTrips(now time.Time) (start time.Time, end time.Time) {
	const tripSearchRangeSeconds = 60 * 60 * 8
	start = now.Add(time.Duration(-tripSearchRangeSeconds) * time.Second)
	end = now.Add(time.Duration(tripSearchRangeSeconds) * time.Second)
	return
}
