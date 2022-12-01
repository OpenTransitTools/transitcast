package tripupdate

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/OpenTransitTools/transitcast/business/data/gtfsrtproto"
	"sync"
	"time"
)

// updateWrapper holds gtfs.TripUpdate and gtfsrtproto.TripUpdate that was built from it
type updateWrapper struct {
	tripUpdate       *gtfs.TripUpdate
	tripUpdateProtoc *gtfsrtproto.TripUpdate
}

// makeUpdateWrapper builds updateWrapper from gtfs.TripUpdate
func makeUpdateWrapper(tripUpdate *gtfs.TripUpdate) *updateWrapper {
	u := updateWrapper{
		tripUpdate: tripUpdate,
	}
	tripScheduleRelationship := gtfsrtproto.TripDescriptor_SCHEDULED
	stopScheduleRelationship := gtfsrtproto.TripUpdate_StopTimeUpdate_SCHEDULED
	stopNoDataRelationship := gtfsrtproto.TripUpdate_StopTimeUpdate_NO_DATA
	tripUpdateProtoc := gtfsrtproto.TripUpdate{
		Trip: &gtfsrtproto.TripDescriptor{
			TripId:               &tripUpdate.TripId,
			RouteId:              &tripUpdate.RouteId,
			ScheduleRelationship: &tripScheduleRelationship,
		},
		Vehicle: &gtfsrtproto.VehicleDescriptor{
			Id: &tripUpdate.VehicleId,
		},
		StopTimeUpdate: []*gtfsrtproto.TripUpdate_StopTimeUpdate{},
		Timestamp:      &tripUpdate.Timestamp,
	}
	var stopTimeUpdates []*gtfsrtproto.TripUpdate_StopTimeUpdate
	for _, stopTimeUpdate := range tripUpdate.StopTimeUpdates {
		//make new variables so pointers in gtfsStopUpdate doesn't end up pointing to the stopTimeUpdate
		//that's reused by range
		stopSequence := stopTimeUpdate.StopSequence
		stopId := stopTimeUpdate.StopId
		gtfsStopUpdate := gtfsrtproto.TripUpdate_StopTimeUpdate{
			StopSequence: &stopSequence,
			StopId:       &stopId,
		}

		if stopTimeUpdate.PredictionSource == gtfs.NoFurtherPredictions {
			gtfsStopUpdate.ScheduleRelationship = &stopNoDataRelationship
		} else {
			arrivalDelay := int32(stopTimeUpdate.ArrivalDelay)
			gtfsStopUpdate.ScheduleRelationship = &stopScheduleRelationship
			gtfsStopUpdate.Arrival = &gtfsrtproto.TripUpdate_StopTimeEvent{
				Delay: &arrivalDelay,
			}
			if stopTimeUpdate.DepartureDelay != nil {
				departureDelay := int32(stopTimeUpdate.ArrivalDelay)
				gtfsStopUpdate.Departure = &gtfsrtproto.TripUpdate_StopTimeEvent{
					Delay: &departureDelay,
				}
			}
		}

		stopTimeUpdates = append(stopTimeUpdates, &gtfsStopUpdate)
	}
	tripUpdateProtoc.StopTimeUpdate = stopTimeUpdates
	u.tripUpdateProtoc = &tripUpdateProtoc
	return &u
}

// updateCollection contains all current updateWrappers and provides thread safe access to them
type updateCollection struct {
	mu             sync.Mutex
	tripUpdatesMap map[string]*updateWrapper
	tripUpdates    []*updateWrapper
}

// makeUpdateCollection updateCollection factory
func makeUpdateCollection() *updateCollection {
	return &updateCollection{
		tripUpdatesMap: make(map[string]*updateWrapper),
		tripUpdates:    make([]*updateWrapper, 0),
	}
}

// addTripUpdate stores new updateWrapper, discards it if updateCollection already contains a newer updateWrapper for
// the same trip
func (c *updateCollection) addTripUpdate(newUpdate *updateWrapper) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if trip, present := c.tripUpdatesMap[newUpdate.tripUpdate.TripId]; present {
		//new trip is older than previous one, don't replace it
		if trip.tripUpdate.Timestamp > newUpdate.tripUpdate.Timestamp {
			return false
		}
	}
	c.tripUpdatesMap[newUpdate.tripUpdate.TripId] = newUpdate
	newTripUpdates := make([]*updateWrapper, 0)
	for _, u := range c.tripUpdatesMap {
		newTripUpdates = append(newTripUpdates, u)
	}
	c.tripUpdates = newTripUpdates
	return true
}

// updateList returns all updateWrappers currently stored
func (c *updateCollection) updateList() []*updateWrapper {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tripUpdates
}

// expireUpdates removes all updateWrappers that are older than "expireAfterSeconds".
// returns the number of updateWrappers that have been removed and how many are currently stored.
func (c *updateCollection) expireUpdates(at time.Time, expireAfterSeconds int) (removed int, currentSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newMap := make(map[string]*updateWrapper)
	newTripUpdates := make([]*updateWrapper, 0)
	for _, u := range c.tripUpdates {
		seconds := uint64(at.Unix()) - u.tripUpdate.Timestamp
		if seconds < uint64(expireAfterSeconds) {
			newTripUpdates = append(newTripUpdates, u)
			newMap[u.tripUpdate.TripId] = u
		}
	}
	previousSize := len(c.tripUpdates)
	c.tripUpdatesMap = newMap
	c.tripUpdates = newTripUpdates
	currentSize = len(c.tripUpdates)
	return previousSize - currentSize, currentSize
}
