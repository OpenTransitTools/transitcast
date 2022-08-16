package aggregator

import (
	"encoding/json"
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/nats-io/nats.go"
	logger "log"
	"os"
	"sync"
	"time"
)

//startObservedStopTransitionListener listens on NATS on 'vehicle-monitor-results' subject,
//expecting gtfs.VehicleMonitorResults. Adds all gtfs.VehicleMonitorResults.ObservedStopTimes to observedStopTransitions
//collection
//unlike the startTripUpdateListener, no queue is used so a gtfs-aggregator receives all ObservedStopTimes
func startObservedStopTransitionListener(
	log *logger.Logger,
	wg *sync.WaitGroup,
	osts *observedStopTransitions,
	natsConn *nats.Conn,
	shutdownSignal chan bool) {

	wg.Add(1)
	defer wg.Done()

	ch := make(chan *nats.Msg, 64)
	log.Printf("Subscribing to vehicle-monitor-results in ObservedStopTransitionListener on nats server: %v\n",
		natsConn.Servers())
	sub, err := natsConn.ChanSubscribe("vehicle-monitor-results", ch)
	if err != nil {
		log.Printf("Unable to establish subscription to nats server: %v\n", err)
		os.Exit(1)
	}

	//clean up nats
	defer func() {
		log.Printf("Unsubscribing to vehicle-monitor-results in ObservedStopTransitionListener\n")
		err = sub.Unsubscribe()
		if err != nil {
			log.Printf("Error when attempting to unsubscribe: %v\n", err)
		}
	}()
	for {
		select {
		case msg := <-ch:
			fileOSTMessage(log, osts, msg)
			break
		case <-shutdownSignal:
			log.Printf("exiting ObservedStopTransition listener on shutdown signal\n")
			return
		}
	}

}

//fileOSTMessage unmarshal gtfs.VehicleMonitorResults from NATS msg, and add gtfs.ObservedStopTime to
//observedStopTransitions collection
func fileOSTMessage(log *logger.Logger,
	osts *observedStopTransitions,
	msg *nats.Msg) {
	var vehicleMonitorResults gtfs.VehicleMonitorResults
	err := json.Unmarshal(msg.Data, &vehicleMonitorResults)
	if err != nil {
		log.Printf("Error parsing VehicleMonitorResults: %v, payload:%s", err, string(msg.Data))
		return
	}
	for _, ost := range vehicleMonitorResults.ObservedStopTimes {
		osts.newOST(ost)
	}
}

//observedStopTransitions holds all ObservedStopTimes witnessed for use in stop passage features used in model inference
type observedStopTransitions struct {
	stopToStopOSTMap     map[string]*gtfs.ObservedStopTime
	maximumTransitionAge time.Duration
	mu                   sync.Mutex
}

//makeObservedStopTransitions builds observedStopTransitions
func makeObservedStopTransitions(maximumTransitionSeconds int) *observedStopTransitions {
	return &observedStopTransitions{
		stopToStopOSTMap:     make(map[string]*gtfs.ObservedStopTime),
		maximumTransitionAge: time.Duration(maximumTransitionSeconds) * time.Second,
		mu:                   sync.Mutex{},
	}
}

//stopTransitionName returns the name of stop transition between two stops, for use in observedStopTransitions map
func stopTransitionName(from string, to string) string {
	return fmt.Sprintf("%s_%s", from, to)
}

//newOST adds a gtfs.ObservedStopTime to the collection
func (t *observedStopTransitions) newOST(ost *gtfs.ObservedStopTime) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopToStopOSTMap[stopTransitionName(ost.StopId, ost.NextStopId)] = ost
}

//getOst retrieves the last gtfs.ObservedStopTime between two stops.
//will return nil if the gtfs.ObservedStopTime is too old as defined by observedStopTransitions.maximumTransitionAge
func (t *observedStopTransitions) getOst(from string, to string, at time.Time) *gtfs.ObservedStopTime {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := stopTransitionName(from, to)
	ost, isMapContainsKey := t.stopToStopOSTMap[key]
	if !isMapContainsKey {
		return nil
	}
	age := ost.ObservedTime.Sub(at)
	if age > t.maximumTransitionAge {
		delete(t.stopToStopOSTMap, key)
		return nil
	}
	return ost
}
