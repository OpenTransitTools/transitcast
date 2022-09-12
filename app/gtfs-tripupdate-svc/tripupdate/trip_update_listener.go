package tripupdate

import (
	"encoding/json"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/nats-io/nats.go"
	logger "log"
	"os"
	"sync"
)

//runTripUpdateListener starts NATS subscription on tripUpdatePredictionSubject for gtfs.TripUpdate messages.
//Store results in updateCollection. Ends NATS subscription and returns on shutdownSignal
func runTripUpdateListener(
	log *logger.Logger,
	wg *sync.WaitGroup,
	natsConn *nats.Conn,
	updateCollection *updateCollection,
	tripUpdatePredictionSubject string,
	shutdownSignal chan bool) {
	wg.Add(1)
	defer wg.Done()

	ch := make(chan *nats.Msg, 64)
	log.Printf("Subscribing to tripUpdates on subject:%s on nats: %v\n", tripUpdatePredictionSubject,
		natsConn.Servers())
	sub, err := natsConn.ChanSubscribe(tripUpdatePredictionSubject, ch)
	if err != nil {
		log.Printf("Unable to establish subscription to nats server: %v\n", err)
		os.Exit(1)
	}

	for {
		select {
		case msg := <-ch:
			processTripUpdateFromMsg(log, msg, updateCollection)
			break
		case <-shutdownSignal:
			log.Printf("ending TripUpdate listener on shutdown signal\n")
			log.Printf("unsubscribing to nats\n")
			err = sub.Unsubscribe()
			if err != nil {
				log.Printf("Error unsubscribing to nats:%s", err)
			}
			return
		}
	}
}

//processTripUpdateFromMsg un-marshal gtfs.TripUpdate from nats.Msg, craete updateWrapper and store
//result in updateCollection
func processTripUpdateFromMsg(log *logger.Logger, msg *nats.Msg, updateCollection *updateCollection) {
	var tripUpdate gtfs.TripUpdate
	err := json.Unmarshal(msg.Data, &tripUpdate)
	if err != nil {
		log.Printf("error parsing TripUpdate: %s, payload:%s", err, string(msg.Data))
		return
	}
	newUpdate := makeUpdateWrapper(&tripUpdate)
	updateCollection.addTripUpdate(newUpdate)

}
