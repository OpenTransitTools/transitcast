package tripupdate

import (
	"github.com/nats-io/nats.go"
	logger "log"
	"os"
	"sync"
	"time"
)

//StartServices brings up backgroundLoop, tripUpdateListener and webservice. Exits application on shutdown signal
func StartServices(log *logger.Logger,
	expireTripUpdateSeconds int,
	httpPort int,
	natsConn *nats.Conn,
	tripUpdatePredictionSubject string,
	shutdownSignal chan os.Signal) {

	wg := sync.WaitGroup{}

	//create shared container
	updateCollection := makeUpdateCollection()

	//create shutdown channels
	backgroundLoopShutdown := make(chan bool, 1)
	tripUpdateListenerShutdown := make(chan bool, 1)
	webServiceShutdown := make(chan bool, 1)

	//start all child services
	go runBackgroundLoop(log, &wg, updateCollection, backgroundLoopShutdown, expireTripUpdateSeconds)
	go runTripUpdateListener(log, &wg, natsConn, updateCollection, tripUpdatePredictionSubject,
		tripUpdateListenerShutdown)
	go runWebService(log, &wg, updateCollection, expireTripUpdateSeconds, httpPort, webServiceShutdown)
	select {
	case <-shutdownSignal:
		log.Printf("Exiting on shutdown signal, shutting down subroutines")
		backgroundLoopShutdown <- true
		tripUpdateListenerShutdown <- true
		webServiceShutdown <- true
		wg.Wait()
		log.Printf("Subroutines shut down, exiting trip update service")

	}

}

//runBackgroundLoop frequently runs clean up on updateCollection
func runBackgroundLoop(log *logger.Logger,
	wg *sync.WaitGroup,
	updateCollection *updateCollection,
	shutdownSignal chan bool,
	expireTripUpdateSeconds int) {
	wg.Add(1)
	defer wg.Done()

	sleepChan := make(chan bool)

	loopDuration := time.Duration(3) * time.Second
	sleep := loopDuration

	for {

		go func() {
			time.Sleep(sleep)
			sleepChan <- true
		}()

		select {
		case <-shutdownSignal:
			log.Printf("Exiting background loop on shutdown signal")

			return
		case <-sleepChan:
		}

		removedUpdates, currentUpdateSize := updateCollection.expireUpdates(time.Now(), expireTripUpdateSeconds)

		log.Printf("Trip Update collection has %d trips. Removed %d old trips", currentUpdateSize, removedUpdates)

	}
}
