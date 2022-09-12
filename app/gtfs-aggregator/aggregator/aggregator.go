package aggregator

import (
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/jmoiron/sqlx"
	"github.com/nats-io/nats.go"
	logger "log"
	"os"
	"sync"
	"time"
)

//Conf contains all configurable parameters in aggregator
type Conf struct {
	ExpirePredictionSeconds               int
	MaximumObservedTransitionAgeInSeconds int
	MinimumRMSEModelImprovement           float64
	MinimumObservedStopCount              int
	PredictionSubject                     string
	ExpirePredictorSeconds                int
	LimitEarlyDepartureSeconds            int
	InferenceBuckets                      int
	IncludedRouteIds                      []string
}

//StartPredictionAggregator starts all routines for aggregation of predicted trips
//shuts down all routines after receiving on shutdownSignal
func StartPredictionAggregator(log *logger.Logger,
	db *sqlx.DB,
	shutdownSignal chan os.Signal,
	natsConn *nats.Conn,
	conf Conf) error {

	//create shared objects

	log.Println("Creating shared aggregator structures")
	log.Println("Creating pendingPredictionsCollection")
	pendingPredictions := makePendingPredictionsCollection(conf.ExpirePredictionSeconds)
	log.Println("Creating ObservedStopTransitions")
	osts := makeObservedStopTransitions(conf.MaximumObservedTransitionAgeInSeconds)
	log.Println("Creating predictionPublisher")
	publisher := makePredictionPublisher(log, natsConn, conf.PredictionSubject,
		conf.LimitEarlyDepartureSeconds)
	log.Println("Creating tripPredictorsCollection")
	predictorsCollection, err := makeTripPredictorsCollection(db, osts,
		conf.MinimumRMSEModelImprovement, conf.MinimumObservedStopCount, conf.ExpirePredictorSeconds)
	log.Println("Done creating shared aggregator structures")

	if err != nil {
		return err
	}

	// start up background loop
	wg := sync.WaitGroup{}
	backgroundLoopShutdown := make(chan bool, 1)
	ostSubscriptionShutdown := make(chan bool, 1)
	tripUpdateSubscriberShutdown := make(chan bool, 1)
	inferenceListenerShutdown := make(chan bool, 1)

	log.Println("Starting background loop")
	go runBackgroundLoop(log, &wg, pendingPredictions, predictorsCollection, backgroundLoopShutdown)
	log.Println("Starting ObservedStopTransitionListener")
	go startObservedStopTransitionListener(log, &wg, osts, natsConn, ostSubscriptionShutdown)
	log.Println("Starting TripUpdateListener")
	go startTripUpdateListener(log, &wg, osts, natsConn, tripUpdateSubscriberShutdown, predictorsCollection,
		pendingPredictions, publisher, conf.IncludedRouteIds, conf.InferenceBuckets)
	log.Println("Starting InferenceListener")
	go startInferenceResponseListener(log, &wg, natsConn, inferenceListenerShutdown, pendingPredictions, publisher)

	select {
	case <-shutdownSignal:
		log.Printf("Exiting on shutdown signal, shutting down subroutines")
		backgroundLoopShutdown <- true
		ostSubscriptionShutdown <- true
		tripUpdateSubscriberShutdown <- true
		inferenceListenerShutdown <- true
		wg.Wait()
		log.Printf("Subroutines shut down, exiting aggregator")

	}
	return nil
}

//runBackgroundLoop frequently runs clean up on pendingPredictionsCollection and tripPredictorsCollection
func runBackgroundLoop(log *logger.Logger,
	wg *sync.WaitGroup,
	pendingPredictions *pendingPredictionsCollection,
	tripPredictorsCollection *tripPredictorsCollection,
	shutdownSignal chan bool) {
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

		// mark the time we start working
		start := time.Now()

		expiredPredictions, pendingPredictionsAfterCleanup := pendingPredictions.removeExpiredPredictions(start)

		completedPredictions, incompletePredictions := countExpiredPredictionCompletions(expiredPredictions)

		log.Printf("PendingPredictions has %d. failed: %d, completed: %d\n",
			pendingPredictionsAfterCleanup, incompletePredictions, completedPredictions)

		pendingAtStart, afterCleanup := tripPredictorsCollection.removeExpiredPredictors(start)

		log.Printf("tripPredictorsCollection have %d removed %d\n", afterCleanup, pendingAtStart-afterCleanup)

		workTook := time.Now().Sub(start)

		// if the work took longer than loopEverySeconds don't sleep at all on the next loop
		if workTook >= loopDuration {
			sleep = time.Duration(0)
		} else {
			sleep = loopDuration - workTook
		}
	}
}

//countExpiredPredictionCompletions count number of predictions completed and not completed in expiredBatches
func countExpiredPredictionCompletions(expiredBatches []*predictionBatch) (completed int, notCompleted int) {

	completed = 0
	notCompleted = 0
	for _, batch := range expiredBatches {
		for _, pendingTrip := range batch.pendingTripPredictions {

			for _, stop := range pendingTrip.tripPrediction.stopPredictions {
				if stop.predictionComplete {
					if stop.predictionSource == gtfs.StopMLPrediction ||
						stop.predictionSource == gtfs.TimepointMLPrediction {
						completed++
					}

				} else {
					notCompleted++
				}
			}
		}
	}
	return completed, notCompleted
}
