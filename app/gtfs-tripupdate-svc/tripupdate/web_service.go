package tripupdate

import (
	"context"
	"encoding/json"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/OpenTransitTools/transitcast/business/data/gtfsrtproto"
	"github.com/gorilla/mux"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	logger "log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

//defaultHttpHandler simple default http handler for default route
type defaultHttpHandler struct {
}

//ServeHTTP implements defaultHttpHandler http.Handler interface
func (h *defaultHttpHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Add("Application-Status", "OK")
}

//gtfsTripUpdateHandler holds data needed to respond and log tripUpdate requests
type gtfsTripUpdateHandler struct {
	log                     *logger.Logger
	updateCollection        *updateCollection
	expireTripUpdateSeconds uint64
}

//gtfsTripUpdateHandler factory
func makeGtfsTripUpdateHandler(log *logger.Logger,
	updateCollection *updateCollection,
	expireTripUpdateSeconds int) *gtfsTripUpdateHandler {
	return &gtfsTripUpdateHandler{
		log:                     log,
		updateCollection:        updateCollection,
		expireTripUpdateSeconds: uint64(expireTripUpdateSeconds),
	}
}

//ServeHTTP implements gtfsTripUpdateHandler's  http.Handler interface
func (t *gtfsTripUpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	asText := strings.ToLower(r.FormValue("text")) == "true"
	asJson := strings.ToLower(r.FormValue("json")) == "true"
	if asJson {
		t.serveJSON(w)
	} else {
		t.serveGTFSRT(asText, w)
	}
}

//serveGTFSRT sends tripUpdates in google protocol buffer format, or as text if asText is true
func (t *gtfsTripUpdateHandler) serveGTFSRT(asText bool, w http.ResponseWriter) {
	feedMessage := t.buildFeedMessage(uint64(time.Now().Unix()))

	if asText {
		t.writeProtocolBufferAsText(feedMessage, w)
	} else {
		t.writeProtocolBuffer(feedMessage, w)
	}

}

//writeProtocolBuffer marshal gtfsrtproto.FeedMessage as protocol buffer to http.ResponseWriter
func (t *gtfsTripUpdateHandler) writeProtocolBuffer(feedMessage *gtfsrtproto.FeedMessage, w http.ResponseWriter) {
	bytes, err := proto.Marshal(feedMessage)
	if err != nil {
		t.log.Printf("Failed to marshal gtfsrtproto.FeedMessage to bytes, error:%s", err)
		http.Error(w, "Error serving request", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/grtfeed")
	bytesWritten, err := w.Write(bytes)
	if err != nil {
		t.log.Printf("Error writing bytes to http.ResponseWriter, error:%s", err)
		return
	}
	t.log.Printf("wrote %d bytes for grtfeed", bytesWritten)
}

//writeProtocolBufferAsText write plain text formatting of gtfsrtproto.FeedMessage to http.ResponseWritter
func (t *gtfsTripUpdateHandler) writeProtocolBufferAsText(feedMessage *gtfsrtproto.FeedMessage, w http.ResponseWriter) {
	stringResponse := prototext.MarshalOptions{Multiline: true}.Format(feedMessage)
	w.Header().Set("Content-Type", "text/plain")
	bytesWritten, err := w.Write([]byte(stringResponse))
	if err != nil {
		t.log.Printf("Error writing bytes to http.ResponseWriter, error:%s", err)
		http.Error(w, "Error serving request", http.StatusInternalServerError)
		return
	}
	t.log.Printf("wrote %d bytes for grtfeed in text format", bytesWritten)
}

//serveJSON sends all gtfs.TripUpdate as json, wrapped by JsonTripUpdateResponseWrapper to http.ResponseWriter
func (t *gtfsTripUpdateHandler) serveJSON(w http.ResponseWriter) {
	now := uint64(time.Now().Unix())
	jsonWrapper := makeJsonTripUpdateResponseWrapper(now, t.currentUpdates(now))
	jsonData, err := json.Marshal(jsonWrapper)
	if err != nil {
		t.log.Printf("Error marshaling tripUpdates to json: error:%v\n", err)
		http.Error(w, "Error serving request", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	byteCount, err := w.Write(jsonData)
	if err != nil {
		t.log.Printf("Error writing json response: %s", err)
		return
	}
	t.log.Printf("wrote %d bytes in json response.", byteCount)

}

//currentUpdates retrieves all updateWrappers that have not expired as of "now"
func (t *gtfsTripUpdateHandler) currentUpdates(now uint64) []*updateWrapper {

	allUpdates := t.updateCollection.updateList()
	var results []*updateWrapper
	for _, u := range allUpdates {
		if now-u.tripUpdate.Timestamp <= t.expireTripUpdateSeconds {
			results = append(results, u)
		}
	}
	return results
}

//buildFeedMessage retrieve current tripUpdates as of "now" and build gtfsrtproto.FeedMessage from them
func (t *gtfsTripUpdateHandler) buildFeedMessage(now uint64) *gtfsrtproto.FeedMessage {
	gtfsRealtimeVersion := "2.0"
	incrementality := gtfsrtproto.FeedHeader_FULL_DATASET
	feedMessage := gtfsrtproto.FeedMessage{
		Header: &gtfsrtproto.FeedHeader{
			GtfsRealtimeVersion: &gtfsRealtimeVersion,
			Incrementality:      &incrementality,
			Timestamp:           &now,
		},
		Entity: []*gtfsrtproto.FeedEntity{},
	}
	var tripUpdateEntities []*gtfsrtproto.FeedEntity
	for _, update := range t.currentUpdates(now) {
		tripUpdateEntities = append(tripUpdateEntities, makeTripUpdateFeedEntity(update))
	}

	feedMessage.Entity = tripUpdateEntities
	return &feedMessage
}

//makeTripUpdateFeedEntity create gtfsrtproto.FeedEntity from tripUpdateProtoc in updateWrapper
func makeTripUpdateFeedEntity(update *updateWrapper) *gtfsrtproto.FeedEntity {
	entity := gtfsrtproto.FeedEntity{
		Id:         &update.tripUpdate.TripId,
		TripUpdate: update.tripUpdateProtoc,
	}

	return &entity
}

//JsonTripUpdateResponseWrapper provides json response wrapper around gtfs.TripUpdates
type JsonTripUpdateResponseWrapper struct {
	Timestamp   uint64             `json:"timestamp"`
	TripUpdates []*gtfs.TripUpdate `json:"trip_updates"`
}

//makeJsonTripUpdateResponseWrapper creates JsonTripUpdateResponseWrapper with tripUpdates from updateWrapper
func makeJsonTripUpdateResponseWrapper(now uint64, updates []*updateWrapper) *JsonTripUpdateResponseWrapper {
	tripUpdates := make([]*gtfs.TripUpdate, 0)
	for _, update := range updates {
		tripUpdates = append(tripUpdates, update.tripUpdate)
	}
	return &JsonTripUpdateResponseWrapper{
		Timestamp:   now,
		TripUpdates: tripUpdates,
	}
}

//createServer creates configured http.Server for responding to gtfs-rt tripUpdate requests
func createServer(log *logger.Logger,
	updateCollection *updateCollection,
	expireTripUpdateSeconds int,
	httpPort int) *http.Server {

	tripUpdateService := makeGtfsTripUpdateHandler(log, updateCollection, expireTripUpdateSeconds)

	r := mux.NewRouter()
	r.Handle("/", &defaultHttpHandler{})
	r.Handle("/tripUpdate", tripUpdateService)
	srv := &http.Server{
		Addr: strings.Join([]string{"0.0.0.0", strconv.Itoa(httpPort)}, ":"),
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      r,
	}
	return srv
}

//runWebService starts up tripUpdate web service, and terminates on shutdown signal
func runWebService(log *logger.Logger,
	wg *sync.WaitGroup,
	updateCollection *updateCollection,
	expireTripUpdateSeconds int,
	httpPort int,
	shutdownSignal chan bool,
) {
	wg.Add(1)
	defer wg.Done()
	srv := createServer(log, updateCollection, expireTripUpdateSeconds, httpPort)
	log.Printf("Starting server on port %d", httpPort)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("server ListenAndServe ended. %s", err)
		}
	}()
	shutdownCtx, serverCancelFunc := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer serverCancelFunc()

	select {
	case <-shutdownSignal:
		log.Printf("ending webservice on shutdown signal")
		err := srv.Shutdown(shutdownCtx)
		if err != nil {
			log.Printf("error shutting down webservice, error:%s", err)
		}
	}

}
