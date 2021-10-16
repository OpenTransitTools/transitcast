package monitor

import (
	"bytes"
	gtfsrtproto2 "github.com/OpenTransitTools/transitcast/business/data/gtfsrtproto"
	"google.golang.org/protobuf/proto"
	"log"
	"net/http"
	"strconv"
	"time"
)

//vehiclePosition contains fields read from a GTFS-RT vehicle activity feed.
//fields that are optional are pointers and will be nil if they were not present in the feed
type vehiclePosition struct {
	Id                string
	Label             string
	Timestamp         int64
	TripId            *string
	RouteId           *string
	Latitude          *float32
	Longitude         *float32
	Bearing           *float32
	VehicleStopStatus VehicleStopStatus
	StopSequence      *uint32
	StopId            *string
}

//positionIsSame returns true unless any position related differences are seen in other vehiclePosition
//secondsTolerance allows for some skew in the vehiclePosition.Timestamp, due to slight variations
//typically a few seconds, between service calls to VehiclePosition service being handled by different servers
//which may have received the position a few seconds apart
func (v *vehiclePosition) positionIsSame(v2 *vehiclePosition, secondsTolerance int64) bool {
	if v == nil {
		return v2 == nil
	} else if v2 == nil {
		return false
	}
	if v.Id != v2.Id || v.VehicleStopStatus != v2.VehicleStopStatus {
		return false
	}
	if v.Timestamp-v2.Timestamp > secondsTolerance {
		return false
	}
	if v.StopSequence != nil && v2.StopSequence != nil && *v.StopSequence != *v2.StopSequence {
		return false
	}
	if v.Latitude != nil && v2.Latitude != nil && *v.Latitude != *v2.Latitude {
		return false
	}
	if v.Longitude != nil && v2.Longitude != nil && *v.Longitude != *v2.Longitude {
		return false
	}

	return true
}

//String implements Stringer interface for vehiclePosition
func (v *vehiclePosition) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("vehiclePosition{ id:")
	buffer.WriteString(v.Id)
	buffer.WriteString(", Label:\"")
	buffer.WriteString(v.Label)
	buffer.WriteString("\", TripId:")
	if v.TripId == nil {
		buffer.WriteString("unknown")
	} else {
		buffer.WriteString(*v.TripId)
	}
	buffer.WriteString(", previousStopSequence:")
	if v.StopSequence == nil {
		buffer.WriteString("unknown")
	} else {
		buffer.WriteString(strconv.FormatInt(int64(*v.StopSequence), 10))
	}
	buffer.WriteString(", StopPosition:")
	buffer.WriteString(v.VehicleStopStatus.String())
	buffer.WriteString(", Timestamp: ")
	buffer.WriteString(strconv.FormatInt(v.Timestamp, 10))
	buffer.WriteString(" }")
	return buffer.String()
}

// VehicleStopStatus defines the possible relationship a vehicle has to a stop in GTFS
type VehicleStopStatus int

const (
	Unknown VehicleStopStatus = -1
	// IncomingAt indicates vehicle is just about to arrive at the stop (on a stop
	// display, the vehicle symbol typically flashes).
	IncomingAt VehicleStopStatus = 0
	// StoppedAt indicates vehicle is at the stop.
	StoppedAt VehicleStopStatus = 1
	// InTransitTo indicates vehicle has departed a previous stop and is in transit to the next stop.
	InTransitTo VehicleStopStatus = 2
)

// String - Stringer interface for VehicleStopStatus
func (s *VehicleStopStatus) String() string {
	if s == nil {
		return "unknown"
	}
	switch *s {
	case IncomingAt:
		return "INCOMING_AT"
	case StoppedAt:
		return "STOPPED_AT"
	case InTransitTo:
		return "IN_TRANSIT_TO"
	}
	return "Unknown"
}

// IsUnknown convenience method to test for unknown VehicleStopStatus
func (s *VehicleStopStatus) IsUnknown() bool {
	return *s == Unknown
}

// retrieveBytes pulls bytes from url using simple GET request
func retrieveBytes(log *log.Logger, url string) ([]byte, error) {

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() {
		innerErr := resp.Body.Close()
		if innerErr != nil {
			log.Printf("error closing http response body. error: %v\n", innerErr)
		}
	}()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return nil, err
	}

	//log.Infof("Wrote %v bytes, %v", byteCount, resp.Header)
	return buf.Bytes(), nil
}

/*
getVehiclePositions Retrieves gtfs-realtime vehicle positions and loads them into a non-protocol buffer object.
Any changes to the GTFS-realtime protocol or generated code can be handled here and not elsewhere in the program.
*/
func getVehiclePositions(log *log.Logger, url string) ([]vehiclePosition, error) {
	gtfsResponseBytes, err := retrieveBytes(log, url)
	if err != nil {
		return nil, err
	}
	feedMessage := gtfsrtproto2.FeedMessage{}
	err = proto.Unmarshal(gtfsResponseBytes, &feedMessage)
	if err != nil {
		log.Printf("Unable to unmarshal FeedMessage: %v\n", err)
		return nil, err
	}
	var vehiclePositions []vehiclePosition
	now := time.Now().Unix()
	for _, entity := range feedMessage.Entity {
		if entity.Vehicle == nil {
			continue
		}
		vehicle := entity.Vehicle
		vehicleDescriptor := vehicle.Vehicle
		if vehicleDescriptor == nil || vehicleDescriptor.Id == nil {
			log.Printf("Vehicle entity missing vehicle identifier, %v\n", entity.Id)
			continue
		}
		position := vehiclePosition{
			Id:                *vehicleDescriptor.Id,
			StopSequence:      vehicle.CurrentStopSequence,
			VehicleStopStatus: getVehicleStopStatus(vehicle.CurrentStatus),
		}
		if vehicleDescriptor.Label != nil {
			position.Label = *vehicleDescriptor.Label
		}

		trip := vehicle.Trip
		if trip != nil {
			position.TripId = trip.TripId
			position.RouteId = trip.RouteId
		}

		if vehicle.Position != nil {
			vehPos := vehicle.Position
			position.Latitude = vehPos.Latitude
			position.Longitude = vehPos.Longitude
			position.Bearing = vehPos.Bearing
		}
		if vehicle.Timestamp != nil {
			position.Timestamp = int64(*vehicle.Timestamp)
		} else {
			position.Timestamp = now
		}
		if vehicle.StopId != nil {
			position.StopId = vehicle.StopId
		}

		vehiclePositions = append(vehiclePositions, position)
	}
	return vehiclePositions, nil
}

// getVehicleStopStatus converts gtfs status to VehicleStopStatus
func getVehicleStopStatus(status *gtfsrtproto2.VehiclePosition_VehicleStopStatus) VehicleStopStatus {
	if status == nil {
		return Unknown
	}
	switch *status {
	case gtfsrtproto2.VehiclePosition_INCOMING_AT:
		return IncomingAt
	case gtfsrtproto2.VehiclePosition_STOPPED_AT:
		return StoppedAt
	case gtfsrtproto2.VehiclePosition_IN_TRANSIT_TO:
		return InTransitTo
	default:
		return Unknown
	}
}
