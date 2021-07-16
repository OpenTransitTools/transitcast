package main

import (
	"fmt"
	"github.com/ardanlabs/conf"
	"time"
)

//tripExportCmd contains required arguments for exportTrip command execution
type tripExportCmd struct {
	tripId          string
	date            time.Time
	destinationFile string
}

//parseTripExportCmd using conf.Args attemps to load tripExportCmd, returns error if any arguments are not present or malformed
func parseTripExportCmd(args conf.Args) (*tripExportCmd, error) {

	tripId := args.Num(1)
	if len(tripId) < 1 {
		return nil, fmt.Errorf("expected tripId id with command exportTrip")
	}
	dateString := args.Num(2)
	if len(dateString) < 1 {
		return nil, fmt.Errorf("expected dateString in yyyy-MM-dd format with command exportTrip")
	}
	//Mon Jan 2 15:04:05 -0700 MST 2006
	date, err := time.Parse("2006-01-02T15:04:05-0700", dateString)

	if err != nil {
		return nil, fmt.Errorf("exportTrip cmd expects dateString in yyyy-MM-ddTHH:mm:ssZ format where Z is local time minus UTC, error: %w", err)
	}
	destinationFile := args.Num(3)
	if len(destinationFile) < 1 {
		return nil, fmt.Errorf("expected destination command exportTrip")
	}
	return &tripExportCmd{
		tripId:          tripId,
		date:            date,
		destinationFile: destinationFile,
	}, nil

}
