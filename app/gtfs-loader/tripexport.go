package main

import (
	"fmt"
	"github.com/ardanlabs/conf"
	"time"
)

// tripExportCmd contains required arguments for exportTrip command execution
type tripExportCmd struct {
	tripId          string
	date            time.Time
	destinationFile string
}

// parseTripExportCmd using conf.Args attemps to load tripExportCmd, returns error if any arguments are not present or malformed
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

// aggregatorExportCmd contains required arguments for export aggregator command execution
type aggregatorExportCmd struct {
	start           time.Time
	end             time.Time
	vehicleId       string
	destinationFile string
}

// parseTripExportCmd using conf.Args attemps to load tripExportCmd, returns error if any arguments are not present or malformed
func parseAggregatorExportCmd(args conf.Args) (*aggregatorExportCmd, error) {

	startDate, err := parseDateArg(1, "start", args)
	if err != nil {
		return nil, err
	}

	endDate, err := parseDateArg(2, "end", args)
	if err != nil {
		return nil, err
	}

	vehicleId := args.Num(3)
	if len(vehicleId) < 1 {
		return nil, fmt.Errorf("expected vehicleId id in position 3")
	}

	destinationFile := args.Num(4)
	if len(destinationFile) < 1 {
		return nil, fmt.Errorf("expected destination command exportTrip in position 4")
	}
	return &aggregatorExportCmd{
		start:           *startDate,
		end:             *endDate,
		vehicleId:       vehicleId,
		destinationFile: destinationFile,
	}, nil

}

// parseDateArg retrieves and parses date argument from args
// returns result or error with description of expected parameter
func parseDateArg(argPosition int, name string, args conf.Args) (*time.Time, error) {
	dateString := args.Num(argPosition)
	if len(dateString) < 1 {
		return nil, fmt.Errorf("expected %s in yyyy-MM-dd format in position %d", name, argPosition)
	}
	date, err := time.Parse("2006-01-02T15:04:05-0700", dateString)
	if err != nil {
		return nil, fmt.Errorf("expected %s in yyyy-MM-dd format in position %d, unable to parse %s",
			name, argPosition, dateString)
	}
	return &date, nil
}
