package main

import (
	"fmt"
	"gitlab.trimet.org/transittracker/transitmon/foundation/database"
	logger "log"
	"os"
	"strconv"

	"github.com/ardanlabs/conf"
	"gitlab.trimet.org/transittracker/transitmon/app/gtfs-loader/gtfsmanager"
)

var build = "develop"

func main() {
	log := logger.New(os.Stdout, "GTFS_LOADER : ", logger.LstdFlags|logger.Lmicroseconds|logger.Lshortfile)
	if err := run(log); err != nil {
		log.Printf("main: error: %v", err)
		os.Exit(1)
	}
}

func run(log *logger.Logger) error {
	var cfg struct {
		conf.Version
		Args conf.Args
		DB   struct {
			User       string `conf:"default:postgres"`
			Password   string `conf:"default:postgres,noprint"`
			Host       string `conf:"default:0.0.0.0"`
			Name       string `conf:"default:postgres"`
			DisableTLS bool   `conf:"default:true"`
		}
		GTFS struct {
			Url           string `conf:"default:https://developer.trimet.org/schedule/gtfs.zip"`
			TempDir       string `conf:"default:gtfs_tmp"`
			ForceDownload bool   `conf:"default:false"`
		}
	}
	cfg.Version.SVN = build
	cfg.Version.Desc = "Maintain gtfs schedule instances in database"

	const prefix = "LOADER"

	usage, err := conf.Usage(prefix, &cfg)
	if err != nil {
		return fmt.Errorf("generating config usage: %w", err)
	}

	if err := conf.Parse(os.Args[1:], prefix, &cfg); err != nil {
		switch err {
		case conf.ErrHelpWanted:
			printUsage(usage)
			return nil
		case conf.ErrVersionWanted:
			version, err := conf.VersionString(prefix, &cfg)
			if err != nil {
				return fmt.Errorf("generating config version: %w", err)
			}
			fmt.Println(version)
			return nil
		}
		return fmt.Errorf("parsing config: %w", err)
	}

	// =========================================================================
	// App Starting

	// Print the build version for our logs. Also expose it under /debug/vars.
	log.Printf("main : Started : Application initializing : version %s", build)
	defer log.Println("main: Completed")

	out, err := conf.String(&cfg)
	if err != nil {
		return fmt.Errorf("generating config for output: %w", err)
	}
	log.Printf("main: Config :\n%v\n", out)

	// =========================================================================
	// Start Database

	log.Println("main: Initializing database support")

	db, err := database.Open(database.Config{
		User:       cfg.DB.User,
		Password:   cfg.DB.Password,
		Host:       cfg.DB.Host,
		Name:       cfg.DB.Name,
		DisableTLS: cfg.DB.DisableTLS,
	})
	if err != nil {
		return fmt.Errorf("connecting to db: %w", err)
	}
	defer func() {
		log.Printf("main: Database Stopping : %s", cfg.DB.Host)
		err = db.Close()
		if err != nil {
			log.Printf("main: error closing database: %v", err)
		}
	}()

	switch cfg.Args.Num(0) {
	case "load":
		err = gtfsmanager.UpdateGTFSSchedule(log, db, cfg.GTFS.TempDir, cfg.GTFS.Url, cfg.GTFS.ForceDownload)
		if err != nil {
			return err
		}
		return gtfsmanager.ListGTFSSchedules(db)
	case "delete":
		dataSetIdString := cfg.Args.Num(1)
		if len(dataSetIdString) < 1 {
			return fmt.Errorf("expected data set id with command delete")
		}
		dataSetId, err := strconv.ParseInt(cfg.Args.Num(1), 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse data set id %s, error: %w", dataSetIdString, err)
		}
		return gtfsmanager.DeleteGTFSSchedule(log, db, dataSetId)

	case "list":
		return gtfsmanager.ListGTFSSchedules(db)
	case "exportTrip":
		exportCmd, err := parseTripExportCmd(cfg.Args)
		if err != nil {
			log.Printf("error parsing exportTrip command: %v", err)
			printUsage(usage)
			return err
		}
		return gtfsmanager.ExportTripToJson(log, db, exportCmd.date, exportCmd.tripId, exportCmd.destinationFile)

	default:
		printUsage(usage)
		return nil
	}
}

func printUsage(confUsage string) {
	fmt.Println(confUsage)
	fmt.Println("commands:")
	fmt.Println("load: download and update (if needed) latest gtfs data set")
	fmt.Println("delete <dataSetID>: remove a gtfs data set from the database with <dataSetID>")
	fmt.Println("list: list all gtfs data sets in the database")
	fmt.Println("exportTrip <tripID> <date in yyyy-MM-ddTHH:mm:ssZ> " +
		"(where Z is local time minus UTC, example -0700 for 7 hours) <destination>: export trip instance in json format to destination file")
}
