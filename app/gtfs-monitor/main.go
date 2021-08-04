package main

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/app/gtfs-monitor/monitor"
	"github.com/OpenTransitTools/transitcast/foundation/database"
	"github.com/ardanlabs/conf"
	logger "log"
	"os"
	"os/signal"
	"syscall"
)

var build = "develop"

func main() {
	log := logger.New(os.Stdout, "GTFS_MONITOR : ", logger.LstdFlags|logger.Lmicroseconds|logger.Lshortfile)
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
			VehiclePositionsUrl   string  `conf:"default:https://developer.trimet.org/ws/V1/VehiclePositions"`
			LoadEverySeconds      int     `conf:"default:3"`
			EarlyTolerance        float64 `conf:"default:0.1"`
			ExpirePositionSeconds int     `conf:"default:900"`
		}
	}
	cfg.Version.SVN = build
	cfg.Version.Desc = "Maintain gtfs schedule instances in database"
	const prefix = "MONITOR"
	if err := conf.Parse(os.Args[1:], prefix, &cfg); err != nil {
		switch err {
		case conf.ErrHelpWanted:
			usage, err := conf.Usage(prefix, &cfg)
			if err != nil {
				return fmt.Errorf("generating config usage: %w", err)
			}
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

	// Make a channel to listen for an interrupt or terminate signal from the OS.
	// Use a buffered channel because the signal package requires it.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	return monitor.RunVehicleMonitorLoop(log, db, cfg.GTFS.VehiclePositionsUrl, cfg.GTFS.LoadEverySeconds,
		cfg.GTFS.EarlyTolerance, cfg.GTFS.ExpirePositionSeconds, shutdown)

}

func printUsage(confUsage string) {
	fmt.Println(confUsage)
}
