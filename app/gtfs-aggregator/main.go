package main

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/app/gtfs-aggregator/aggregator"
	"github.com/OpenTransitTools/transitcast/foundation/database"
	"github.com/ardanlabs/conf"
	"github.com/nats-io/nats.go"
	logger "log"
	"os"
	"os/signal"
	"syscall"
)

var build = "develop"

func main() {
	log := logger.New(os.Stdout, "AGGREGATOR : ", logger.LstdFlags|logger.Lmicroseconds|logger.Lshortfile)
	if err := run(log); err != nil {
		log.Printf("main: error: %+v", err)
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
		NATS struct {
			URL string `conf:"default:localhost"`
		}
		ExpirePredictionSeconds               int     `conf:"default:8"`
		MaximumObservedTransitionAgeInSeconds int     `conf:"default:3600"`
		MinimumRMSEModelImprovement           float64 `conf:"default:0.0"`
		MinimumObservedStopCount              int     `conf:"default:100"`
		PredictionSubject                     string  `conf:"default:trip-update-prediction"`
		ExpirePredictorSeconds                int     `conf:"default:3600"`
		LimitEarlyDepartureSeconds            int     `conf:"default:60"`
	}
	cfg.Version.SVN = build
	cfg.Version.Desc = "Listens to vehicle data generated by gtfs-monitor, collects statistics, requests " +
		"model inference and collates the results into predicted trip segments"
	const prefix = "AGGREGATOR"
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

	// =========================================================================
	// Start nats

	log.Printf("main: Connecting to NATS\n")
	natsConnection, err := nats.Connect(cfg.NATS.URL)
	if err != nil {
		return fmt.Errorf("unable to establish connection to nats server: %w", err)
	}
	defer func() {
		log.Printf("main: closing connection to NATS")
		natsConnection.Close()
	}()

	// Make a channel to listen for an interrupt or terminate signal from the OS.
	// Use a buffered channel because the signal package requires it.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	log.Printf("starting aggregator\n")
	return aggregator.StartPredictionAggregator(log, db, shutdown, natsConnection,
		aggregator.Conf{
			ExpirePredictionSeconds:               cfg.ExpirePredictionSeconds,
			MaximumObservedTransitionAgeInSeconds: cfg.MaximumObservedTransitionAgeInSeconds,
			MinimumRMSEModelImprovement:           cfg.MinimumRMSEModelImprovement,
			MinimumObservedStopCount:              cfg.MinimumObservedStopCount,
			PredictionSubject:                     cfg.PredictionSubject,
			ExpirePredictorSeconds:                cfg.ExpirePredictorSeconds,
			LimitEarlyDepartureSeconds:            cfg.LimitEarlyDepartureSeconds,
		})

}

func printUsage(confUsage string) {
	fmt.Println(confUsage)
}
