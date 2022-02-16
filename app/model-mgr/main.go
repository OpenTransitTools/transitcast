package main

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/app/model-mgr/modelmgr"
	"github.com/OpenTransitTools/transitcast/foundation/database"
	"github.com/ardanlabs/conf"
	logger "log"
	"os"
)

var build = "develop"

func main() {
	log := logger.New(os.Stdout, "MODEL_MGR : ", logger.LstdFlags|logger.Lmicroseconds|logger.Lshortfile)
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
		SearchScheduleDays int `conf:"default:120"`
	}
	cfg.Version.SVN = build
	cfg.Version.Desc = "Maintain models required by current schedule in database"

	const prefix = "MODEL_MGR"

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
	case "discover":
		log.Printf("Discovering models")
		err := modelmgr.DiscoverAndRecordRequiredModels(log, db, cfg.SearchScheduleDays)
		return err
	default:
		printUsage(usage)
		return nil
	}
}

func printUsage(confUsage string) {
	fmt.Println(confUsage)
	fmt.Println("commands:")
	fmt.Println("discover: examine current schedule and discover required models")
}
