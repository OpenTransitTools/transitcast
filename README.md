# Transitcast

## Real time transit monitor

This project uses a static gtfs static schedules and frequent queries against a gtfs-rt vehicle position feed generating
an estimated transition time it takes for each vehicle to move from scheduled stop to scheduled stop recording these
an "observed_stop_time" table. These records can later be used to train a machine learning model to make vehicle travel
predictions.

This project is in currently in a proof of concept stage. Its purpose is to discover if data collected from a publicly
available gtfs and gtfs-rt are adequate to produce predictions that are better than reporting the mean time taken to
travel between the same stops. Its only been run against data from developer.trimet.org at this time, and assumes that
all static gtfs distance units are measured in feet.

If this proof of concept is successful this project will grow into a larger project to produce viable ML models and use
them to predict arrival times for a transit agency.

Go modules are vendored with 'go mod vendor' to ensure reproducible builds.

### Project Structure

Project structure is based on Ardan Labs service example project.

https://github.com/ardanlabs/service

This project contains two executables. 'gtfs-loader' is responsible for loading a static gtfs schedule. 'gtfs-monitor'
is meant to run continuously to monitor a gtfs-rt vehicle feed. Their entry points can be found in
app/gtfs-loader/main.go and app/gtfs-monitor/main.go. Code specific to these programs are located under their respective
folders.

The business folder contains packages that can be shared across current or future executables in this project.

The foundation folder contains packages that can be shared across current or future projects. They make no policy on
application choices such as logging, configuration or execution lifecycles. Packages in this folder are candidates for
separate "kit" project.

The Ardan Labs conf package is used to gather environment variables and command line arguments for configuration.

https://github.com/ardanlabs/conf

### Project usage

#### build

Use go 1.16 or better. To build the two executables change the working directory to this project and run:

    go build ./app/gtfs-loader
	go build ./app/gtfs-monitor
    go build ./app/model-mgr

#### Database

Uses a postgresql database. Create a user and database, and 'grant all on database' to that user.

The project doesn't currently generate or maintain its own schema. Run the contents of ddl/schedule_and_monitor_ddl.sql
followed by ddl/models_ddl.sql on the database while logged in as that user.

The 'observed_stop_time' and 'trip_deviation' tables are partitioned. Partitions will need to be manually created before 
the table can be used by the gtfs-monitor program. For example to create partitions for the month of August and 
September 2021 run the following:

    create table observed_stop_time_part_2021_08 partition of observed_stop_time for values from ('2021-08-01') to ('2021-09-01');
    create table observed_stop_time_part_2021_09 partition of observed_stop_time for values from ('2021-09-01') to ('2021-10-01');
    create table trip_deviation_part_2021_08 partition of trip_deviation for values from ('2021-08-01') to ('2021-09-01');
    create table trip_deviation_part_2021_09 partition of trip_deviation for values from ('2021-09-01') to ('2021-10-01');

#### gtfs-load

gtfs-loader should be run on a frequent basis to check that the latest static gtfs schedule is loaded from an url using
the "load" command. HTTP "ETag" or "Last-Modified" headers provided by the server are used to see if a new version of
the gtfs needs to be loaded. If neither of these are available or accurate the gtfs-loader should be run manually with
the --gtfs-force-download argument or LOADER_GTFS_FORCE_DOWNLOAD environment variable set to true when a new schedule
becomes available, or on regular basis as appropriate for the publisher of the static gtfs schedule.

Example environment variable setup and usage to load or update gtfs schedule

    export LOADER_DB_USER=database_username
    export LOADER_DB_PASSWORD=a_good_password 
    export LOADER_DB_NAME=database_name
    export LOADER_DB_HOST=database_host
    export LOADER_GTFS_URL=https://developer.trimet.org/schedule/gtfs.zip
    ./gtfs-loader load

gtfs-load has a 'list' command to list instances of the gtfs-static schedule that are loaded in the database. These are
stored as a 'data set' where each static gtfs table has a 'data set id'. Schedule data at any particular time uses the
same 'data set id' to identify what schedule was current at the time.

gtfs-load 'delete' can be used to remove a gtfs data set and all schedule rows associated with it.

Requires calendar.txt, trips.txt, stop_times.txt and shapes.txt in GTFS file. Optionally loads calendar_dates.txt if present.

GTFS optional fields required by this project: 

- trips.txt requires shape_id column
- stop_times.txt requires shape_dist_traveled column

#### gtfs-monitor

gtfs-monitor frequently polls a gtfs-rt vehicle position feed monitoring bus transition times between stops, recording
results to the 'observed_stop_time' table. It expects to find a current gtfs schedule in the database loaded by
gtfs-loader.

gtfs-monitor is intended to run inside a container. Logging is sent to STDOUT. If running in a container is not desired
it can be run inside a terminal multiplexer such as screen or tmux.

Example environment variable setup and usage to monitor gtfs-rt schedule. Replace <appid> with a valid appid in this
example:

    export MONITOR_DB_USER=database_username
    export MONITOR_DB_PASSWORD=a_good_password 
    export MONITOR_DB_NAME=database_name
    export MONITOR_DB_HOST=database_host
    export MONITOR_GTFS_VEHICLE_POSITIONS_URL=https://developer.trimet.org/ws/V1/VehiclePositions/appid/<appid>
    ./gtfs-monitor

#### model-mgr

model-mgr examines currently active Dataset as loaded by the last gtfs-loader and creates ml_model and ml_model_stop 
records that are required to cover current schedule.

Example environment variable setup and usage to create ml_model records:

    export MODEL_MGR_DB_USER=database_username
    export MODEL_MGR_DB_PASSWORD=a_good_password
    export MODEL_MGR_DB_NAME=database_name
    export MODEL_MGR_DB_HOST=database_host
    ./gtfs-mgr discover

