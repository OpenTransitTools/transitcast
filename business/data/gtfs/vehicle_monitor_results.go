package gtfs

//VehicleMonitorResults holds all information produced from observing a vehicle move
//ObservedStopTimes may be empty if the vehicle has not been seen moving between stops
//TripDeviations will be included for any trip within range of the vehicle
type VehicleMonitorResults struct {
	VehicleId         string
	ObservedStopTimes []*ObservedStopTime
	TripDeviations    []*TripDeviation
}
