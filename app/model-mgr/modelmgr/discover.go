package modelmgr

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/OpenTransitTools/transitcast/business/data/mlmodels"
	"github.com/jmoiron/sqlx"
	"strings"
	"time"
)

//discoveredModels holds all unique MLModels by name
type discoveredModels struct {
	modelsByName map[string]*mlmodels.MLModel
}

//makeDiscoveredModels makeDiscoveredModels builder
func makeDiscoveredModels() *discoveredModels {
	return &discoveredModels{modelsByName: make(map[string]*mlmodels.MLModel)}
}

//addModel convenience method for adding model to map
func (dm *discoveredModels) addModel(model *mlmodels.MLModel) {
	dm.modelsByName[model.ModelName] = model
}

//containsModel convenience method to check for presence of model by model_name
func (dm *discoveredModels) containsModel(modelName string) bool {
	_, contains := dm.modelsByName[modelName]
	return contains
}

//getUniqueTripIds retrieves all trip ids in dataset that are active during activeServiceIds
func loadUniqueTripIds(db *sqlx.DB,
	dataSet *gtfs.DataSet,
	activeServiceIds []string) ([]string, error) {

	var tripIds []string
	query := "select trip_id from trip where data_set_id = ? and service_id in (?)"
	query, args, err := sqlx.In(query, dataSet.Id, activeServiceIds)
	if err != nil {
		return nil, fmt.Errorf("unable to convert query. query:%s error: %w", query, err)
	}
	err = db.Select(&tripIds, db.Rebind(query), args...)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve trip_ids from from table. query:%s error: %w", query, err)
	}
	return tripIds, nil

}

//loadStopTimesForTrip retrieves gtfs.StopTime in stop sequence order for tripId
func loadStopTimesForTrip(db *sqlx.DB,
	dataSet *gtfs.DataSet,
	tripId string) ([]*gtfs.StopTime, error) {

	query := "select * from stop_time where data_set_id = $1 and trip_id = $2 " +
		"order by stop_sequence"
	rows, err := db.Queryx(query, dataSet.Id, tripId)
	if err != nil {
		return nil, fmt.Errorf("unable to load stops for trip_id %s, error: %w", tripId, err)
	}

	stopTimes := make([]*gtfs.StopTime, 0)
	for rows.Next() {
		sti := gtfs.StopTime{}
		err = rows.StructScan(&sti)
		if err != nil {
			return nil, fmt.Errorf("unable to load stop time for trip_id %s, error: %w", tripId, err)
		}
		stopTimes = append(stopTimes, &sti)
	}
	return stopTimes, nil
}

//updateRequiredModelIfNeeded sets mlmodels.MLModel.CurrentlyRelevant to true and updates record if needed
func updateRequiredModelIfNeeded(db *sqlx.DB, model *mlmodels.MLModel) (*mlmodels.MLModel, error) {

	if !model.CurrentlyRelevant {
		model.CurrentlyRelevant = true
		return mlmodels.UpdateMLModel(db, model)
	}
	return model, nil
}

//markModelsNotRelevant sets all mlmodels.MLModel.CurrentlyRelevant columns to false for models map
//and updates record if needed
func markModelsNotRelevant(db *sqlx.DB, models map[string]*mlmodels.MLModel) (int, error) {
	count := 0
	for _, model := range models {
		if model.CurrentlyRelevant {
			model.CurrentlyRelevant = false
			_, err := mlmodels.UpdateMLModel(db, model)
			if err != nil {
				return count, err
			}
			count++
		}
	}
	return count, nil
}

//discoverCurrentModels looks through days of service for all trips in current dataset
//and returns discoveredModels containing all models needed
func discoverCurrentModels(db *sqlx.DB, days int) (*discoveredModels, error) {
	//get current dataset
	dateSet, err := gtfs.GetLatestDataSet(db)
	if err != nil {
		return nil, err
	}

	//retrieve the model times for records being recorded
	timePointModelType, stopsModelTime, err := getModelTypes(db)
	if err != nil {
		return nil, err
	}

	//retrieve all active unique service ids from now to days ahead
	now := time.Now()
	activeServiceIds, err := gtfs.GetActiveServiceIdsBetween(db, dateSet, now, now.AddDate(0, 0, days))

	//retrieve all tripids active for those service ids
	if err != nil {
		return nil, err
	}
	tripIds, err := loadUniqueTripIds(db, dateSet, activeServiceIds)

	//load all unique models for all stops on those trips
	if err != nil {
		return nil, err
	}
	models, err := discoverModelsInTrips(db, dateSet, tripIds, timePointModelType, stopsModelTime)
	if err != nil {
		return nil, err
	}

	return models, err
}

//discoverModelsInTrips creates models for each tripId for dataSet
func discoverModelsInTrips(
	db *sqlx.DB,
	dataSet *gtfs.DataSet,
	tripIds []string,
	timePointModelType *mlmodels.MLModelType,
	stopsModelTime *mlmodels.MLModelType) (*discoveredModels, error) {

	models := makeDiscoveredModels()

	//limit := 5
	//count := 0
	for _, tripId := range tripIds {
		//if count > limit {
		//	return models, nil
		//}
		stopTimes, err := loadStopTimesForTrip(db, dataSet, tripId)
		if err != nil {
			return nil, fmt.Errorf("while discovering models error: %w", err)
		}
		discoverModelsOnTrip(models, stopTimes, timePointModelType, stopsModelTime)
		//count++

	}
	return models, nil
}

//discoverModelsOnTrip add MLModels to discoveredModels for stopTimes on trip, in stop sequence order
func discoverModelsOnTrip(models *discoveredModels,
	stopTimes []*gtfs.StopTime,
	timePointModelType *mlmodels.MLModelType,
	stopsModelTime *mlmodels.MLModelType) {
	var previousStop *gtfs.StopTime
	var currentStops []*gtfs.StopTime
	for _, currentStopTime := range stopTimes {
		currentStops = append(currentStops, currentStopTime)
		if previousStop != nil {
			addModel(models, []*gtfs.StopTime{previousStop, currentStopTime}, stopsModelTime)
			//check if this is a timepoint
			if currentStopTime.Timepoint == 1 {
				//don't create model if two timepoints are adjacent
				if len(currentStops) > 2 {
					addModel(models, currentStops, timePointModelType)
				}
				currentStops = []*gtfs.StopTime{currentStopTime}
			}
		}
		previousStop = currentStopTime
	}
}

//addModel creates and adds model to discoveredModels
func addModel(models *discoveredModels, stopTimes []*gtfs.StopTime, modelType *mlmodels.MLModelType) {
	modelName := getModelNameForStops(stopTimes)
	if models.containsModel(modelName) {
		return
	}
	model := makeModel(stopTimes, modelName, modelType)
	//model will be nil if there aren't enough stops
	if model != nil {
		models.addModel(model)
	}

}

//makeModel creates model with MLStopTimes for each gtfs.StopTime pair.
func makeModel(stopTimes []*gtfs.StopTime,
	modelName string,
	modelType *mlmodels.MLModelType) *mlmodels.MLModel {
	if len(stopTimes) < 2 {
		return nil
	}
	now := time.Now()
	model := mlmodels.MakeMLModel(modelType, 1, now, modelName)
	modelStopSequence := 1
	var previousStopTime *gtfs.StopTime
	for _, stopTime := range stopTimes {
		if previousStopTime != nil {
			model.ModelStops = append(model.ModelStops,
				mlmodels.MakeMLModelStop(modelStopSequence, previousStopTime.StopId, stopTime.StopId))
		}
		previousStopTime = stopTime
	}
	return model
}

//getModelNameForStops names a model based on its series of stops
func getModelNameForStops(stopTimes []*gtfs.StopTime) string {
	stopNames := make([]string, len(stopTimes))
	for i, st := range stopTimes {
		stopNames[i] = st.StopId
	}
	return strings.Join(stopNames, "_")
}

//getModelTypes returns all MLModelTypes currently known.
//returns model types for timepoint, stops in that order
func getModelTypes(db *sqlx.DB) (*mlmodels.MLModelType, *mlmodels.MLModelType, error) {
	timePointModelType, err := mlmodels.GetMLModelType(db, "Timepoints")
	if err != nil {
		return nil, nil, err
	}
	stopsModelType, err := mlmodels.GetMLModelType(db, "Stops")

	return timePointModelType, stopsModelType, err
}
