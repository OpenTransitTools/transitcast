package aggregator

import (
	"encoding/json"
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/OpenTransitTools/transitcast/business/data/mlmodels"
	"io/ioutil"
	"testing"
	"time"
)

func getTestTrip(serviceDate time.Time, fileName string, t *testing.T) *gtfs.TripInstance {
	var result *gtfs.TripInstance
	file, err := ioutil.ReadFile(fmt.Sprintf("testdata/%s", fileName))
	if err != nil {
		t.Errorf("unable to read test trips file: %v", err)
	}
	err = json.Unmarshal(file, &result)
	if err != nil {
		t.Errorf("unable to read test trips file: %v", err)
	}
	for _, s := range result.StopTimeInstances {
		s.ArrivalDateTime = gtfs.MakeScheduleTime(serviceDate, s.ArrivalTime)
		s.DepartureDateTime = gtfs.MakeScheduleTime(serviceDate, s.DepartureTime)
	}

	return result
}

func getTestModels(fileName string, t *testing.T) []*mlmodels.MLModel {
	var result []*mlmodels.MLModel
	file, err := ioutil.ReadFile(fmt.Sprintf("testdata/%s", fileName))
	if err != nil {
		t.Errorf("unable to read test trips file: %v", err)
	}
	err = json.Unmarshal(file, &result)
	if err != nil {
		t.Errorf("unable to read test trips file: %v", err)
	}
	return result
}

func getTestModelMap(t *testing.T, fileNames ...string) map[string]*mlmodels.MLModel {

	modelMap := make(map[string]*mlmodels.MLModel)

	for _, fileName := range fileNames {
		for _, model := range getTestModels(fileName, t) {
			modelMap[model.ModelName] = model
		}
	}

	return modelMap
}
