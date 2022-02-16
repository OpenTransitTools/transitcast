package modelmgr

import (
	"encoding/json"
	"github.com/OpenTransitTools/transitcast/business/data/gtfs"
	"github.com/OpenTransitTools/transitcast/business/data/mlmodels"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func Test_discoverModelsOnTrip(t *testing.T) {
	orangeLineTrip := getTestStopTimesFromJson("orangeLineTripSouthbound.json", t)

	type expectedModels struct {
		name          string
		expectedStops []string
	}

	expectedModelsOnOrangeLineTrip := []expectedModels{
		{
			name: "7601_9303",
			expectedStops: []string{
				"7601", "9303",
			},
		},
		{
			name: "9303_7627",
			expectedStops: []string{
				"9303", "7627",
			},
		},
		{
			name: "7627_7646",
			expectedStops: []string{
				"7627", "7646",
			},
		},
		{
			name: "7646_7608",
			expectedStops: []string{
				"7646", "7608",
			},
		},
		{
			name: "7608_7618",
			expectedStops: []string{
				"7608", "7618",
			},
		},
		{
			name: "7618_7606",
			expectedStops: []string{
				"7618", "7606",
			},
		},
		{
			name: "7606_13710",
			expectedStops: []string{
				"7606", "13710",
			},
		},
		{
			name: "13710_13711",
			expectedStops: []string{
				"13710", "13711",
			},
		},
		{
			name: "13713_13714",
			expectedStops: []string{
				"13713", "13714",
			},
		},
		{
			name: "13714_13715",
			expectedStops: []string{
				"13714", "13715",
			},
		},
		{
			name: "13715_13716",
			expectedStops: []string{
				"13715", "13716",
			},
		},
		{
			name: "13716_13717",
			expectedStops: []string{
				"13716", "13717",
			},
		},
		{
			name: "13717_13718",
			expectedStops: []string{
				"13717", "13718",
			},
		},
		{
			name: "13718_13720",
			expectedStops: []string{
				"13718", "13720",
			},
		},
		{
			name: "7601_9303_7627_7646",
			expectedStops: []string{
				"7601", "9303", "7627", "7646",
			},
		},
		{
			name: "7646_7608_7618_7606_13710",
			expectedStops: []string{
				"7646", "7608", "7618", "7606", "13710",
			},
		},
		{
			name: "13711_13712",
			expectedStops: []string{
				"13711", "13712",
			},
		},
		{
			name: "13712_13713",
			expectedStops: []string{
				"13712", "13713",
			},
		},
		{
			name: "13712_13713_13714_13715",
			expectedStops: []string{
				"13712", "13713", "13714", "13715",
			},
		},
		{
			name: "13715_13716_13717_13718",
			expectedStops: []string{
				"13715", "13716", "13717", "13718",
			},
		},
	}

	timePointModelType := &mlmodels.MLModelType{
		MLModelTypeId: 1,
		Name:          "Timepoints",
	}
	stopsModelType := &mlmodels.MLModelType{
		MLModelTypeId: 2,
		Name:          "Stops",
	}
	type args struct {
		stopTimes [][]*gtfs.StopTime
	}

	tests := []struct {
		name          string
		args          args
		expectedModel []expectedModels
	}{
		{
			name: "Orange Line models",
			args: args{
				stopTimes: [][]*gtfs.StopTime{orangeLineTrip},
			},
			expectedModel: expectedModelsOnOrangeLineTrip,
		},
		{
			name: "Orange Line models, same stops three times",
			args: args{
				stopTimes: [][]*gtfs.StopTime{orangeLineTrip, orangeLineTrip, orangeLineTrip},
			},
			expectedModel: expectedModelsOnOrangeLineTrip,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			models := makeDiscoveredModels()
			for _, stopTimes := range tt.args.stopTimes {
				discoverModelsOnTrip(models, stopTimes, timePointModelType, stopsModelType)
			}
			if len(models.modelsByName) != len(tt.expectedModel) {
				t.Errorf("expected %d models, but instead have %d", len(tt.expectedModel), len(models.modelsByName))
			}
			for _, expectedModel := range tt.expectedModel {
				model, present := models.modelsByName[expectedModel.name]
				if !present {
					t.Errorf("didn't find model named %s", expectedModel.name)
					return
				}
				if len(expectedModel.expectedStops) == 2 && model.MLModelTypeId != stopsModelType.MLModelTypeId {
					t.Errorf("model '%s' with two stops expected to be of stopsModelType", expectedModel.name)
					return
				} else if len(expectedModel.expectedStops) > 2 && model.MLModelTypeId != timePointModelType.MLModelTypeId {
					t.Errorf("model '%s' with multiple stops expected to be of timePointModelType", expectedModel.name)
					return
				}
				if len(model.ModelStops) != len(expectedModel.expectedStops)-1 {
					t.Errorf("model '%s' expected to have %d MLModelStops, but has %d", expectedModel.name,
						len(expectedModel.expectedStops)-1, len(model.ModelStops))
					return
				}
				previousStopName := ""
				for i, stopName := range expectedModel.expectedStops {
					if i != 0 { //skip zero since we need the previous and current stop
						modelStop := model.ModelStops[i-1]

						if previousStopName != modelStop.StopId || stopName != modelStop.NextStopId {
							t.Errorf("model '%s' stop at %d expected to be %s to %s , but found %s to %s",
								expectedModel.name, i, previousStopName, stopName,
								modelStop.StopId, modelStop.NextStopId)
							return
						}

					}
					previousStopName = stopName

				}
			}

		})
	}
}

func getTestStopTimesFromJson(fileName string, t *testing.T) []*gtfs.StopTime {
	var result []*gtfs.StopTime
	file, err := ioutil.ReadFile(filepath.Join("testdata", fileName))
	if err != nil {
		t.Errorf("unable to read test trips file: %v", err)
	}
	err = json.Unmarshal(file, &result)
	if err != nil {
		t.Errorf("unable to read test trips file: %v", err)
	}
	return result
}
