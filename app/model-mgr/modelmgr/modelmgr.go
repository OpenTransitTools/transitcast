package modelmgr

import (
	"fmt"
	"github.com/OpenTransitTools/transitcast/business/data/mlmodels"
	"github.com/jmoiron/sqlx"
	"log"
)

//DiscoverAndRecordRequiredModels examines current dataset and discovers all models to cover service,
//ensures there are mlmodels.MLModel rows present, and marks any existing rows as not relevant
func DiscoverAndRecordRequiredModels(log *log.Logger, db *sqlx.DB, days int) error {
	log.Printf("Loading all current models\n")
	existingModelsByName, err := mlmodels.GetAllCurrentMLModelsByName(db)
	if err != nil {
		log.Printf("Unable to load existing models from database. error: %s", err)
		return err
	}
	log.Printf("Found %d existing models \n", len(existingModelsByName))
	//retrieve required models
	log.Printf("Finding all required models for current dataset\n")
	requiredModels, err := discoverCurrentModels(db, days)
	if err != nil {
		return fmt.Errorf("unable to discover models, error: %s", err)
	}
	log.Printf("Found %d models required by current dataset\n", len(requiredModels.modelsByName))

	//ensure all required models are present and marked as currently relevant.
	log.Printf("Recording required models")
	existingModelCount := 0
	newModelCount := 0

	for _, requiredModel := range requiredModels.modelsByName {
		if existingModel, present := existingModelsByName[requiredModel.ModelName]; present {
			existingModelCount++
			//remove model from existingModelsByName so any remaining models can be marked as not relevant to the
			//current dataset
			delete(existingModelsByName, requiredModel.ModelName)
			_, err = updateRequiredModelIfNeeded(db, existingModel)
			if err != nil {
				log.Printf("after recording %d models failed to update %+v. error: %s\n",
					newModelCount, requiredModel, err)
			}
		} else {
			_, err = mlmodels.RecordNewMLModel(db, requiredModel)
			if err != nil {
				log.Printf("after recording %d models failed to record %+v. error: %s\n",
					newModelCount, requiredModel, err)
				return err
			}
			newModelCount++
		}
	}
	//update any models remaining in existingModelsByName as not relevant to the current dataset
	markedNotRelevant, err := markModelsNotRelevant(db, existingModelsByName)

	if err != nil {
		log.Printf("after recording %d models, updating %d old models as not relevant, "+
			"failed to mark all irrelevant models as not relevant. error: %s\n",
			newModelCount, markedNotRelevant, err)
		return err
	}

	log.Printf("Recorded %d new models, found %d existing models, "+
		"marked %d models as not relevant to current dataset\n", newModelCount, existingModelCount, markedNotRelevant)
	log.Printf("Total models currently relevant: %d\n", newModelCount+existingModelCount)
	return nil
}
