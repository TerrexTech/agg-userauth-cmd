package user

import (
	"encoding/json"
	"log"

	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

type userUpdate struct {
	Filter map[string]interface{} `json:"filter"`
	Update map[string]interface{} `json:"update"`
}

type updateResult struct {
	MatchedCount  int64 `json:"matchedCount,omitempty"`
	ModifiedCount int64 `json:"modifiedCount,omitempty"`
}

// Update handles "update" events.
func Update(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	userUpdate := &userUpdate{}

	err := json.Unmarshal(event.Data, userUpdate)
	if err != nil {
		err = errors.Wrap(err, "Update: Error while unmarshalling Event-data")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	if len(userUpdate.Filter) == 0 {
		err = errors.New("blank filter provided")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	update := userUpdate.Update

	if len(update) == 0 {
		err = errors.New("blank update provided")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	if update["userID"] != nil && update["userID"] == (uuuid.UUID{}).String() {
		err = errors.New("found blank userID in update")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}
	if update["password"] != nil && update["password"] == "" {
		err = errors.New("found blank password in update")
		err = errors.Wrap(err, "Update")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	updateStats, err := collection.UpdateMany(userUpdate.Filter, update)
	if err != nil {
		err = errors.Wrap(err, "Update: Error in UpdateMany")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     DatabaseError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	result := &updateResult{
		MatchedCount:  updateStats.MatchedCount,
		ModifiedCount: updateStats.ModifiedCount,
	}
	resultMarshal, err := json.Marshal(result)
	if err != nil {
		err = errors.Wrap(err, "Update: Error marshalling User Update-result")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	return &model.KafkaResponse{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		EventAction:   event.EventAction,
		Result:        resultMarshal,
		ServiceAction: event.ServiceAction,
		UUID:          event.UUID,
	}
}
