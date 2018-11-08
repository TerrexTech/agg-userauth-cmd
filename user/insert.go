package user

import (
	"encoding/json"
	"log"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
)

// Insert handles "insert" events.
func Insert(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	user := &User{}
	err := json.Unmarshal(event.Data, user)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error while unmarshalling Event-data")
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

	if user.UserID == (uuuid.UUID{}) {
		userID, err := uuuid.NewV4()
		if err != nil {
			err = errors.Wrap(err, "Insert: Error generating UserID")
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
		user.UserID = userID
	}
	if user.FirstName == "" {
		err = errors.New("missing FirstName")
		err = errors.Wrap(err, "Insert")
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
	if user.Email == "" {
		err = errors.New("missing Email")
		err = errors.Wrap(err, "Insert")
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
	if user.UserName == "" {
		err = errors.New("missing Username")
		err = errors.Wrap(err, "Insert")
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
	if user.Password == "" {
		err = errors.New("missing Password")
		err = errors.Wrap(err, "Insert")
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
	if user.Role == "" {
		err = errors.New("missing Role")
		err = errors.Wrap(err, "Insert")
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

	insertResult, err := collection.InsertOne(user)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error Inserting User into Mongo")
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
	insertedID, assertOK := insertResult.InsertedID.(objectid.ObjectID)
	if !assertOK {
		err = errors.New("error asserting InsertedID from InsertResult to ObjectID")
		err = errors.Wrap(err, "Insert")
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

	user.ID = insertedID
	result, err := json.Marshal(user)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error marshalling User Insert-result")
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
		Result:        result,
		ServiceAction: event.ServiceAction,
		UUID:          event.UUID,
	}
}
