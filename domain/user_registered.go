package domain

import (
	"encoding/json"

	"github.com/TerrexTech/go-common-models/model"

	"github.com/TerrexTech/agg-userauth-model/user"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

func userRegistered(coll *mongo.Collection, event *model.Event) error {
	user := &user.User{}
	err := json.Unmarshal(event.Data, user)
	if err != nil {
		err = errors.Wrap(err, "Error while unmarshalling Event-data")
		return err
	}

	_, err = coll.InsertOne(user)
	if err != nil {
		err = errors.Wrap(err, "Error Inserting User into Mongo")
		return err
	}

	return nil
}
