package command

import (
	"encoding/json"
	"time"

	"github.com/TerrexTech/agg-userauth-model/user"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

type deleteResult struct {
	MatchedCount int `json:"matchedCount,omitempty"`
}

func deleteUser(c *cmdConfig) ([]byte, *model.Event, *model.Error) {
	userModel := &user.User{}
	err := json.Unmarshal(c.cmd.Data, userModel)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling cmd-data into User")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}

	matches, err := c.coll.Find(userModel)
	if err != nil || len(matches) == 0 {
		err = errors.New("user not found")
		return nil, nil, model.NewError(model.UserError, err.Error())
	}
	result := deleteResult{
		MatchedCount: len(matches),
	}
	marshalResult, err := json.Marshal(result)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling result")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}

	uuid, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error generating Event-UUID")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}
	event := &model.Event{
		Action:        "UserDeleted",
		AggregateID:   user.AggregateID,
		CorrelationID: c.cmd.UUID,
		Data:          c.cmd.Data,
		NanoTime:      time.Now().UnixNano(),
		Source:        "agg-userauth-cmd",
		UUID:          uuid,
		YearBucket:    2018,
	}

	return marshalResult, event, nil
}
