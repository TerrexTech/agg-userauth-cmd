package command

import (
	"encoding/json"
	"time"

	"github.com/TerrexTech/agg-userauth-model/user"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

type updateParams struct {
	Filter *user.User `json:"filter,omitempty"`
	Update *user.User `json:"update,omitempty"`
}

func updateUser(c *cmdConfig) ([]byte, *model.Event, *model.Error) {
	params := &updateParams{}
	err := json.Unmarshal(c.cmd.Data, params)
	if err != nil {
		err = errors.Wrap(err, "Error while unmarshalling cmd-data")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}

	validateErr := validateParams(params)
	if err != nil {
		return nil, nil, validateErr
	}

	match, err := c.coll.FindOne(params.Filter)
	if err != nil {
		err = errors.Wrap(err, "Error finding User")
		return nil, nil, model.NewError(model.UserError, err.Error())
	}
	matchedUser, assertOK := match.(*user.User)
	if !assertOK {
		err = errors.New("error asserting find-result to user-map")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}

	userMap, err := userToMap(matchedUser)
	if err != nil {
		err = errors.Wrap(err, "error getting user-map")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}

	updatedUser, err := patchUser(c.cmd.Data, userMap)
	if err != nil {
		err = errors.Wrap(err, "Error patching user")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}

	updateResult := map[string]interface{}{
		"filter": params.Filter,
		"update": updatedUser,
	}
	marshalResult, err := json.Marshal(updateResult)
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
		Action:        "UserUpdated",
		AggregateID:   user.AggregateID,
		CorrelationID: c.cmd.UUID,
		Data:          marshalResult,
		NanoTime:      time.Now().UnixNano(),
		Source:        c.serviceName,
		UUID:          uuid,
		YearBucket:    2018,
	}

	return marshalResult, event, nil
}

func userToMap(u *user.User) (map[string]interface{}, error) {
	marshalUser, err := json.Marshal(u)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling user")
		return nil, err
	}

	userMap := map[string]interface{}{}
	err = json.Unmarshal(marshalUser, &userMap)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling user into map")
		return nil, err
	}
	return userMap, nil
}

func patchUser(
	updateParams []byte,
	userMap map[string]interface{},
) (map[string]interface{}, error) {
	// Extract Update from UpdateParams
	updateParamsMap := map[string]map[string]interface{}{}
	err := json.Unmarshal(updateParams, &updateParamsMap)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling UpdateParams")
		return nil, err
	}

	if updateParamsMap["update"] == nil {
		err = errors.New("Update-field not found in updateParamsMap")
		return nil, err
	}
	for k, v := range updateParamsMap["update"] {
		userMap[k] = v
	}

	return userMap, nil
}

func validateParams(params *updateParams) *model.Error {
	filter := params.Filter
	if filter == nil {
		err := errors.New("nil filter provided")
		return model.NewError(model.UserError, err.Error())
	}

	update := params.Update
	if update == nil {
		err := errors.New("nil update provided")
		return model.NewError(model.UserError, err.Error())
	}
	if update.UserID != "" {
		err := errors.New("userID cannot be changed")
		return model.NewError(model.UserError, err.Error())
	}
	if update.UserName != "" {
		err := errors.New("username cannot be changed")
		return model.NewError(model.UserError, err.Error())
	}
	if update.Password == "" {
		err := errors.New("found blank password")
		return model.NewError(model.UserError, err.Error())
	}

	return nil
}
