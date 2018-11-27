package command

import (
	"encoding/json"
	"time"

	"github.com/TerrexTech/agg-userauth-model/user"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
)

func registerUser(c *cmdConfig) ([]byte, *model.Event, *model.Error) {
	userModel := &user.User{}
	err := json.Unmarshal(c.cmd.Data, userModel)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling command-data into User")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}

	userModel, idErr := updateUserID(userModel)
	if idErr != nil {
		return nil, nil, idErr
	}
	validateErr := validateUser(c.coll, userModel)
	if validateErr != nil {
		return nil, nil, validateErr
	}

	hashedPass, err := bcrypt.GenerateFromPassword([]byte(userModel.Password), 10)
	if err != nil {
		err = errors.Wrap(err, "Error creating Hash from password")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}
	userModel.Password = string(hashedPass)

	cmdData, err := json.Marshal(userModel)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling User")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}

	eventID, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error generating EventID")
		return nil, nil, model.NewError(model.InternalError, err.Error())
	}
	event := &model.Event{
		Action:        "UserRegistered",
		AggregateID:   user.AggregateID,
		CorrelationID: c.cmd.UUID,
		Data:          cmdData,
		NanoTime:      time.Now().UnixNano(),
		Source:        c.serviceName,
		UUID:          eventID,
		YearBucket:    2018,
	}

	return cmdData, event, nil
}

func updateUserID(userModel *user.User) (*user.User, *model.Error) {
	var userID uuuid.UUID
	var err error

	if userModel.UserID != "" {
		userID, err = uuuid.FromString(userModel.UserID)
		if err != nil {
			err = errors.Wrap(err, "Error parsing UserID")
			return nil, model.NewError(model.UserError, err.Error())
		}
	}

	if userID == (uuuid.UUID{}) {
		userID, err := uuuid.NewV4()
		if err != nil {
			err = errors.Wrap(err, "Error generating UserID")
			return nil, model.NewError(model.UserError, err.Error())
		}
		userModel.UserID = userID.String()
	}

	return userModel, nil
}

func validateUser(coll *mongo.Collection, userModel *user.User) *model.Error {
	if userModel.FirstName == "" {
		err := errors.New("missing FirstName for user")
		return model.NewError(model.UserError, err.Error())
	}
	if userModel.Email == "" {
		err := errors.New("missing Email for user")
		return model.NewError(model.UserError, err.Error())
	}
	if userModel.UserName == "" {
		err := errors.New("missing UserName for user")
		return model.NewError(model.UserError, err.Error())
	}
	if userModel.Password == "" {
		err := errors.New("missing Password for user")
		return model.NewError(model.UserError, err.Error())
	}
	if userModel.Role == "" {
		err := errors.New("missing Role for user")
		return model.NewError(model.UserError, err.Error())
	}

	_, err := coll.FindOne(map[string]interface{}{
		"$or": []user.User{
			user.User{
				UserID: userModel.UserID,
			},
			user.User{
				UserName: userModel.UserName,
			},
		},
	})
	if err == nil {
		err = errors.New("user already exists")
		return model.NewError(model.UserError, err.Error())
	}

	return nil
}
