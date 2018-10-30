package user

import (
	"encoding/json"

	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
)

// AggregateID is the global AggregateID for UserAuth Aggregate.
const AggregateID int8 = 1

// User defines the User Aggregate.
type User struct {
	ID        objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	UserID    uuuid.UUID        `bson:"userID,omitempty" json:"userID,omitempty"`
	Email     string            `bson:"email,omitempty" json:"email,omitempty"`
	FirstName string            `bson:"first_name,omitempty" json:"first_name,omitempty"`
	LastName  string            `bson:"last_name,omitempty" json:"last_name,omitempty"`
	Username  string            `bson:"username,omitempty" json:"username,omitempty"`
	Password  string            `bson:"password,omitempty" json:"password,omitempty"`
	Roles     []string          `bson:"roles,omitempty" json:"roles,omitempty"`
}

// marshalUser is a simplified User, for convenient marshalling/unmarshalling operations
type marshalUser struct {
	ID        objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	UserID    string            `bson:"userID,omitempty" json:"userID,omitempty"`
	Email     string            `bson:"email,omitempty" json:"email,omitempty"`
	FirstName string            `bson:"firstName,omitempty" json:"firstName,omitempty"`
	LastName  string            `bson:"lastName,omitempty" json:"lastName,omitempty"`
	Username  string            `bson:"username,omitempty" json:"username,omitempty"`
	Password  string            `bson:"password,omitempty" json:"password,omitempty"`
	Roles     []string          `bson:"roles,omitempty" json:"roles,omitempty"`
}

// MarshalBSON returns bytes of BSON-type.
func (u *User) MarshalBSON() ([]byte, error) {
	mu := &marshalUser{
		ID:        u.ID,
		UserID:    u.UserID.String(),
		FirstName: u.FirstName,
		LastName:  u.LastName,
		Email:     u.Email,
		Username:  u.Username,
		Password:  u.Password,
		Roles:     u.Roles,
	}

	m, err := bson.Marshal(mu)
	if err != nil {
		err = errors.Wrap(err, "MarshalBSON Error")
	}
	return m, err
}

// MarshalJSON returns bytes of JSON-type.
func (u *User) MarshalJSON() ([]byte, error) {
	mu := map[string]interface{}{
		"firstName": u.FirstName,
		"lastName":  u.LastName,
		"email":     u.Email,
		"username":  u.Username,
		"password":  u.Password,
		"roles":     u.Roles,
		"userID":    u.UserID.String(),
	}

	if u.ID != objectid.NilObjectID {
		mu["_id"] = u.ID.Hex()
	}

	m, err := json.Marshal(mu)
	if err != nil {
		err = errors.Wrap(err, "MarshalJSON Error")
	}
	return m, err
}

// UnmarshalBSON returns BSON-type from bytes.
func (u *User) UnmarshalBSON(in []byte) error {
	m := make(map[string]interface{})
	err := bson.Unmarshal(in, m)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalBSON Error")
		return err
	}

	err = u.unmarshalFromMap(m)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalBSON Error")
	}
	return err
}

// UnmarshalJSON returns JSON-type from bytes.
func (u *User) UnmarshalJSON(in []byte) error {
	m := make(map[string]interface{})
	err := json.Unmarshal(in, &m)
	if err != nil {
		err = errors.Wrap(err, "Unmarshal Error")
		return err
	}

	err = u.unmarshalFromMap(m)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalJSON Error")
	}
	return err
}

// unmarshalFromMap unmarshals Map into User.
func (u *User) unmarshalFromMap(m map[string]interface{}) error {
	var err error
	var assertOK bool

	// Hoping to discover a better way to do this someday
	if m["_id"] != nil {
		u.ID, assertOK = m["_id"].(objectid.ObjectID)
		if !assertOK {
			u.ID, err = objectid.FromHex(m["_id"].(string))
			if err != nil {
				err = errors.Wrap(err, "Error while asserting ObjectID")
				return err
			}
		}
	}

	if m["userID"] != nil {
		u.UserID, err = uuuid.FromString(m["userID"].(string))
		if err != nil {
			err = errors.Wrap(err, "Error while asserting UserID")
			return err
		}
	}

	if m["email"] != nil {
		u.Email, assertOK = m["email"].(string)
		if !assertOK {
			return errors.New("Error while asserting Email")
		}
	}
	if m["firstName"] != nil {
		u.FirstName, assertOK = m["firstName"].(string)
		if !assertOK {
			return errors.New("Error while asserting FirstName")
		}
	}
	if m["lastName"] != nil {
		u.LastName, assertOK = m["lastName"].(string)
		if !assertOK {
			return errors.New("Error while asserting LastName")
		}
	}
	if m["username"] != nil {
		u.Username, assertOK = m["username"].(string)
		if !assertOK {
			return errors.New("Error while asserting Username")
		}
	}
	if m["password"] != nil {
		u.Password, assertOK = m["password"].(string)
		if !assertOK {
			return errors.New("Error while asserting Password")
		}
	}
	if m["roles"] != nil {
		roles := []string{}
		for _, r := range m["roles"].([]interface{}) {
			role, assertOK := r.(string)
			if !assertOK {
				return errors.New("Error while asserting Role")
			}
			roles = append(roles, role)
		}
		u.Roles = roles
	}

	return nil
}
