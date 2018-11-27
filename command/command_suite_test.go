package command

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"

	"github.com/TerrexTech/agg-userauth-cmd/util"
	"github.com/TerrexTech/agg-userauth-model/user"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestCommand tests Command-handling.
func TestCommand(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"SERVICE_NAME",

		"KAFKA_BROKERS",

		"KAFKA_CONSUMER_GROUP_REQUEST",
		"KAFKA_CONSUMER_TOPIC_REQUEST",

		"KAFKA_CONSUMER_GROUP_ESRESP",
		"KAFKA_CONSUMER_TOPIC_ESRESP",

		"KAFKA_PRODUCER_TOPIC_ESREQ",
		"KAFKA_PRODUCER_TOPIC_EVENTS",

		"KAFKA_END_OF_STREAM_TOKEN",

		"MONGO_HOSTS",
		"MONGO_USERNAME",
		"MONGO_PASSWORD",

		"MONGO_DATABASE",
		"MONGO_AGG_COLLECTION",
		"MONGO_META_COLLECTION",

		"MONGO_CONNECTION_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "CommandHandler Suite")
}

func testError(coll *mongo.Collection, action string, data []byte) {
	uuid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())
	cid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	mockCmd := &model.Command{
		Action:        action,
		CorrelationID: cid,
		Data:          data,
		ResponseTopic: "test-topic",
		Source:        "test-source",
		SourceTopic:   "test_source-topic",
		Timestamp:     time.Now().UTC().Unix(),
		TTLSec:        15,
		UUID:          uuid,
	}

	c := &cmdConfig{
		coll:        coll,
		serviceName: "test-svc",
		cmd:         mockCmd,
	}

	result, event, cmdErr := registerUser(c)
	Expect(result).To(BeNil())
	Expect(event).To(BeNil())
	Expect(cmdErr.Code).ToNot(BeZero())
	Expect(cmdErr.Message).ToNot(BeEmpty())
}

func testValid(coll *mongo.Collection, action string, data []byte) ([]byte, *model.Event) {
	uuid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())
	cid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	mockCmd := &model.Command{
		Action:        action,
		CorrelationID: cid,
		Data:          data,
		ResponseTopic: "test-topic",
		Source:        "test-source",
		SourceTopic:   "test_source-topic",
		Timestamp:     time.Now().UTC().Unix(),
		TTLSec:        15,
		UUID:          uuid,
	}

	c := &cmdConfig{
		coll:        coll,
		serviceName: "test-svc",
		cmd:         mockCmd,
	}

	var (
		result []byte
		event  *model.Event
		cmdErr *model.Error
	)
	switch action {
	case "RegisterUser":
		result, event, cmdErr = registerUser(c)
	case "DeleteUser":
		result, event, cmdErr = deleteUser(c)
	case "UpdateUser":
		result, event, cmdErr = updateUser(c)
	}

	Expect(cmdErr).To(BeNil())
	Expect(event.CorrelationID).To(Equal(mockCmd.UUID))

	return result, event
}

var _ = Describe("CommanHandler", func() {
	var (
		coll *mongo.Collection
	)

	BeforeSuite(func() {
		mc, err := util.LoadMongoConfig()
		Expect(err).ToNot(HaveOccurred())
		coll = mc.AggCollection
	})

	Describe("DeleteUser", func() {
		It("should return error if user is not found", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			userModel := user.User{
				UserID:   uid.String(),
				UserName: "test-name",
			}
			marshalUser, err := json.Marshal(userModel)
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "DeleteUser", marshalUser)
		})

		It("should return UserDeleted event", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := user.User{
				UserID:   uid.String(),
				UserName: uid.String(),
			}
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			marshalUser, err := json.Marshal(mockUser)
			Expect(err).ToNot(HaveOccurred())

			result, event := testValid(coll, "DeleteUser", marshalUser)

			delResult := &deleteResult{}

			err = json.Unmarshal(result, delResult)
			Expect(err).ToNot(HaveOccurred())
			Expect(delResult.MatchedCount).To(BeNumerically(">", 0))
			err = json.Unmarshal(event.Data, delResult)
			Expect(err).ToNot(HaveOccurred())
			Expect(delResult.MatchedCount).To(BeNumerically(">", 0))
		})
	})

	Describe("RegisterUser", func() {
		Describe("Validations", func() {
			var userModel *user.User

			BeforeEach(func() {
				uid, err := uuuid.NewV4()
				Expect(err).ToNot(HaveOccurred())
				userModel = &user.User{
					UserID:    uid.String(),
					Email:     "test-email",
					FirstName: "test-fname",
					LastName:  "test-lname",
					UserName:  "test-uname",
					Password:  "test-pass",
					Role:      "test-role",
				}
			})

			It("should create UserID if it is blank", func() {
				userModel.UserID = ""
				marshalUser, err := json.Marshal(userModel)
				Expect(err).ToNot(HaveOccurred())

				result, event := testValid(coll, "RegisterUser", marshalUser)

				userModel := &user.User{}
				err = json.Unmarshal(result, userModel)
				Expect(err).ToNot(HaveOccurred())
				Expect(userModel.UserID).ToNot(BeEmpty())

				userModel = &user.User{}
				err = json.Unmarshal(event.Data, userModel)
				Expect(err).ToNot(HaveOccurred())
				Expect(userModel.UserID).ToNot(BeEmpty())
			})

			It("should return error if Email is blank", func() {
				userModel.Email = ""
				marshalUser, err := json.Marshal(userModel)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "RegisterUser", marshalUser)
			})

			It("should return error if FirstName is blank", func() {
				userModel.FirstName = ""
				marshalUser, err := json.Marshal(userModel)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "RegisterUser", marshalUser)
			})

			It("should return error if UserName is blank", func() {
				userModel.UserName = ""
				marshalUser, err := json.Marshal(userModel)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "RegisterUser", marshalUser)
			})

			It("should return error if Password is blank", func() {
				userModel.Password = ""
				marshalUser, err := json.Marshal(userModel)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "RegisterUser", marshalUser)
			})

			It("should return error if Role is blank", func() {
				userModel.Role = ""
				marshalUser, err := json.Marshal(userModel)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "RegisterUser", marshalUser)
			})
		})

		It("should return error if username or userID already exists", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := user.User{
				UserID:   uid.String(),
				UserName: uid.String(),
			}
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			marshalUser, err := json.Marshal(mockUser)
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "RegisterUser", marshalUser)
		})

		It("should return UserRegistered event on valid params", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := user.User{
				UserName:  uid.String(),
				FirstName: "test-name",
				LastName:  "test-lname",
				Email:     "test-email",
				Password:  "test-password",
				Role:      "test-role",
			}
			marshalUser, err := json.Marshal(mockUser)
			Expect(err).ToNot(HaveOccurred())

			result, event := testValid(coll, "RegisterUser", marshalUser)

			regUser := &user.User{}
			err = json.Unmarshal(result, regUser)
			Expect(err).ToNot(HaveOccurred())
			err = bcrypt.CompareHashAndPassword([]byte(regUser.Password), []byte(mockUser.Password))
			Expect(err).ToNot(HaveOccurred())
			Expect(regUser.UserID).ToNot(BeEmpty())
			Expect(mockUser.UserName).To(Equal(regUser.UserName))
			Expect(mockUser.FirstName).To(Equal(regUser.FirstName))
			Expect(mockUser.LastName).To(Equal(regUser.LastName))
			Expect(mockUser.Email).To(Equal(regUser.Email))
			Expect(mockUser.Role).To(Equal(regUser.Role))

			regUser = &user.User{}
			err = json.Unmarshal(event.Data, regUser)
			Expect(err).ToNot(HaveOccurred())
			err = bcrypt.CompareHashAndPassword([]byte(regUser.Password), []byte(mockUser.Password))
			Expect(err).ToNot(HaveOccurred())
			Expect(regUser.UserID).ToNot(BeEmpty())
			Expect(mockUser.UserName).To(Equal(regUser.UserName))
			Expect(mockUser.FirstName).To(Equal(regUser.FirstName))
			Expect(mockUser.LastName).To(Equal(regUser.LastName))
			Expect(mockUser.Email).To(Equal(regUser.Email))
			Expect(mockUser.Role).To(Equal(regUser.Role))
		})
	})

	Describe("Update", func() {
		It("should return error if Filter is nil", func() {
			params, err := json.Marshal(updateParams{
				Update: &user.User{
					UserName: "test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateUser", params)
		})

		It("should return error if Update is nil", func() {
			params, err := json.Marshal(updateParams{
				Filter: &user.User{
					UserName: "test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateUser", params)
		})

		It("should return error if UserID is attempted to be changed", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := user.User{
				UserID:   uid.String(),
				UserName: uid.String(),
			}
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			params, err := json.Marshal(updateParams{
				Filter: &user.User{
					UserName: uid.String(),
				},
				Update: &user.User{
					UserID: "test-id",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateUser", params)
		})

		It("should return error if UserName is attempted to be changed", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := user.User{
				UserID:   uid.String(),
				UserName: uid.String(),
			}
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			params, err := json.Marshal(updateParams{
				Filter: &user.User{
					UserName: uid.String(),
				},
				Update: &user.User{
					UserName: "test-name",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateUser", params)
		})

		It("should return error if Password is attempted to be set to blank", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := user.User{
				UserID:   uid.String(),
				UserName: uid.String(),
			}
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			params, err := json.Marshal(updateParams{
				Filter: &user.User{
					UserName: uid.String(),
				},
				Update: &user.User{
					UserName: "test-name",
					Password: "",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateUser", params)
		})

		It("should return error if Filter returns no users", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			params, err := json.Marshal(updateParams{
				Filter: &user.User{
					UserName: uid.String(),
				},
				Update: &user.User{
					FirstName: "test-name",
					Password:  "",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateUser", params)
		})

		It("should return UserUpdated event on valid params", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := user.User{
				UserID:   uid.String(),
				UserName: uid.String(),
			}
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			params, err := json.Marshal(updateParams{
				Filter: &user.User{
					UserName: uid.String(),
				},
				Update: &user.User{
					FirstName: "test-name",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			result, event := testValid(coll, "UpdateUser", params)
			upResult := map[string]interface{}{}

			var _ = event
			err = json.Unmarshal(result, &upResult)
			Expect(err).ToNot(HaveOccurred())

			updatedUser, assertOK := upResult["update"].(map[string]interface{})
			Expect(assertOK).To(BeTrue())

			Expect(updatedUser).To(HaveKeyWithValue("userID", mockUser.UserID))
			Expect(updatedUser).To(HaveKeyWithValue("userName", mockUser.UserName))
			Expect(updatedUser).To(HaveKeyWithValue("firstName", "test-name"))

			filter, assertOK := upResult["filter"].(map[string]interface{})
			Expect(assertOK).To(BeTrue())
			Expect(filter).To(HaveKeyWithValue("userName", uid.String()))
		})
	})
})
