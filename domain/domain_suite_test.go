package domain

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"

	"github.com/TerrexTech/agg-userauth-cmd/util"
	"github.com/TerrexTech/agg-userauth-model/user"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestEvent tests Event-handling.
func TestEvent(t *testing.T) {
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
	RunSpecs(t, "EventHandler Suite")
}

var _ = Describe("EventHandler", func() {
	var (
		coll *mongo.Collection
	)

	BeforeSuite(func() {
		mc, err := util.LoadMongoConfig()
		Expect(err).ToNot(HaveOccurred())
		coll = mc.AggCollection
	})

	Describe("UserDeleted", func() {
		It("should delete user", func() {
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
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				Action:        "UserDeleted",
				AggregateID:   1,
				CorrelationID: cid,
				Data:          marshalUser,
				NanoTime:      time.Now().UTC().UnixNano(),
				Source:        "test-source",
				UserUUID:      uid,
				UUID:          uuid,
				Version:       1,
				YearBucket:    2018,
			}

			err = userDeleted(coll, mockEvent)
			Expect(err).ToNot(HaveOccurred())

			_, err = coll.FindOne(mockUser)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("UserRegistered", func() {
		It("should delete user", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := user.User{
				UserID:    uid.String(),
				Email:     "test-email",
				FirstName: "test-firstName",
				LastName:  "test-lastName",
				UserName:  uid.String(),
				Password:  "test-password",
				Role:      "test-role",
			}

			marshalUser, err := json.Marshal(mockUser)
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				Action:        "UserRegistered",
				AggregateID:   1,
				CorrelationID: cid,
				Data:          marshalUser,
				NanoTime:      time.Now().UTC().UnixNano(),
				Source:        "test-source",
				UserUUID:      uid,
				UUID:          uuid,
				Version:       1,
				YearBucket:    2018,
			}

			err = userRegistered(coll, mockEvent)
			Expect(err).ToNot(HaveOccurred())

			result, err := coll.FindOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			findUser, assertOK := result.(*user.User)
			Expect(assertOK).To(BeTrue())

			Expect(mockUser.UserID).To(Equal(findUser.UserID))
			Expect(mockUser.Email).To(Equal(findUser.Email))
			Expect(mockUser.FirstName).To(Equal(findUser.FirstName))
			Expect(mockUser.LastName).To(Equal(findUser.LastName))
			Expect(mockUser.UserName).To(Equal(findUser.UserName))
			Expect(mockUser.Password).To(Equal(findUser.Password))
			Expect(mockUser.Role).To(Equal(findUser.Role))
		})
	})

	Describe("UserUpdated", func() {
		It("should delete user", func() {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := user.User{
				UserID:   uid.String(),
				UserName: uid.String(),
			}
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			fName, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			params := &updateParams{
				Filter: map[string]interface{}{
					"userID": uid.String(),
				},
				Update: map[string]interface{}{
					"firstName": fName.String(),
				},
			}

			marshalParams, err := json.Marshal(params)
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				Action:        "UserUpdated",
				AggregateID:   1,
				CorrelationID: cid,
				Data:          marshalParams,
				NanoTime:      time.Now().UTC().UnixNano(),
				Source:        "test-source",
				UserUUID:      uid,
				UUID:          uuid,
				Version:       1,
				YearBucket:    2018,
			}

			err = userUpdated(coll, mockEvent)
			Expect(err).ToNot(HaveOccurred())

			result, err := coll.FindOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			findUser, assertOK := result.(*user.User)
			Expect(assertOK).To(BeTrue())
			Expect(findUser.FirstName).To(Equal(fName.String()))
		})
	})
})
