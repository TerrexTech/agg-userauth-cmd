package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/agg-userauth-cmd/user"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/uuuid"
	"github.com/joho/godotenv"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func Byf(s string, args ...interface{}) {
	By(fmt.Sprintf(s, args...))
}

func TestUser(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_EVENT_GROUP",

		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",

		"KAFKA_PRODUCER_EVENT_TOPIC",
		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",
		"KAFKA_PRODUCER_RESPONSE_TOPIC",

		"MONGO_HOSTS",
		"MONGO_USERNAME",
		"MONGO_PASSWORD",
		"MONGO_DATABASE",
		"MONGO_CONNECTION_TIMEOUT_MS",
		"MONGO_RESOURCE_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "UserAuthAggregate Suite")
}

var _ = Describe("UserAuthAggregate", func() {
	var (
		kafkaBrokers          []string
		eventsTopic           string
		producerResponseTopic string

		mockUser  *user.User
		mockEvent *model.Event
	)
	BeforeSuite(func() {
		kafkaBrokers = *commonutil.ParseHosts(
			os.Getenv("KAFKA_BROKERS"),
		)
		eventsTopic = os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")
		producerResponseTopic = os.Getenv("KAFKA_PRODUCER_RESPONSE_TOPIC")

		userID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())

		mockUser = &user.User{
			UserID:    userID,
			FirstName: "test-first",
			LastName:  "test-last",
			Email:     "test-email",
			UserName:  "test-username",
			Password:  "test-password",
			Role:      "test-user",
		}
		marshalUser, err := json.Marshal(mockUser)
		Expect(err).ToNot(HaveOccurred())

		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		uid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		UUID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		mockEvent = &model.Event{
			EventAction:   "insert",
			CorrelationID: cid,
			AggregateID:   user.AggregateID,
			Data:          marshalUser,
			NanoTime:      time.Now().UnixNano(),
			UserUUID:      uid,
			UUID:          UUID,
			Version:       0,
			YearBucket:    2018,
		}
	})

	Describe("User Operations", func() {
		It("should insert record", func(done Done) {
			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "agguser.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.UUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.UUID))

					user := &user.User{}
					err = json.Unmarshal(kr.Result, user)
					Expect(err).ToNot(HaveOccurred())

					if user.UserID == mockUser.UserID {
						mockUser.ID = user.ID
						Expect(user).To(Equal(mockUser))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())
			findResult, err := aggColl.FindOne(mockUser)
			Expect(err).ToNot(HaveOccurred())
			findUser, assertOK := findResult.(*user.User)
			Expect(assertOK).To(BeTrue())
			Expect(findUser).To(Equal(mockUser))

			close(done)
		}, 20)

		It("should update record", func(done Done) {
			Byf("Creating update args")
			filterUser := map[string]interface{}{
				"userID": mockUser.UserID,
			}
			mockUser.FirstName = "new-firstname"
			mockUser.UserName = "new-password"
			// Remove ObjectID because this is not passed from gateway
			mockID := mockUser.ID
			mockUser.ID = objectid.NilObjectID
			update := map[string]interface{}{
				"filter": filterUser,
				"update": mockUser,
			}
			marshalUpdate, err := json.Marshal(update)
			Expect(err).ToNot(HaveOccurred())
			// Reassign back ID so we can compare easily with database-entry
			mockUser.ID = mockID

			Byf("Creating update MockEvent")
			UUID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.EventAction = "update"
			mockEvent.Data = marshalUpdate
			mockEvent.NanoTime = time.Now().UnixNano()
			mockEvent.UUID = UUID

			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "agguser.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.UUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.UUID))

					result := map[string]int{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					if result["matchedCount"] != 0 && result["modifiedCount"] != 0 {
						Expect(result["matchedCount"]).To(Equal(1))
						Expect(result["modifiedCount"]).To(Equal(1))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())
			findResult, err := aggColl.FindOne(mockUser)
			Expect(err).ToNot(HaveOccurred())
			findUser, assertOK := findResult.(*user.User)
			Expect(assertOK).To(BeTrue())
			Expect(findUser).To(Equal(mockUser))

			close(done)
		}, 20)

		It("should delete record", func(done Done) {
			Byf("Creating delete args")
			deleteArgs := map[string]interface{}{
				"userID": mockUser.UserID,
			}
			marshalDelete, err := json.Marshal(deleteArgs)
			Expect(err).ToNot(HaveOccurred())

			Byf("Creating delete MockEvent")
			UUID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.EventAction = "delete"
			mockEvent.Data = marshalDelete
			mockEvent.NanoTime = time.Now().UnixNano()
			mockEvent.UUID = UUID

			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "agguser.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.UUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.UUID))

					result := map[string]int{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					if result["deletedCount"] != 0 {
						Expect(result["deletedCount"]).To(Equal(1))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())
			_, err = aggColl.FindOne(mockUser)
			Expect(err).To(HaveOccurred())

			close(done)
		}, 20)
	})
})
