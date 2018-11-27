package test

import (
	"context"
	"encoding/json"
	"os"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/agg-userauth-cmd/util"
	"github.com/TerrexTech/agg-userauth-model/user"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/crypto/bcrypt"
)

var _ = Describe("EventTest", func() {
	var (
		coll *mongo.Collection

		reqTopic   string
		eventTopic string

		producer   *kafka.Producer
		consConfig *kafka.ConsumerConfig
	)

	BeforeEach(func() {
		var _ = coll
		mc, err := util.LoadMongoConfig()
		Expect(err).ToNot(HaveOccurred())
		coll = mc.AggCollection

		kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
		kafkaBrokers := *commonutil.ParseHosts(kafkaBrokersStr)

		producer, err = kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		})
		Expect(err).ToNot(HaveOccurred())

		eventTopic = "event.rns_eventstore.events"
		reqTopic = os.Getenv("KAFKA_CONSUMER_TOPIC_REQUEST")
		consConfig = &kafka.ConsumerConfig{
			KafkaBrokers: kafkaBrokers,
			Topics:       []string{eventTopic},
			GroupName:    "test.group.1",
		}
	})

	Describe("RegisterUser", func() {
		It("should create RegisterUser response", func(done Done) {
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

			mockCmd := createCmd(producer.Input(), "RegisterUser", reqTopic, "test-topic", marshalUser)

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				event := &model.Event{}
				err := json.Unmarshal(msg.Value, event)
				Expect(err).ToNot(HaveOccurred())

				if event.CorrelationID == mockCmd.UUID {
					userModel := &user.User{}
					err = json.Unmarshal(event.Data, userModel)
					Expect(err).ToNot(HaveOccurred())

					if userModel.UserID == mockUser.UserID {
						err = bcrypt.CompareHashAndPassword([]byte(userModel.Password), []byte(mockUser.Password))
						Expect(err).ToNot(HaveOccurred())
						userModel.Password = ""
						mockUser.Password = ""
						Expect(*userModel).To(Equal(mockUser))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 15)
	})

	Describe("DeleteUser", func() {
		It("should create DeleteUser response", func(done Done) {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockUser := &user.User{
				UserID:    uid.String(),
				Email:     "test-email",
				FirstName: "test-firstName",
				LastName:  "test-lastName",
				UserName:  uid.String(),
				Password:  "test-password",
				Role:      "test-role",
			}
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			marshalUser, err := json.Marshal(mockUser)
			Expect(err).ToNot(HaveOccurred())

			mockCmd := createCmd(producer.Input(), "DeleteUser", reqTopic, "test-topic", marshalUser)

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				event := &model.Event{}
				err := json.Unmarshal(msg.Value, event)
				Expect(err).ToNot(HaveOccurred())

				if event.CorrelationID == mockCmd.UUID {
					delResult := &user.User{}
					err = json.Unmarshal(event.Data, delResult)
					Expect(err).ToNot(HaveOccurred())

					Expect(mockUser).To(Equal(delResult))
					return true
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 15)
	})

	Describe("UpdateUser", func() {
		It("should create UpdateUser response", func(done Done) {
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
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			fName, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			params := map[string]interface{}{
				"filter": &user.User{
					UserID: uid.String(),
				},
				"update": &user.User{
					FirstName: fName.String(),
				},
			}
			marshalParams, err := json.Marshal(params)
			Expect(err).ToNot(HaveOccurred())

			mockCmd := createCmd(producer.Input(), "UpdateUser", reqTopic, "test-topic", marshalParams)

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				event := &model.Event{}
				err := json.Unmarshal(msg.Value, event)
				Expect(err).ToNot(HaveOccurred())

				if event.CorrelationID == mockCmd.UUID {
					updateResult := map[string]interface{}{}
					err = json.Unmarshal(event.Data, &updateResult)
					Expect(err).ToNot(HaveOccurred())

					updatedUser, assertOK := updateResult["update"].(map[string]interface{})

					Expect(assertOK).To(BeTrue())
					Expect(updatedUser).To(HaveKeyWithValue("userID", mockUser.UserID))
					Expect(updatedUser).To(HaveKeyWithValue("email", mockUser.Email))
					Expect(updatedUser).To(HaveKeyWithValue("firstName", fName.String()))
					Expect(updatedUser).To(HaveKeyWithValue("lastName", mockUser.LastName))
					Expect(updatedUser).To(HaveKeyWithValue("userName", mockUser.UserName))
					Expect(updatedUser).To(HaveKeyWithValue("password", mockUser.Password))
					Expect(updatedUser).To(HaveKeyWithValue("role", mockUser.Role))

					filter, assertOK := updateResult["filter"].(map[string]interface{})
					Expect(assertOK).To(BeTrue())
					Expect(filter).To(HaveKeyWithValue("userID", uid.String()))

					return true
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 15)
	})
})
