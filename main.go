package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

const (
	insertType = "INSERT"
)

var (
	topicARN = "arn:aws:sns:us-east-2:161142984839:contactsDynamoFredy"
)

type (
	User struct {
		ID        string `dynamodbav:"id" json:"id"`
		Status    string `dynamodbav:"status" json:"status"`
		FirstName string `json:"firstName"`
		LastName  string `json:"lastName"`
	}
)

func main() {
	lambda.Start(handleRequest)
}

func handleRequest(ctx context.Context, e events.DynamoDBEvent) error {
	users := make([]User, 0)
	for _, record := range e.Records {
		fmt.Printf("Processing request data for event ID %s, type %s.\n", record.EventID, record.EventName)
		if strings.EqualFold(record.EventName, insertType) {
			userGot := UnmarshalDataToUserStruct(record.Change.NewImage)
			users = append(users, userGot)
		}
	}

	if len(users) == 0 {
		log.Printf("no records were inserted, skipping publishing event")
		return nil
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(os.Getenv("AWS_REGION")),
		},
	}))

	svc := sns.New(sess)
	dataToSend, err := json.Marshal(users)
	if err != nil {
		log.Printf("error marshaling data: %v", err)
		return err
	}
	_, err = svc.Publish(&sns.PublishInput{
		Message:  aws.String(string(dataToSend)),
		TopicArn: aws.String(topicARN),
	})
	if err != nil {
		log.Printf("error publishing event: %v", err)
		return err
	}
	return nil
}

func UnmarshalDataToUserStruct(attributes map[string]events.DynamoDBAttributeValue) User {
	firstName := attributes["firstName"].String()
	lastName := attributes["lastName"].String()
	id := attributes["id"].String()
	status := attributes["status"].String()

	return User{
		ID:        id,
		Status:    status,
		FirstName: firstName,
		LastName:  lastName,
	}
}
