package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

var (
	topicARN = "arn:aws:sns:us-east-2:161142984839:contactsFredy"
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
	users := make([]User, len(e.Records))
	for index, record := range e.Records {
		fmt.Printf("Processing request data for event ID %s, type %s.\n", record.EventID, record.EventName)
		userGot := UnmarshalDataToUserStruct(record.Change.NewImage)
		users[index] = userGot
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
