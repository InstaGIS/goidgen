package goidgen_test

import (
	"context"
	"flag"
	"log"
	"os"
	"testing"

	"github.com/InstaGIS/godynamodb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var (
	test          = &godynamodb.Test{}
	tableWithPK   = "TableUsingPKOnly"
	tableWithPKSK = "TableUsingPKAndSK"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		log.Printf("skipping integration tests")
		os.Exit(0)
	}
	os.Exit(test.TestMain(m, func(svc *dynamodb.Client) error {
		return createTables(svc)
	}))
}

func createTables(svc *dynamodb.Client) error {
	// create tableWithPK
	createRequest := svc.CreateTableRequest(&dynamodb.CreateTableInput{
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("PK"),
				AttributeType: "S",
			},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("PK"),
				KeyType:       dynamodb.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(tableWithPK),
	})
	_, err := createRequest.Send(context.Background())
	if err != nil {
		return err
	}
	// create tableWithPKSK
	createRequest = svc.CreateTableRequest(&dynamodb.CreateTableInput{
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("PK"),
				AttributeType: "S",
			},
			{
				AttributeName: aws.String("SK"),
				AttributeType: "S",
			},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("PK"),
				KeyType:       dynamodb.KeyTypeHash,
			},
			{
				AttributeName: aws.String("SK"),
				KeyType:       dynamodb.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(tableWithPKSK),
	})
	_, err = createRequest.Send(context.Background())
	if err != nil {
		return err
	}

	// test data
	testItems := []map[string]dynamodb.AttributeValue{
		{
			"PK":         {S: aws.String("ID_GENERATOR#UserFile")},
			"TYPE":       {S: aws.String("ID_GENERATOR")},
			"name":       {S: aws.String("UserFile")},
			"lastNumber": {N: aws.String("2")},
		},
	}
	for _, item := range testItems {
		request := svc.PutItemRequest(&dynamodb.PutItemInput{
			Item:      item,
			TableName: aws.String(tableWithPK),
		})
		_, err = request.Send(context.Background())
		if err != nil {
			return err
		}
		// add SK
		item["SK"] = item["PK"]
		request = svc.PutItemRequest(&dynamodb.PutItemInput{
			Item:      item,
			TableName: aws.String(tableWithPKSK),
		})
		_, err = request.Send(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}
