package goidgen

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/speps/go-hashids"
)

const entityType = "IDGENERATOR"

type IDGenerator struct {
	dynamoClient dynamodbiface.ClientAPI
	table        string
	withSK       bool
	name         string
	hashID       *hashids.HashID
}

type idGeneratorItem struct {
	PK         string `dynamodbav:"PK"`
	SK         string `dynamodbav:"SK,omitempty"`
	Type       string `dynamodbav:"TYPE"`
	Name       string `dynamodbav:"name"`
	LastNumber int    `dynamodbav:"lastNumber"`
}

func New(ctx context.Context, dynamoClient dynamodbiface.ClientAPI, table string, withSK bool, name string) (*IDGenerator, error) {
	// create *IDGenerator
	h, err := newHashid(name)
	if err != nil {
		return nil, err
	}
	gen := &IDGenerator{
		dynamoClient: dynamoClient,
		table:        table,
		withSK:       withSK,
		name:         name,
		hashID:       h,
	}
	// save it in Dynamo
	pk := pk(name)
	var sk string
	if withSK {
		sk = pk
	}
	item, err := dynamodbattribute.MarshalMap(idGeneratorItem{
		PK:   pk,
		SK:   sk,
		Type: entityType,
		Name: name,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Dynamo item: %s", err)
	}
	request := dynamoClient.PutItemRequest(&dynamodb.PutItemInput{
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
		Item:                item,
		TableName:           aws.String(table),
	})
	_, err = request.Send(ctx)
	if err != nil {
		awsErr, ok := err.(awserr.Error)
		if ok && awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return nil, fmt.Errorf("an IDGenerator named %s already exists", name)
		}
		return nil, fmt.Errorf("error saving generator: %s", err)
	}
	return gen, nil
}

var ErrNotFound = errors.New("not found")

func Open(ctx context.Context, dynamoClient dynamodbiface.ClientAPI, table string, withSK bool, name string) (*IDGenerator, error) {
	// get from Dynamo
	key := key(name, withSK)
	request := dynamoClient.GetItemRequest(&dynamodb.GetItemInput{
		Key:                    key,
		ReturnConsumedCapacity: dynamodb.ReturnConsumedCapacityNone,
		TableName:              aws.String(table),
	})
	response, err := request.Send(ctx)
	if err != nil {
		return nil, err
	}
	if len(response.Item) == 0 {
		return nil, ErrNotFound
	}
	var item idGeneratorItem
	err = dynamodbattribute.UnmarshalMap(response.Item, &item)
	if err != nil {
		return nil, err
	}
	// create and return *IDGenerator
	h, err := newHashid(name)
	if err != nil {
		return nil, err
	}
	idGen := &IDGenerator{
		dynamoClient: dynamoClient,
		table:        table,
		withSK:       withSK,
		name:         name,
		hashID:       h,
	}
	return idGen, nil
}

// Next returns the next ID
func (ig *IDGenerator) Next(ctx context.Context) (string, error) {
	key := key(ig.name, ig.withSK)
	request := ig.dynamoClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]string{"#n": "lastNumber"},
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":zero": {N: aws.String("0")},
			":inc":  {N: aws.String("1")},
		},
		Key:                         key,
		ReturnConsumedCapacity:      dynamodb.ReturnConsumedCapacityNone,
		ReturnItemCollectionMetrics: dynamodb.ReturnItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValueAllNew,
		TableName:                   aws.String(ig.table),
		UpdateExpression:            aws.String("SET #n = if_not_exists(#n, :zero) + :inc"), // now lastNumber holds the next number
	})
	response, err := request.Send(ctx)
	if err != nil {
		return "", err
	}
	// if the attribute type/name doesn't exists, it means we added a new generator...
	_, ok := response.Attributes["TYPE"]
	if !ok { // so, we add the type and name attribute to complete the item
		request = ig.dynamoClient.UpdateItemRequest(&dynamodb.UpdateItemInput{
			ExpressionAttributeNames: map[string]string{
				"#t": "TYPE",
				"#n": "name",
			},
			ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
				":t": {S: aws.String(entityType)},
				":n": {S: aws.String(ig.name)},
			},
			Key:                         key,
			ReturnConsumedCapacity:      dynamodb.ReturnConsumedCapacityNone,
			ReturnItemCollectionMetrics: dynamodb.ReturnItemCollectionMetricsNone,
			ReturnValues:                dynamodb.ReturnValueNone,
			TableName:                   aws.String(ig.table),
			UpdateExpression:            aws.String("SET #t=:t, #n=:n"),
		})
		_, err = request.Send(ctx)
		if err != nil {
			return "", fmt.Errorf("error setting type and name in new id generator: %w", err)
		}
	}
	// get lastNumber and return id
	lastNumberValue, ok := response.Attributes["lastNumber"]
	if !ok {
		return "", fmt.Errorf("missing lastNumber attribute from response")
	}
	if lastNumberValue.N == nil {
		return "", fmt.Errorf("invalid lastNumber attribute from response, should be a Number")
	}
	lastNumber, err := strconv.Atoi(*lastNumberValue.N)
	if err != nil {
		return "", fmt.Errorf("invalid lastNumber attribute value from response: %w", err)
	}
	id, err := ig.hashID.Encode([]int{lastNumber})
	if err != nil {
		return "", err
	}
	return id, nil
}

func pk(name string) string {
	return entityType + "#" + name
}

func key(name string, withSK bool) map[string]dynamodb.AttributeValue {
	pk := pk(name)
	key := map[string]dynamodb.AttributeValue{
		"PK": {S: aws.String(pk)},
	}
	if withSK {
		key["SK"] = dynamodb.AttributeValue{S: aws.String(pk)}
	}
	return key
}

func newHashid(name string) (*hashids.HashID, error) {
	hd := hashids.NewData()
	hd.Salt = name
	hd.MinLength = 2
	h, err := hashids.NewWithData(hd)
	if err != nil {
		return nil, err
	}
	return h, nil
}
