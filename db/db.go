package db

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	//AwsRegion name of the region
	AwsRegion = "eu-west-1"
)

var db = dynamodb.New(session.New(), aws.NewConfig().WithRegion(AwsRegion))

func QueryTableByParams(params *dynamodb.QueryInput) ([]map[string]*dynamodb.AttributeValue, error) {
	result, err := db.Query(params)

	if err != nil {
		return nil, err
	}
	if *result.Count <= 0 {
		return nil, nil
	}

	return result.Items, nil
}

func QueryTableByPk(tableName string, pkKey string, pk string) ([]map[string]*dynamodb.AttributeValue, error) {
	fmt.Println("QueryByPK: " + tableName + " - Key: " + pkKey + " Value: " + pk)
	params := &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("#pk = :pkValue"),
		ExpressionAttributeNames: map[string]*string{
			"#pk": aws.String(pkKey),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pkValue": {
				S: aws.String(pk),
			},
		},
	}
	return QueryTableByParams(params)
}

func QueryTableByPkSk(tableName string, pkKey string, pk string, skKey string, sk string) ([]map[string]*dynamodb.AttributeValue, error) {
	fmt.Println("QueryByPKSk: " + tableName + " - Key: " + pkKey + " Value: " + pk + " & SK: " + skKey + " Value: " + sk)
	params := &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("#pk = :pkValue AND begins_with(#sk, :skValue)"),
		ExpressionAttributeNames: map[string]*string{
			"#pk": aws.String(pkKey),
			"#sk": aws.String(skKey),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pkValue": {
				S: aws.String(pk),
			},
			":skValue": {
				S: aws.String(sk),
			},
		},
	}
	return QueryTableByParams(params)
}

func QueryTableByGSISk(tableName string, indexName string, skKey string, sk string) ([]map[string]*dynamodb.AttributeValue, error) {
	fmt.Println("QueryByPKSk: " + tableName + " - Key: " + skKey + " Value: " + sk)
	params := &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String(indexName),
		KeyConditionExpression: aws.String("#sk = :skValue"),
		ExpressionAttributeNames: map[string]*string{
			"#sk": aws.String(skKey),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":skValue": {
				S: aws.String(sk),
			},
		},
	}
	return QueryTableByParams(params)
}

func QueryTableByGSISkData(tableName string, indexName string, skKey string, sk string, dataKey string, data string) ([]map[string]*dynamodb.AttributeValue, error) {
	fmt.Println("QueryByPKSk: " + tableName + " - Key: " + skKey + " Value: " + sk + " - Data: " + dataKey + " Value: " + data)
	params := &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String(indexName),
		KeyConditionExpression: aws.String("#sk = :skValue AND begins_with(#data, :dataValue)"),
		ExpressionAttributeNames: map[string]*string{
			"#sk":   aws.String(skKey),
			"#data": aws.String("data"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":skValue": {
				S: aws.String(sk),
			},
			":dataValue": {
				S: aws.String(data),
			},
		},
	}
	return QueryTableByParams(params)
}

func QueryTableByGSISkDateRange(tableName string, indexName string, skKey string, sk string, dataKey string, dateStart string, , dateEnd string) ([]map[string]*dynamodb.AttributeValue, error) {
	fmt.Println("QueryByPKSk: " + tableName + " - Key: " + skKey + " Value: " + sk + " - Data: " + dataKey + " DateStart: " + dateStart + " DateEnd: " + dateEnd)
	params := &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String(indexName),
		KeyConditionExpression: aws.String("#sk = :skValue AND #data BETWEEN :dateStartValue AND :dateEndValue"),
		ExpressionAttributeNames: map[string]*string{
			"#sk":   aws.String(skKey),
			"#data": aws.String("data"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":skValue": {
				S: aws.String(sk),
			},
			":dateStartValue": {
				S: aws.String(dateStart),
			},
			":dateEndValue": {
				S: aws.String(dateEnd),
			},
		},
	}
	return QueryTableByParams(params)
}

func PutItem(tableName string, item interface{}) error {
	// func putItem(tableName string, pkKey string, pk string, skKey string, sk string, dataKey string, data string, item interface{}) error {
	// input := &dynamodb.PutItemInput{
	// 	TableName: aws.String(tableName),
	// 	Item: map[string]*dynamodb.AttributeValue{
	// 		pkKey: {
	// 			S: aws.String(pk),
	// 		},
	// 		skKey: {
	// 			S: aws.String(sk),
	// 		},
	// 		dataKey: {
	// 			S: aws.String(data),
	// 		},
	// 	},
	// }
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		fmt.Println("Got error marshalling item:")
		fmt.Println(item)
		fmt.Println(err.Error())
		return err
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      av,
	}
	_, err = db.PutItem(input)
	return err
}

func TransactWriteItems(tableName string, items []interface{}) error {
	inputs := make([]*dynamodb.TransactWriteItem, len(items))
	for i, item := range items {

		av, err := dynamodbattribute.MarshalMap(item)
		if err != nil {
			fmt.Println("Got error marshalling item: ")
			fmt.Println(item)
			fmt.Println(err.Error())
			return err
		}

		inputs[i] = &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				TableName: aws.String(tableName),
				Item:      av,
			},
		}
	}

	_, err := db.TransactWriteItems(&dynamodb.TransactWriteItemsInput{
		TransactItems: inputs,
	})

	return err
}
