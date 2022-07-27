package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/tora0091/stockmonthlydata/config"
)

type TargetDate struct {
	Year  int
	Month int
}

type Ticker struct {
	Symbol string  `json:"symble"` // シンボル
	Bid    float64 `json:"bid"`    // 購入単価
	Value  float64 `json:"value"`  // 現在の値
	Hold   int     `json:"hold"`   // 保有数
}

type Result struct {
	CreatedAt string   `json:"created_at"`
	Body      []Ticker `json:"body"`
}

func main() {
	lambda.Start(Handler)
}

func Handler() {
	// 当月を対象
	targetDate := []TargetDate{{Year: time.Now().Year(), Month: int(time.Now().Month())}}
	if !config.OnlyThisMonth() {
		// 対象が当月でない場合は全期間が対象
		targetDate = GetTargetDateList()
	}

	targetPaths := CreateTargetPath(targetDate)
	if err := StoreStockDataFromS3(targetPaths); err != nil {
		log.Fatalln(err)
	}
}

// GetStockDataFromAws, Aws S3よりjsonデータを取得、DynamoDBに格納
func StoreStockDataFromS3(targetPaths []string) error {
	for _, targetPath := range targetPaths {
		// fmt.Println(targetPath)
		data, err := GetFileDataFromS3(config.GetS3Bucket(), targetPath)
		if err != nil {
			return err
		}

		var result Result
		if err := json.Unmarshal(data, &result); err != nil {
			return err
		}

		if err := StoreDataForDynamo(data, result); err != nil {
			return err
		}
	}
	return nil
}

// StoreDataForDynamo, DynamoDB にデータを格納
func StoreDataForDynamo(data []byte, result Result) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(endpoints.ApNortheast1RegionID),
	})
	if err != nil {
		return err
	}

	ddb := dynamodb.New(sess)
	param := &dynamodb.PutItemInput{
		TableName: aws.String(config.GetDatabaseName()),
		Item: map[string]*dynamodb.AttributeValue{
			"date": {
				S: aws.String(result.CreatedAt),
			},
			"body": {
				S: aws.String(string(data)),
			},
		},
	}

	_, err = ddb.PutItem(param)
	if err != nil {
		return err
	}
	return nil
}

// GetTargetDateList, 対象となる日付を取得
func GetTargetDateList() []TargetDate {
	start := time.Date(2021, 6, 1, 0, 0, 0, 0, time.Local)
	end := time.Now()

	targetDateList := []TargetDate{}

	for {
		if start.Before(end) {
			targetDateList = append(targetDateList, TargetDate{
				Year:  start.Year(),
				Month: int(start.Month()),
			})
			start = start.AddDate(0, 1, 0)
		} else {
			break
		}
	}
	return targetDateList
}

// CreateTargetPath, S3へのPathを作成
func CreateTargetPath(targetDate []TargetDate) []string {
	paths := []string{}
	for _, target := range targetDate {
		path := fmt.Sprintf(config.GetS3PathFormat(), target.Year, target.Month)
		paths = append(paths, path)
	}
	return paths
}

// GetFileDataFromS3, S3からデータを取得
func GetFileDataFromS3(bucket, path string) ([]byte, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(endpoints.ApNortheast1RegionID),
	})
	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)
	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(obj.Body)

	return buf.Bytes(), nil
}
