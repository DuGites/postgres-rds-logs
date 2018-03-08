package main

import (
	"fmt"
	"log"
	"time"

	// "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/rds"
)

const (
	region     = "us-east-1"
	format     = "2006-01-02-15 MST"
	nameFormat = "2006-01-02-15"
)

//DBLog ...
type DBLog struct {
	DbName    string `dynamodbav:"db_name"`
	DbLogfile string `dynamodbav:"db_logfile"`
	Marker    string `dynamodbav:"marker"`
	Hour      string `dynamodbav:"hour"`
}

var (
	numberOfLines = aws.Int64(10)
	awsSession    = session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	dySvc         = dynamodb.New(awsSession)
)

func updateDbLogFile(dbname string, logfile string, marker string, hour string) {
	key := map[string]*dynamodb.AttributeValue{
		"db_name": {S: aws.String(dbname)},
	}

	reqInput := &dynamodb.UpdateItemInput{
		TableName:        aws.String("postgres_db_logs"),
		Key:              key,
		UpdateExpression: aws.String("SET #LF = :l, #MK =:m, #HR =:h"),
		ExpressionAttributeNames: map[string]*string{
			"#LF": aws.String("db_logfile"),
			"#MK": aws.String("marker"),
			"#HR": aws.String("hour"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":l": {S: aws.String(logfile)},
			":m": {S: aws.String(marker)},
			":h": {S: aws.String(hour)},
		},
		ReturnValues: aws.String("ALL_NEW"),
	}

	result, err := dySvc.UpdateItem(reqInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			log.Println(aerr.Code(), err) // Does the error string not include the error code? seems like this entire if else block might be the same as `log.Println(err)`
		} else {
			log.Println(err) // Don't need to explicitly call err.Error() - it is assumed when you try to print an error
		}
		return
	}
	log.Println(result)
}

func streamLogTextTOCloudWatch(lines string) {

}

func tailDatabaseLogFile(dbname string, logfile string, marker string, hour string) {
	rdsClient := rds.New(awsSession)

	dbInfo := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(dbname),
		LogFileName:          aws.String(logfile),
		Marker:               aws.String(marker),
		NumberOfLines:        numberOfLines,
	}

	result, err := rdsClient.DownloadDBLogFilePortion(dbInfo)

	if err != nil {
		// Replace if else block with `log.Println(err)` ?
		if aerr, ok := err.(awserr.Error); ok {
			log.Println(aerr.Code(), err)
		} else {
			log.Println(err)
		}
		return
	}
	// We can write to CloudWatch
	fmt.Println(*result.LogFileData)

	// File has been completely streamed
	if !*result.AdditionalDataPending {
		updateDbLogFile(dbname, logfile, "COMPLETED", hour)
		return
	}

	// dbInfo.SetMarker(*result.Marker)
	updateDbLogFile(dbname, logfile, *result.Marker, hour)
	time.Sleep(2 * time.Second) // What is this sleep for? This appears to be unnecessarily burning lambda time.
}

func loadPartiallyStreamedDBS() ([]map[string]*dynamodb.AttributeValue, error) {
	req := &dynamodb.ScanInput{
		TableName: aws.String("postgres_db_logs"),
	}
	result, err := dySvc.Scan(req)
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func main() {
	startTime := time.Now()
	loc, _ := time.LoadLocation("EST")
	prevHourFile := time.Now().In(loc).Add(-1 * time.Hour).Format(nameFormat)
	prevHour := time.Now().In(loc).Add(-1 * time.Hour)

	pgFilePrefix := "error/postgresql.log."

	// New DB's need to be added to the state table.
	dbs, err := loadPartiallyStreamedDBS()
	if err != nil {
		log.Fatalf("Dynamodb Scan Error: %s", err)
	}

	var dblgs []DBLog
	err = dynamodbattribute.UnmarshalListOfMaps(dbs, &dblgs)
	if err != nil {
		log.Fatal(err)
	}
	if len(dblgs) == 0 {
		log.Fatal("Configuration db has no databases loaded to capture logs.")
	}

	for _, db := range dblgs {
		elapsed := time.Since(startTime)
		if elapsed.Minutes() > 4.5 { // Go idiom to limit indentation when possible for the happy path, which means generally avoiding 'else' blocks if possible and checking for the 'unhappy' path with if statements
			return
		}

		fileName := db.DbLogfile
		if db.DbLogfile == "NULL" {
			// you can get all logs files that are present instead of previous hour
			fileName = pgFilePrefix + prevHourFile
		}

		if db.Marker == "COMPLETED" {
			timestamp, _ := time.Parse(format, db.Hour)
			nextHour := timestamp.Add(1 * time.Hour)

			if nextHour.Hour() > time.Now().In(loc).Hour() {
				log.Printf("Skipping log file error/postgresql.log.%v\n", time.Now().In(loc).Format(nameFormat))
				log.Printf("%s log file has been completed\n", db.DbLogfile)
				return
			}

			fileName = pgFilePrefix + nextHour.Format(nameFormat)
			tailDatabaseLogFile(db.DbName, fileName, "0", nextHour.Format(format))
			return
		}

		log.Printf("Streaming %s File from Marker %s", fileName, db.Marker)
		if db.Hour != "-1" {
			tailDatabaseLogFile(db.DbName, fileName, db.Marker, db.Hour)
		} else {
			tailDatabaseLogFile(db.DbName, fileName, db.Marker, prevHour.Format(format))
		}
	}
}

// func main() {
// 	// Make the handler available for Remote Procedure Call by AWS Lambda
// 	lambda.Start(streamer)
// }
