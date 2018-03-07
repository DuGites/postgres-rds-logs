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
	format     = "2006-01-02 15:04:05.999999999 -0700 MST"
	nameFormat = "2006-01-02-15"
)

/*
	When a new DB row is added make sure to set the DBName  to the Rds Instance name.
	The DBLogFile to "NULL", The Marker to 0 and the hour to -1.

	Completed Log file DynamoDb
	{
	  "db_logfile": "error/postgresql.log.2018-04-27-21",
	  "db_name": "goon-rds-demo",
	  "hour": "2018-04-27-21 EST",
	  "marker": "COMPLETED"
	}
	This is a new Rds DB "goon-rds-demo" added to be streamed.
	{
	  "db_logfile": "NULL",
	  "db_name": "goon-rds-demo",
	  "hour": "-1",
	  "marker": "0"
	}
*/

//DBLog ...
type DBLog struct {
	DbName    string `dynamodbav:"db_name"`
	DbLogfile string `dynamodbav:"db_logfile"`
	Marker    string `dynamodbav:"marker"`
	Hour      string `dynamodbav:"streamed_timestamp"`
}

var (
	numberOfLines = aws.Int64(10)
	awsSession    = session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	dySvc         = dynamodb.New(awsSession)
)

func updateDbLogFile(dbname string, logfile string, marker string, streamedTimeStamp string) {

	key := map[string]*dynamodb.AttributeValue{
		"db_name": {
			S: aws.String(dbname),
		},
	}

	reqInput := &dynamodb.UpdateItemInput{
		TableName:        aws.String("postgres_db_logs"),
		Key:              key,
		UpdateExpression: aws.String("SET #LF = :l, #MK =:m, #HR =:h"),
		ExpressionAttributeNames: map[string]*string{
			"#LF": aws.String("db_logfile"),
			"#MK": aws.String("marker"),
			"#HR": aws.String("streamed_timestamp"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":l": {S: aws.String(logfile)},
			":m": {S: aws.String(marker)},
			":h": {S: aws.String(streamedTimeStamp)},
		},
		ReturnValues: aws.String("ALL_NEW"),
	}

	result, err := dySvc.UpdateItem(reqInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				log.Println(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				log.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				log.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				log.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				log.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
	}
	log.Println(result)
}

func streamLogTextTOCloudWatch(lines string) {

}

func tailDatabaseLogFile(dbname string, logfile string, marker string, hour string) {

	rdsClient := rds.New(awsSession)

	dbInfo := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(dbname),  // DB Instance
		LogFileName:          aws.String(logfile), // FileName with yyyy-mm-dd appended.
		Marker:               aws.String(marker),  // Starts at 0.
		NumberOfLines:        numberOfLines,       // How many lines to stream at a time.
	}

	result, err := rdsClient.DownloadDBLogFilePortion(dbInfo)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case rds.ErrCodeDBInstanceNotFoundFault:
				log.Println(rds.ErrCodeDBInstanceNotFoundFault, aerr.Error())
			case rds.ErrCodeDBLogFileNotFoundFault:
				log.Println(rds.ErrCodeDBLogFileNotFoundFault, aerr.Error())
				updateDbLogFile(dbname, logfile, "COMPLETED", hour)
			default:
				log.Println(aerr.Code, err)
			}
		} else {
			log.Println(err)
		}
		return
	}
	// We can write to CloudWatch so that the cloudwatcher forwader
	// can forward the logs to Sumo.
	fmt.Println(*result.LogFileData)

	// File has been completely streamed
	if !*result.AdditionalDataPending {
		updateDbLogFile(dbname, logfile, "COMPLETED", hour)
	} else {
		// dbInfo.SetMarker(*result.Marker)
		updateDbLogFile(dbname, logfile, *result.Marker, hour)
	}

}

func loadPartiallyStreamedDBS() ([]map[string]*dynamodb.AttributeValue, error) {

	req := &dynamodb.ScanInput{
		TableName: aws.String("postgres_db_logs"),
	}
	result, err := dySvc.Scan(req)
	if err != nil {
		return make([]map[string]*dynamodb.AttributeValue, 0), err
	}
	return result.Items, nil
}

func main() {

	startTime := time.Now()
	loc, _ := time.LoadLocation("EST")
	prevHour := time.Now().In(loc).Add(-1 * time.Hour)
	prevHourFile := prevHour.Format(nameFormat)

	pgFilePrefix := "error/postgresql.log."

	// New DB's need to be added to the state table.
	dbs, err := loadPartiallyStreamedDBS()
	if err != nil {
		log.Fatal("Dynamodb Scan Error: %s", err)
	}

	dblgs := []DBLog{}
	err = dynamodbattribute.UnmarshalListOfMaps(dbs, &dblgs)
	if err != nil {
		log.Fatal(err)
	}
	if len(dblgs) == 0 {
		log.Fatal("Configuration db has no databases loaded to capture logs.")
	}

	for _, db := range dblgs {
		elapsed := time.Since(startTime)
		if elapsed.Minutes() > 4.5 {
			return
		}

		fileName := db.DbLogfile
		if fileName == "NULL" {
			// you can get all logs files that are present instead of previous hour
			// fileName = fmt.Sprintf(pgFile, prevHourFile)
			fileName = pgFilePrefix + prevHourFile
		}

		// If db file is not completed then tail the file.
		if db.Marker != "COMPLETED" {
			log.Printf("Streaming %s File from Marker %s", fileName, db.Marker)
			// -1 denotes a db that is added for the first time. If the hour is -1
			// that means the Db has had no previous log files streamed, hence we
			// read the previous hour in this case. If the db has a log file streamed
			// previously then we take the hour in the state table.
			if db.Hour != "-1" {
				// this is a dblogfile  that has been processed previously
				tailDatabaseLogFile(db.DbName, fileName, db.Marker, db.Hour)
			} else {
				// This case is for a brand new DB added to be streamed.
				tailDatabaseLogFile(db.DbName, fileName, db.Marker, prevHour.String())
			}
			log.Println("If more DB's exist streaming next in line")
			continue // move to the next db after streaming one db.
		}
		// If the Db has a completed Marker then compute the next hour log file to be streamed based
		// on the hour stored in the db and not the current time.
		log.Println("Completed Currrent DBLog File. Moving onto next DBLogFile for Same DB Instance")
		timestamp, _ := time.Parse(format, db.Hour)
		nextHour := timestamp.In(loc).Add(1 * time.Hour)
		fmt.Println(timestamp, nextHour)

		// Only process older files since we would like to get completed files streamed.
		// nextHour.Hour() could be any hour before the current hour. Check if Unix time is
		// less than current time and if so check the hour of the day. If the lambda job starts
		// and the Marker is Completed it will try to tail the log file for the nexthour.
		if nextHour.Unix() < time.Now().In(loc).Unix() {
			if nextHour.Hour() < time.Now().In(loc).Hour() {
				fileName = pgFilePrefix + nextHour.Format(nameFormat)
				// Trying to stream next next hour log file. Setting Marker to 0
				tailDatabaseLogFile(db.DbName, fileName, "0", nextHour.String())
			}
		} else {
			log.Printf("Skipping Next Hour log file error/postgresql.log.%v\n", time.Now().In(loc).Format(nameFormat))
			log.Printf("%s log file has been completed\n", db.DbLogfile)
		}

	}

}

// func main() {
// 	// Make the handler available for Remote Procedure Call by AWS Lambda
// 	lambda.Start(streamer)
// }
