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
			"#HR": aws.String("hour"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":l": {
				S: aws.String(logfile),
			},
			":m": {
				S: aws.String(marker),
			},
			":h": {
				S: aws.String(hour),
			},
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
		DBInstanceIdentifier: aws.String(dbname),
		LogFileName:          aws.String(logfile),
		Marker:               aws.String(marker),
		NumberOfLines:        numberOfLines,
	}

	result, err := rdsClient.DownloadDBLogFilePortion(dbInfo)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case rds.ErrCodeDBInstanceNotFoundFault:
				log.Println(rds.ErrCodeDBInstanceNotFoundFault, aerr.Error())
			case rds.ErrCodeDBLogFileNotFoundFault:
				log.Println(rds.ErrCodeDBLogFileNotFoundFault, aerr.Error())
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
		return
	}
	// We can write to CloudWatch
	fmt.Println(*result.LogFileData)

	// File has been completely streamed
	if !*result.AdditionalDataPending {
		updateDbLogFile(dbname, logfile, "COMPLETED", hour)
	} else {
		// dbInfo.SetMarker(*result.Marker)
		updateDbLogFile(dbname, logfile, *result.Marker, hour)
		time.Sleep(2 * time.Second)

	}

}

func loadPartiallyStreamedDBS() ([]map[string]*dynamodb.AttributeValue, error) {

	req := &dynamodb.ScanInput{
		TableName: aws.String("postgres_db_logs"),
	}
	result, scanerr := dySvc.Scan(req)
	if scanerr != nil {
		return make([]map[string]*dynamodb.AttributeValue, 0), scanerr
	}
	return result.Items, nil
}

func main() {

	startTime := time.Now()
	loc, _ := time.LoadLocation("EST")
	prevHourFile := time.Now().In(loc).Add(-1 * time.Hour).Format(nameFormat)
	prevHour := time.Now().In(loc).Add(-1 * time.Hour)

	pgFile := "error/postgresql.log.%s"
	fileName := ""

	// New DB's need to be added to the state table.
	dbs, scanerr := loadPartiallyStreamedDBS()
	if scanerr != nil {
		log.Fatal("Dynamodb Scan Error: %s", scanerr)
	}

	dblgs := []DBLog{}
	unmrshlerr := dynamodbattribute.UnmarshalListOfMaps(dbs, &dblgs)
	if unmrshlerr != nil {
		unmrsherrstr := fmt.Errorf("%v", unmrshlerr)
		log.Fatal(unmrsherrstr)
	}
	if len(dblgs) == 0 {
		log.Fatal("Configuration db has no databases loaded to capture logs.")
	} else {
		for _, db := range dblgs {
			elapsed := time.Since(startTime)
			if elapsed.Minutes() < 4.5 {

				if db.DbLogfile == "NULL" {
					// you can get all logs files that are present instead of previous hour
					fileName = fmt.Sprintf(pgFile, prevHourFile)
				} else {
					fileName = db.DbLogfile
				}
				if db.Marker != "COMPLETED" {
					log.Printf("Streaming %s File from Marker %s", fileName, db.Marker)
					if db.Hour != "-1" {
						tailDatabaseLogFile(db.DbName, fileName, db.Marker, db.Hour)
					} else {
						tailDatabaseLogFile(db.DbName, fileName, db.Marker, prevHour.Format(format))
					}
				} else {
					timestamp, _ := time.Parse(format, db.Hour)
					nextHour := timestamp.Add(1 * time.Hour)

					if nextHour.Hour() < time.Now().In(loc).Hour() {
						fileName = fmt.Sprintf(pgFile, timestamp.Add(1*time.Hour).Format(nameFormat))
						tailDatabaseLogFile(db.DbName, fileName, "0", nextHour.Format(format))
					} else {
						log.Printf("Skipping log file error/postgresql.log.%v\n", time.Now().In(loc).Format(nameFormat))
						log.Printf("%s log file has been completed\n", db.DbLogfile)
					}

				}
			}

		}
	}

}

// func main() {
// 	// Make the handler available for Remote Procedure Call by AWS Lambda
// 	lambda.Start(streamer)
// }
