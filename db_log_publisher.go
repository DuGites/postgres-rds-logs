package main

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/rds"
)

/*
	When a new DB row is added make sure to set the DBName  to the Rds Instance name.
	The DBLogFile to "NULL", The Marker to 0 and the hour to -1.

	Completed Log file DynamoDb
	{
	  "db_logfile": "error/postgresql.log.2018-04-27-21",
	  "db_name": "goon-rds-demo",
	  "hour": "2018-04-27-21 EST",
	  "marker": "COMPLETED",
	  "seq_token": "RANDOM_NUMERIC_STRING",
	}
	This is a new Rds DB "goon-rds-demo" added to be streamed.
	{
	  "db_logfile": "NULL",
	  "db_name": "goon-rds-demo",
	  "hour": "-1",
	  "marker": "0",
	  "seq_token": "START",
	}
*/

const (
	logFileTS          = "2006-01-02-15"
	logTimeStampFormat = "2006-01-02 15:04:05 MST"
	pgFilePrefix       = "error/postgresql.log."
	logGroupTemplate   = "/postgres/DB_NAME/error"
	initSeqToken       = "START"
	maxBytes           = 1048576
	paddingByte        = 26
)

//DBLog ...
type DBLog struct {
	DbName    string `dynamodbav:"db_name"`
	DbLogfile string `dynamodbav:"db_logfile"`
	Marker    string `dynamodbav:"marker"`
	Hour      string `dynamodbav:"streamed_timestamp"`
	SeqToken  string `dynamodbav:"seq_token"`
}

var (
	awsSession    = session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	dySvc         = dynamodb.New(awsSession)
	cloudWatchSvc = cloudwatchlogs.New(awsSession)
	loc, _        = time.LoadLocation("UTC")
	dbNextToken   = make(map[string]string)

	stateTableName     = os.Getenv("STATE_TABLE")
	subscriptionFilter = os.Getenv("FILTER")
	region             = os.Getenv("REGION")
	lambdaArn          = os.Getenv("LAMBDA")
)

// Updates the Dybamodb State table.
func updateDbLogFile(dbName, logfile, marker, streamedTimeStamp string) {

	key := map[string]*dynamodb.AttributeValue{
		"db_name": {
			S: aws.String(dbName),
		},
	}
	reqInput := &dynamodb.UpdateItemInput{
		TableName:        aws.String(stateTableName),
		Key:              key,
		UpdateExpression: aws.String("SET #LF = :l, #MK =:m, #HR =:h, #TK =:t"),
		ExpressionAttributeNames: map[string]*string{
			"#LF": aws.String("db_logfile"),
			"#MK": aws.String("marker"),
			"#HR": aws.String("streamed_timestamp"),
			"#TK": aws.String("seq_token"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":l": {S: aws.String(logfile)},
			":m": {S: aws.String(marker)},
			":h": {S: aws.String(streamedTimeStamp)},
			":t": {S: aws.String(dbNextToken[dbName])},
		},
		ReturnValues: aws.String("ALL_NEW"),
	}

	res, err := dySvc.UpdateItem(reqInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			log.Println(aerr.Error(), err)
		} else {
			log.Println(err)
		}
	}
	log.Println("Streamed Postgres Error Log File", res)
}

// Subscribes the log group to the SumoLogic Lambda forwarder.
func subscribeLogGroupToLambdaForwarder(logGroupName, lambdaArn string) error {

	subscribeFilterInput := &cloudwatchlogs.PutSubscriptionFilterInput{
		DestinationArn: aws.String(lambdaArn),
		LogGroupName:   aws.String(logGroupName),
		FilterName:     aws.String(subscriptionFilter),
		FilterPattern:  aws.String(""),
	}
	_, err := cloudWatchSvc.PutSubscriptionFilter(subscribeFilterInput)
	if err != nil {
		return err
	}
	log.Printf("Subscribed %s loggroup to lambda function %s", logGroupName, lambdaArn)
	return nil
}

// This method invoked Creation of a CW Group and Stream.
func createLogGroupStream(logGroupName, streamName string) error {

	err := createGroup(logGroupName)
	if err != nil {
		log.Printf("LogGroup Creation failed %v", err)
		return err
	}
	status, err := createStream(logGroupName, streamName)
	if err != nil {
		log.Printf("LogStream Creation failed %v", err)
		return err
	}
	if status == "STREAM_CREATED" {
		err = subscribeLogGroupToLambdaForwarder(logGroupName, lambdaArn)
		if err != nil {
			log.Printf("LogGroup Subscription to Lambda Function failed %v", err)
			return err
		}
	}
	return nil
}

// Creates a CloudWatch LogGroup if it does not exist. If it exists returns nil
func createGroup(logGroupName string) error {

	log.Println("Creating a LogGroup", logGroupName)
	params := &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(logGroupName),
	}
LogGroup:
	_, err := cloudWatchSvc.CreateLogGroup(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case cloudwatchlogs.ErrCodeResourceAlreadyExistsException:
				log.Printf("LogGroup already exists %s", logGroupName)
				return nil
			case "ThrottlingException":
				time.Sleep(2 * time.Second)
				goto LogGroup
			default:
				return aerr
			}
		}
		return err
	}
	return nil
}

// Creates a CloudWatch Stream if it does not exist. If it exists returns nil
func createStream(logGroupName, logStreamName string) (string, error) {

	log.Printf("Creating a LogStream %s inside LogGroup %s", logStreamName, logGroupName)
	params := &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
	}
LogStream:
	_, err := cloudWatchSvc.CreateLogStream(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case cloudwatchlogs.ErrCodeResourceAlreadyExistsException:
				log.Printf("LogStream already exists %s", logStreamName)
				return "STREAM_EXISTS", nil
			case "ThrottlingException":
				time.Sleep(2 * time.Second)
				goto LogStream
			default:
				return "ERROR", aerr
			}
		}
		return "ERROR", err
	}
	return "STREAM_CREATED", nil
}

// Chunks the db log file so that the streamer adheres to the AWS
// PutlogEvents limit.
func logLineChunker(lines []string) ([][]string, error) {

	logStringEvents := make([][]string, 0)
	chunk := make([]string, 0)
	prev, current := 0, 0
	lineBytes := []byte{}

	log.Printf("Number of lines %d", len(lines))
	for _, line := range lines {
		lineBytes = []byte(line)
		prev = current
		current += paddingByte + len(lineBytes)

		if current < maxBytes {
			chunk = append(chunk, line)
			continue
		} else {
			log.Printf("The current byteCounter %d is greater than maxBytes "+
				"%d and bytes of last line %d", current, maxBytes, len(lineBytes))
			logStringEvents = append(logStringEvents, chunk)
			log.Printf("The number of chunks %d", len(logStringEvents))
			log.Printf("Current Max:%d, %d, %d", maxBytes, current, prev)
			current, prev = 0, 0
			chunk = nil
		}
	}
	logStringEvents = append(logStringEvents, chunk)
	return logStringEvents, nil
}

// Forwards DBLogs to a CloudWatch Stream by chunking the file
// if necessary.
func streamLogTextToCloudWatch(dbName, logfile string,
	output *rds.DownloadDBLogFilePortionOutput) error {

	log.Printf("Starting to Stream %s file to CloudWatch Logs", logfile)
	logGroupName := strings.Replace(logGroupTemplate, "DB_NAME", dbName, 1)
	err := createLogGroupStream(logGroupName, dbName)
	if err != nil {
		log.Printf("Failed LogGroup or Stream Creation %v", err)
		return err
	}
	logLines := strings.Split(strings.TrimSpace(*output.LogFileData), "\n")
	logLineChunks, err := logLineChunker(logLines)
	if err != nil {
		log.Printf("Error occurred while chunking the db log file")
		return err
	}

	for index, chunk := range logLineChunks {
		log.Printf("Number of lines in chunk %d is %d", index+1, len(chunk))
		logEvents := make([]*cloudwatchlogs.InputLogEvent, 0, len(chunk))
		seqToken := initSeqToken
		if _, ok := dbNextToken[dbName]; ok {
			seqToken = dbNextToken[dbName]
		}
		for _, line := range chunk {
			logLine := strings.Split(line, "UTC")
			if len(logLine) == 2 {
				logTime := logLine[0]
				eventTime, _ := time.Parse(logTimeStampFormat, logTime+" UTC")
				inputLogEvent := &cloudwatchlogs.InputLogEvent{
					Message:   aws.String(line),
					Timestamp: aws.Int64(eventTime.UnixNano() / int64(time.Millisecond)),
				}
				logEvents = append(logEvents, inputLogEvent)
			}
		}

		if len(logEvents) >= 1 {
			cloudWatchEvents := &cloudwatchlogs.PutLogEventsInput{
				LogGroupName:  aws.String(logGroupName),
				LogStreamName: aws.String(dbName),
				LogEvents:     logEvents,
			}
			if seqToken != initSeqToken {
				log.Println("Starting with an valid seq token", seqToken)
				cloudWatchEvents = &cloudwatchlogs.PutLogEventsInput{
					LogGroupName:  aws.String(logGroupName),
					LogStreamName: aws.String(dbName),
					SequenceToken: aws.String(seqToken),
					LogEvents:     logEvents,
				}
			} else {
				log.Println("Starting with an initial seq token 'START'", seqToken)
			}
		PutLogEvents:
			resp, err := cloudWatchSvc.PutLogEvents(cloudWatchEvents)
			if err != nil {
				log.Printf("The PutLogEvents failed with %v", err)
				if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "ThrottlingException" {
					// Constant backoff
					time.Sleep(2 * time.Second)
					goto PutLogEvents
				}
				return err
			}
			log.Println("The PutLogEvents succeeded with next sequence token", *resp.NextSequenceToken)
			if resp.RejectedLogEventsInfo != nil {
				log.Printf("%s events were rejected", *resp.RejectedLogEventsInfo)
			}
			dbNextToken[dbName] = *resp.NextSequenceToken
		}
	}
	log.Printf("No data exists in %s or the file has been completely streamed", logfile)
	return nil
}

// This method will tail a database log file. If the time difference between
// the current hour and the file hour is greater than 1 it set the Marker as
// completed. If its less than 1 hour it will put the actualy marker value
// denoting that the file is not fully streamed.
func tailDatabaseLogFile(dbName, logfile, marker, hour string) error {

	log.Printf("Starting to tail %s file for db %s from marker %s", logfile, dbName, marker)
	rdsClient := rds.New(awsSession)

	dbInfo := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(dbName),  // DB Instance
		LogFileName:          aws.String(logfile), // FileName with yyyy-mm-dd appended.
		Marker:               aws.String(marker),  // Starts at 0.
	}
Download:
	res, err := rdsClient.DownloadDBLogFilePortion(dbInfo)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case rds.ErrCodeDBInstanceNotFoundFault:
				log.Println(rds.ErrCodeDBInstanceNotFoundFault, aerr.Error())
			case rds.ErrCodeDBLogFileNotFoundFault:
				log.Println(rds.ErrCodeDBLogFileNotFoundFault, aerr.Error())
				updateDbLogFile(dbName, logfile, "COMPLETED", hour)
			case "ThrottlingException":
				time.Sleep(2 * time.Second)
				goto Download
			default:
				log.Println(aerr.Error())
				return err
			}
		} else {
			log.Println(err)
			return err
		}
		return nil
	}
	if len(*res.LogFileData) == 0 {
		log.Printf("No data has been appended to the log file %s from marker %s", logfile, marker)
		return nil
	}
	// Write the log lines to the cloudwatch log group
	log.Printf("The sequence token for db %s is %s", dbName, dbNextToken[dbName])
Streamer:
	err = streamLogTextToCloudWatch(dbName, logfile, res)
	if err != nil {
		log.Println("Streaming to CloudWatch Error", err)
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "ThrottlingException" {
			// Constant backoff
			time.Sleep(2 * time.Second)
			goto Streamer
		}
		return err
	}

	dbHour, _ := time.Parse(logFileTS, hour)
	currentTime := time.Now().In(loc)

	log.Printf("Data Pending %v, CurrentUTCTime - EventUTCTime > 1 hour: %v",
		*res.AdditionalDataPending, currentTime.Sub(dbHour) >= time.Hour)
	log.Print("********Postgres Error Log Data********\n", *res.LogFileData)
	log.Println("********Postgres End Error Log Data********")

	// File has been completely streamed and more than 1 hour has passed.
	if !*res.AdditionalDataPending && currentTime.Sub(dbHour) >= time.Hour {
		updateDbLogFile(dbName, logfile, "COMPLETED", hour)
		log.Printf("Completed Processing DBLog File %s for db %s", logfile, dbName)
	} else if !*res.AdditionalDataPending && currentTime.Sub(dbHour) < time.Hour {
		updateDbLogFile(dbName, logfile, *res.Marker, hour)
		log.Printf("Partially completed processing DBLog File %s for db %s", logfile, dbName)
	}
	return nil
}

// Loads the Postgres DB's from the state table to be streamed.
func loadPartiallyStreamedDBS() ([]map[string]*dynamodb.AttributeValue, error) {

	req := &dynamodb.ScanInput{
		TableName: aws.String(stateTableName),
	}
	res, err := dySvc.Scan(req)
	if err != nil {
		log.Println("loadPartiallyStreamedDBS Error", err)
		return make([]map[string]*dynamodb.AttributeValue, 0), err
	}
	return res.Items, nil
}

// Helper function to get a time instance from a string
func getTimeFromString(dbHour string) time.Time {

	t, err := time.Parse(logFileTS, dbHour)
	if err == nil {
		return t.UTC()
	}
	return time.Time{}
}

// Processes all existing log files for a new database
// or log files from the last time the job ran to the latest file.
func processLogFiles(dbName, dbHour, dbMarker, dbLogFile string) (string, error) {

	finalFile := ""
	input := &rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: aws.String(dbName),
		FilenameContains:     aws.String("error"),
	}

	// We process all existing error log files the first time
	// Subsequent runs will process from the last file present in the state db
	// NULL in the name  denotes a db that is added for the first time. If the hour is -1
	// that means the Db has had no previous log files streamed and the name of the
	// the file is NULL. We process all existing error logs files until the current hour.
	if dbLogFile != "NULL" {
		fileWrittenSince, err := time.Parse(logFileTS, dbHour)
		log.Printf("Get all files written since %v", fileWrittenSince)
		if err != nil {
			log.Println(err)
		}
		input = &rds.DescribeDBLogFilesInput{
			DBInstanceIdentifier: aws.String(dbName),
			FilenameContains:     aws.String("error"),
			FileLastWritten:      aws.Int64(fileWrittenSince.UnixNano() / 1e6),
		}
	}

	rdsClient := rds.New(awsSession)
	logFiles, err := rdsClient.DescribeDBLogFiles(input)
	log.Printf("%d log files exist since the last run", len(logFiles.DescribeDBLogFiles))
	if err != nil {
		log.Println(err.Error())
		return "Failure", err
	}
	// One case is when you start logging for the first time
	// Another case is you run the logger again within the same hour -- the same file.
	// Finally the third case is when you run it after a few days/more than 1 hour -- more files
	for _, logg := range logFiles.DescribeDBLogFiles {
		lgFlHr := time.Unix(*logg.LastWritten/1e3, 0).UTC()
		log.Printf("Log file %s was last written at %v", *logg.LogFileName, lgFlHr)
		dbHourTime := getTimeFromString(dbHour)
		duration := lgFlHr.Sub(dbHourTime)

		if dbHour != "-1" && duration <= time.Hour {
			log.Printf("Processing file %s, %v since %v",
				*logg.LogFileName, duration.Round(time.Minute), dbHourTime)
		} else {
			log.Printf("Processing file %s, %v since %v",
				*logg.LogFileName, duration.Round(time.Hour), dbHourTime)
		}
		// Files other than last file from the previous run or a brand new db
		// If the hours are different then start processing file from "0" Marker
		if lgFlHr.Format(logFileTS) != dbHour {
			if dbHour == "-1" {
				log.Printf("Streaming New DB %s file %s", dbName, *logg.LogFileName)
			}
			log.Printf("Current file date hour  %s != State DB Hour %s", lgFlHr, dbHour)
			err = tailDatabaseLogFile(dbName, *logg.LogFileName, "0", lgFlHr.Format(logFileTS))
		} else {
			// Processing last file from previous run if not completed.
			if dbMarker != "COMPLETED" {
				log.Printf("Streaming File %s from Marker %s", *logg.LogFileName, dbMarker)
				log.Printf("Current file date hour %s", lgFlHr)
				err = tailDatabaseLogFile(dbName, *logg.LogFileName, dbMarker, lgFlHr.Format(logFileTS))
				finalFile = *logg.LogFileName
				continue
			}
			// If completed then proceed to next file in the list
			log.Printf("Completed processing logfile %s for db %s", *logg.LogFileName, dbName)
		}
		if err != nil {
			log.Println(err)
			return "Failure", err
		}
		finalFile = *logg.LogFileName
	}
	return finalFile, nil
}

func startPostgresLogStreamer() error {

	startTime := time.Now()
	dblgs := []DBLog{}
	// New DB's need to be added to the state table.
	dbs, err := loadPartiallyStreamedDBS()
	if err != nil {
		log.Println(err)
		return err
	}
	err = dynamodbattribute.UnmarshalListOfMaps(dbs, &dblgs)
	if err != nil {
		log.Println(err)
		return err
	}
	if len(dblgs) == 0 {
		log.Fatal("Configuration db has no databases loaded to capture logs.")
	}

	for _, db := range dblgs {
		log.Printf("\n############# Postgres DB %s #############\n", db.DbName)

		elapsed := time.Since(startTime)
		dbNextToken[db.DbName] = db.SeqToken
		log.Printf("The sequence token for db %s is %s", db.DbName, dbNextToken[db.DbName])
		if elapsed.Minutes() > 4.5 {
			os.Exit(0)
		}
		finalFile, err := processLogFiles(db.DbName, db.Hour, db.Marker, db.DbLogfile)
		if err != nil {
			log.Println(err)
			return err
		}
		log.Printf("The last file processed for db %s in this run is %s", db.DbName, finalFile)
	}
	return nil
}

func main() {
	lambda.Start(startPostgresLogStreamer)
}
