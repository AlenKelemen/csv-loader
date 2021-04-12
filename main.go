package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const FLOW_FILE_LEN = 6
const FLOW_TYPE_INVALID = 0
const DATE_FORMAT = "2006-01-02 15:04:05.000"
const DATABASE_ENV = "TH_DATABASE_PATH"
const DATABASE_DRIVER = "pgx"

type FlowData struct {
	dateTaken       time.Time
	rawValue        int
	MeasurementType int
}

func main() {
	dir := loadFlag()
	initWatcher(dir)
}

func loadFlag() *string {
	pathFlag := flag.String("p", "", "Path to the directory")
	flag.Parse()
	return pathFlag
}

func initWatcher(dir *string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					readCSV(event.Name)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(*dir)
	if err != nil {
		log.Fatal(err)
	}
	<-done
}

func readCSV(path string) {
	file, err := os.ReadFile(path)

	if err != nil {
		log.Fatal(err)
	}

	data := string(file)

	reader := csv.NewReader(strings.NewReader(data))
	reader.FieldsPerRecord = -1

	result, err := reader.ReadAll()

	if err != nil {
		log.Fatal(err)
	}

	parseCSV(result)
}

func parseCSV(data [][]string) {
	// Dirty hack
	if len(data) != FLOW_FILE_LEN {
		return
	}

	location := time.Now().Local().Location()
	date, err := time.ParseInLocation(DATE_FORMAT, data[2][1], location)

	if err != nil {
		log.Fatal(err)
	}

	flowOne, err := strconv.Atoi(data[3][2] + data[2][2])

	if err != nil {
		log.Fatal(err)
	}

	flowTwo, err := strconv.Atoi(data[5][2] + data[4][2])

	if err != nil {
		log.Fatal(err)
	}

	fA := FlowData{MeasurementType: 10, dateTaken: date, rawValue: flowOne}
	writeToDatabase(fA)
	fB := FlowData{MeasurementType: 11, dateTaken: date, rawValue: flowTwo}
	writeToDatabase(fB)
}

func writeToDatabase(flowData FlowData) {
	if flowData.MeasurementType == FLOW_TYPE_INVALID {
		return
	}

	db, err := sql.Open(DATABASE_DRIVER, os.Getenv(DATABASE_ENV))

	if err != nil {
		log.Println(err)
	}

	defer db.Close()

	if err != nil {
		log.Println(err)
	}

	sql := `INSERT INTO main.reading (device_id, date_taken, raw_value , category_id)
				     VALUES ($1, $2, $3, $4)`

	_, err = db.Exec(sql, 177, flowData.dateTaken, flowData.rawValue, flowData.MeasurementType)

	if err != nil {
		log.Println(err)
	}
}
