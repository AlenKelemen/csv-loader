package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"flag"
	"io"
	"log"
	"os"
	"time"

	"github.com/fsnotify/fsnotify"
	_ "github.com/jackc/pgx/v4/stdlib"
)

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
					parseCSV(event.Name)
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

func parseCSV(path string) {
	file, err := os.Open(path)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))

	reader.Read()

	for {
		row, err := reader.Read()

		if err == io.EOF {
			break
		}

		writeToDatabase(row)
	}
}

func writeToDatabase(row []string) {

	if len(row) != 4 {
		return
	}

	db, err := sql.Open("pgx", os.Getenv("TH_DATABASE_PATH"))

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	format := "2006-01-02 15:04:05"
	date, err := time.Parse(format, row[1])

	if err != nil {
		log.Fatal(err)
	}

	sqlStatement := `INSERT INTO main.reading (device_id, date_taken, raw_value , category_id)
				     VALUES ($1, $2, $3, $4)`

	_, err = db.Exec(sqlStatement, 177, date, row[2], 1)

	if err != nil {
		log.Fatal(err)
	}
}
