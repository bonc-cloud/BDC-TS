// bulk_load_opentsdb loads an OpenTSDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/apache/iotdb-client-go/client"
	"github.com/caict-benchmark/BDC-TS/util/report"
	jsoniter "github.com/json-iterator/go"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// Program option vars:
var (
	count          int
	workers        int
	batchSize      int
	backoff        time.Duration
	doLoad         bool
	memprofile     bool
	reportDatabase string
	reportHost     string
	reportUser     string
	reportPassword string
	reportTagsCSV  string
	host           string
	port		   string
	tabletBatch    int
	tabletsBatch   int
	recordsBatch   int
)

type Record struct {
	Device    string            `json:"device"`
	Timestamp int64             `json:"timestamp"`
	Measurements []string       `json:"measurements"`
	Values    []interface{}     `json:"values"`
	DataTypes []client.TSDataType `json:"dataTypes"`
}

// Global vars
var (
	bufPool        sync.Pool
	batchChan      chan *bytes.Buffer
	inputDone      chan struct{}
	workersGroup   sync.WaitGroup
	backingOffChan chan bool
	backingOffDone chan struct{}
	reportTags     [][2]string
	reportHostname string
	config =  &client.Config{
		UserName: "root",
		Password: "root",
	}

)

// Parse args:
func init() {
	flag.IntVar(&batchSize, "batch-size", 5000, "Batch size (input lines).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	//flag.DurationVar(&backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&memprofile, "memprofile", false, "Whether to write a memprofile (file automatically determined).")
	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics")
	flag.StringVar(&reportUser, "report-user", "", "User for host to send result metrics")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics")
	flag.StringVar(&host, "host", "127.0.0.1", "server ip")
	flag.StringVar(&port, "port", "6667", "client port")
	flag.IntVar(&tabletBatch, "tablet-batch", 1800, "Batch size for a tablet")
	flag.IntVar(&tabletsBatch, "tablets-batch", 1000, "Batch size for tablets")
	flag.IntVar(&recordsBatch, "records-batch", 2000, "Batch size for records")
	flag.Parse()

	config.Host = host
	config.Port = port







	/*if reportHost != "" {
		fmt.Printf("results report destination: %v\n", reportHost)
		fmt.Printf("results report database: %v\n", reportDatabase)

		var err error
		reportHostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() error: %s", err.Error())
		}
		fmt.Printf("hostname for results report: %v\n", reportHostname)

		if reportTagsCSV != "" {
			pairs := strings.Split(reportTagsCSV, ",")
			for _, pair := range pairs {
				fields := strings.SplitN(pair, ":", 2)
				tagpair := [2]string{fields[0], fields[1]}
				reportTags = append(reportTags, tagpair)
			}
		}
		fmt.Printf("results report tags: %v\n", reportTags)
	}*/
}

func main() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	batchChan = make(chan *bytes.Buffer, workers)
	inputDone = make(chan struct{})

	backingOffChan = make(chan bool, 100)
	backingOffDone = make(chan struct{})
	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processBatchesTablet()
		//go processBatchesTablets()
		//go processBatchesRecords()
	}

	go processBackoffMessages()

	start := time.Now()
	itemsRead := scan(batchSize)

	<-inputDone
	close(batchChan)

	workersGroup.Wait()

	close(backingOffChan)
	<-backingOffDone

	end := time.Now()
	took := end.Sub(start)
	rate := float64(itemsRead) / float64(took.Seconds())

	fmt.Printf("loaded %d items in %fsec with %d workers (mean values rate %f/sec)\n", itemsRead, took.Seconds(), workers, rate)
	if reportHost != "" {
		reportParams := &report.LoadReportParams{
			ReportParams: report.ReportParams{
				DBType:             "CirroTimes",
				ReportDatabaseName: reportDatabase,
				ReportHost:         reportHost,
				ReportUser:         reportUser,
				ReportPassword:     reportPassword,
				ReportTags:         reportTags,
				Hostname:           reportHostname,
				Workers:            workers,
				ItemLimit:          -1,
			},
			IsGzip:    true,
			BatchSize: batchSize,
		}
		err := report.ReportLoadResult(reportParams, itemsRead, rate, -1, took)

		if err != nil {
			log.Fatal(err)
		}
	}
}

// scan reads one line at a time from stdin.
// When the requested number of lines per batch is met, send a batch over batchChan for the workers to write.
func scan(linesPerBatch int) int64 {
	buf := bufPool.Get().(*bytes.Buffer)
	var n int
	var itemsRead int64
	newline := []byte("\n")

	scanner := bufio.NewScanner(bufio.NewReaderSize(os.Stdin, 4*1024*1024))
	for scanner.Scan() {
		itemsRead++
		if n > 0 {
			buf.Write(newline)
		}
		buf.Write(scanner.Bytes())

		n++
		if n >= linesPerBatch {
			batchChan <- buf
			buf = bufPool.Get().(*bytes.Buffer)
			n = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		buf.Write(newline)
		batchChan <- buf
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	return itemsRead
}

func buildTablet(dvId string, measurementSchemas []*client.MeasurementSchema, values map[int][]interface{}, timestamps []int64) *client.Tablet {
	rowCount := len(timestamps)
	tablet, _ := client.NewTablet(dvId, measurementSchemas, rowCount)
	for row := 0; row < rowCount; row++ {
		tablet.SetTimestamp(timestamps[row], row)
		for index, measurement := range measurementSchemas{
			switch measurement.DataType {
			case client.INT64:
				coulumnValue,_ := values[row][index].(json.Number).Int64()
				tablet.SetValueAt(coulumnValue, index, row)
			case client.INT32:
				coulumnValue,_ := values[row][index].(json.Number).Int64()
				tablet.SetValueAt(int32(coulumnValue), index, row)
			case client.FLOAT:
				coulumnValue,_ := values[row][index].(json.Number).Float64()
				tablet.SetValueAt(float32(coulumnValue), index, row)
			case client.DOUBLE:
				coulumnValue,_ := values[row][index].(json.Number).Float64()
				tablet.SetValueAt(coulumnValue, index, row)
			case client.TEXT:
				tablet.SetValueAt(values[row][index].(string), index, row)
			}

		}
	}
	return tablet
}


func processBatchesTablets() {
	session := client.NewSession(config)
	session.Open(false, 0)
	for batch := range batchChan {
		if doLoad {
			deviceRow    := make(map[string]int)
			dvMeasurementSchemas := make(map[string][]*client.MeasurementSchema)
			dvTimeStamps := make(map[string][]int64)
			dvValues := make(map[string]map[int][]interface{})
			batchData :=batch.String()
			lines := strings.Split(batchData,"\n")
			for _, line := range lines {
				var record Record
				decoder := jsoniter.NewDecoder(strings.NewReader(line))
				decoder.UseNumber()
				decoder.Decode(&record)
				jsoniter.Marshal(record)
				device := record.Device
				var measurementSchemas []*client.MeasurementSchema
				if dvMeasurementSchemas[device] == nil{
					dvValues[device] = make(map[int][]interface{})
				}

				for index, measurement := range record.Measurements {
					if dvMeasurementSchemas[device] == nil {

						measurementSchema := &client.MeasurementSchema{
							Measurement: measurement,
							DataType:    record.DataTypes[index],
						}
						measurementSchemas = append(measurementSchemas, measurementSchema)
					}
					rowIndex := deviceRow[device]
					dvValues[device][rowIndex] = append(dvValues[device][rowIndex], record.Values[index])

				}

				dvTimeStamps[device] = append(dvTimeStamps[device], record.Timestamp)
				if dvMeasurementSchemas[device] == nil{
					dvMeasurementSchemas[device] = measurementSchemas
				}
				deviceRow[device] ++
			}
			var tablets []*client.Tablet
			for k, _ := range dvMeasurementSchemas {
				tablet := buildTablet(k,dvMeasurementSchemas[k], dvValues[k], dvTimeStamps[k])
				tablets = append(tablets, tablet)
				if len(tablets) == tabletsBatch{
					session.InsertTablets(tablets,false)
					tablets = []*client.Tablet{}
				}
			}
			session.InsertTablets(tablets,false)
		}
		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)
	}
	session.Close()
	workersGroup.Done()
}

func translateValues(dataTypes [][]client.TSDataType, values [][]interface{}) [][]interface{} {
	for i := 0; i < len(dataTypes); i++ {
		for j := 0; j < len(dataTypes[i]); j++ {
			switch dataTypes[i][j] {
			case client.INT64:
				values[i][j], _ =values[i][j].(json.Number).Int64()
			case client.INT32:
				value, _ :=values[i][j].(json.Number).Int64()
				values[i][j] = int32(value)
			case client.FLOAT:
				value, _ :=values[i][j].(json.Number).Float64()
				values[i][j] = float32(value)
			case client.DOUBLE:
				values[i][j], _ =values[i][j].(json.Number).Float64()
			case client.TEXT:
				values[i][j] = values[i][j].(string)
			}
		}

	}
	return values
}

func processBatchesRecords() {
	session := client.NewSession(config)
	session.Open(false, 0)
	var deviceIds []string
	var measurements [][]string
	var dataTypes [][]client.TSDataType
	var values [][]interface{}
	var	timestamps []int64
	for batch := range batchChan {
		if doLoad {
			batchData := batch.String()
			lines := strings.Split(batchData, "\n")
			for _, line := range lines {
				var record Record
				decoder := jsoniter.NewDecoder(strings.NewReader(line))
				decoder.UseNumber()
				decoder.Decode(&record)
				jsoniter.Marshal(record)
				deviceIds = append(deviceIds, record.Device)
				measurements = append(measurements, record.Measurements)
				dataTypes = append(dataTypes, record.DataTypes)
				values = append(values, record.Values)
				timestamps = append(timestamps, record.Timestamp)
				if len(timestamps) == recordsBatch {
					values = translateValues(dataTypes, values)
					session.InsertRecords(deviceIds, measurements, dataTypes, values, timestamps)
					deviceIds = []string{}
					measurements = [][]string{}
					dataTypes = [][]client.TSDataType{}
					values = [][]interface{}{}
					timestamps = []int64{}
				}
			}
		}
		batch.Reset()
		bufPool.Put(batch)
	}
	if len(deviceIds)>0{
		values = translateValues(dataTypes, values)
		session.InsertRecords(deviceIds, measurements, dataTypes, values, timestamps)
	}

	session.Close()
	workersGroup.Done()
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatchesTablet() {
	deviceRow    := make(map[string]int)
	dvMeasurementSchemas := make(map[string][]*client.MeasurementSchema)
	dvTimeStamps := make(map[string][]int64)
	dvValues := make(map[string]map[int][]interface{})
	session := client.NewSession(config)
	session.Open(false, 0)
	for batch := range batchChan {
		if doLoad {
			batchData :=batch.String()
			lines := strings.Split(batchData,"\n")
			for _, line := range lines {
				var record Record
				decoder := jsoniter.NewDecoder(strings.NewReader(line))
				decoder.UseNumber()
				decoder.Decode(&record)
				jsoniter.Marshal(record)
				device := record.Device
				if deviceRow[device] == tabletBatch {
					tablet := buildTablet(device,dvMeasurementSchemas[device], dvValues[device], dvTimeStamps[device])
					session.InsertTablet(tablet,false)
					delete(deviceRow, device)
					delete(dvMeasurementSchemas, device)
					delete(dvValues, device)
					delete(dvTimeStamps, device)
				}
				var measurementSchemas []*client.MeasurementSchema
				if dvMeasurementSchemas[device] == nil{
					dvValues[device] = make(map[int][]interface{})
				}

				for index, measurement := range record.Measurements {
					if dvMeasurementSchemas[device] == nil {

						measurementSchema := &client.MeasurementSchema{
							Measurement: measurement,
							DataType:    record.DataTypes[index],
						}
						measurementSchemas = append(measurementSchemas, measurementSchema)
					}
					rowIndex := deviceRow[device]
					dvValues[device][rowIndex] = append(dvValues[device][rowIndex], record.Values[index])

				}

				dvTimeStamps[device] = append(dvTimeStamps[device], record.Timestamp)
				if dvMeasurementSchemas[device] == nil{
					dvMeasurementSchemas[device] = measurementSchemas
				}
				deviceRow[device] ++
			}
		}
		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)
	}
	for k, _ := range dvMeasurementSchemas {
		tablet := buildTablet(k,dvMeasurementSchemas[k], dvValues[k], dvTimeStamps[k])
		session.InsertTablet(tablet,false)
	}
	session.Close()
	workersGroup.Done()
}

func processBackoffMessages() {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range backingOffChan {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			fmt.Printf("backoff took %.02fsec\n", took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	fmt.Printf("backoffs took a total of %fsec of runtime\n", totalBackoffSecs)
	backingOffDone <- struct{}{}
}

// TODO(rw): listDatabases lists the existing data in OpenTSDB.
func listDatabases(daemonUrl string) ([]string, error) {
	return nil, nil
}
