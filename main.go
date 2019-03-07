package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/go-pg/pg"
	"github.com/gocarina/gocsv"
	"github.com/urfave/cli"
)

// QUERY is the tsdb query used as part of this challenge.
const QUERY string = `SELECT time_bucket('1 minute', ts) AS bucket, MAX(usage), MIN(usage)
	FROM cpu_usage
	WHERE host = $1 AND ts > $2 AND ts < $3
	GROUP BY bucket ORDER BY bucket;`

func main() {
	// Run the CLI.
	if err := runCli(); err != nil {
		log.Fatal(err)
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Controller ////////////////////////////////////////////////////////////////////////////////////

// WorkerMap is a type alias used by the Controller for worker communication & coordination.
type WorkerMap map[string]*WorkerChannels

// QueryParam is a data struct used for deserializing query parameters.
type QueryParam struct {
	Host  string `csv:"hostname"`
	Start string `csv:"start_time"`
	End   string `csv:"end_time"`
}

// WorkerChannels is a struct to keep track of communication channels between controller & worker.
type WorkerChannels struct {
	// A channel for sending work out to a worker.
	WorkOut chan<- *QueryParam

	// A channel used to determine if the worker is still working. After the worker has received
	// a termination signal, it will close this channel once it is done.
	ControlIn <-chan interface{}
}

// Controller is the type which is used to delegate work to the workers & coordniate shutdown.
type Controller struct {
	db                 *pg.DB
	preparedQuery      *pg.Stmt
	data               []*QueryParam
	workers            WorkerMap
	collectorChan      chan WorkSample
	collectorControlIn chan interface{}
}

// NewController will build a new controller instance.
func NewController(db *pg.DB, data []*QueryParam, preparedQuery *pg.Stmt) *Controller {
	workers := WorkerMap{}
	collectorChan := make(chan WorkSample, 200)
	collectorControlIn := make(chan interface{}, 0)
	return &Controller{db, preparedQuery, data, workers, collectorChan, collectorControlIn}
}

// Run will start the controller's main routine & will return once all work has finished.
func (c *Controller) Run() {
	// First order of business, build our collector.
	go NewCollector(c.collectorChan, c.collectorControlIn).Run()

	// Iterate over all query params and dispatch work.
	for _, qp := range c.data {
		if worker, ok := c.workers[qp.Host]; ok {
			// We already have a live worker, dispatch an event to it.
			worker.WorkOut <- qp
		} else {
			// Build communication channels.
			workChan := make(chan *QueryParam, 100)
			controlChan := make(chan interface{}, 0)

			// Build the new worker and spawn it.
			worker := &Worker{qp.Host, workChan, controlChan, c.collectorChan, c.preparedQuery}
			go worker.Run()

			// Send the worker its first task.
			c.workers[qp.Host] = &WorkerChannels{workChan, controlChan}
			workChan <- qp
		}
	}

	// Close work channels.
	for _, worker := range c.workers {
		close(worker.WorkOut)
	}

	// Wait for workers to finish.
	for _, worker := range c.workers {
		<-worker.ControlIn
	}

	// Workers are finished, so close the metrics chan & then wait for the collector to finish.
	close(c.collectorChan)
	<-c.collectorControlIn // Will block until connection is closed.

	fmt.Println("\nDone.")
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Collector /////////////////////////////////////////////////////////////////////////////////////

// Collector is used to receive all results from the workers and create our benchmark data.
type Collector struct {
	// The input channel used for receiving metrics from the various workers.
	// Once all workers have finished their work, the controller will close this channel and await
	// for the controlOut channel to be closed before finishing the program.
	receiver <-chan WorkSample

	// When this channel is closed, the Controller will know that it is safe to shutdown.
	controlOut chan<- interface{}

	// This collectors data store.
	data *DataStore
}

// NewCollector will build a new Collector instance.
func NewCollector(receiver <-chan WorkSample, controlOut chan<- interface{}) *Collector {
	return &Collector{receiver, controlOut, &DataStore{workerOps: map[string]uint64{}}}
}

// Run will run the collector's execution loop.
func (c *Collector) Run() {
	// Collect all metrics from workers.
	for sample := range c.receiver {
		c.AddWorkSample(sample)
	}

	// Metrics receiver has been closed, so it is time to crunch the numbers.
	median, average := c.calcMedianAndAverage()
	fmt.Printf(`
### Main Benchmarks
Total processing time: %s
# of queries executed: %d
Min Query Time:        %s
Max Query Time:        %s
Median Query Time:     %s
Average Query Time:    %s

### Worker Stats
// These stats show the number of workers spawned total as,
// well as the number of queries performed per worker.
Workers Spawned:    %d
`,
		c.data.totalProcessingTime.String(), c.data.numQueries,
		c.data.minQueryTime.String(), c.data.maxQueryTime.String(),
		median.String(), average.String(), len(c.data.workerOps),
	)

	for key, val := range c.data.workerOps {
		fmt.Printf("Worker %s: %d\n", key, val)
	}

	close(c.controlOut)
}

// AddWorkSample will update the data store with the given work sample.
func (c *Collector) AddWorkSample(sample WorkSample) {
	c.data.numQueries++
	c.data.totalProcessingTime += sample.OpTime
	c.data.allQueryTimes = append(c.data.allQueryTimes, sample.OpTime)

	// Update min query time as needed.
	if c.data.minQueryTime == nil {
		c.data.minQueryTime = &sample.OpTime
	} else if sample.OpTime < *c.data.minQueryTime {
		c.data.minQueryTime = &sample.OpTime
	}

	// Update max query as needed.
	if c.data.maxQueryTime == nil {
		c.data.maxQueryTime = &sample.OpTime
	} else if sample.OpTime > *c.data.maxQueryTime {
		c.data.maxQueryTime = &sample.OpTime
	}

	// Keep track of the number of ops run per worker.
	if val, exists := c.data.workerOps[sample.Worker]; exists {
		c.data.workerOps[sample.Worker] = val + 1
	} else {
		c.data.workerOps[sample.Worker] = 1
	}
}

// Calculate the median and average durations on the collected data.
func (c *Collector) calcMedianAndAverage() (time.Duration, time.Duration) {
	// Calculate median.
	sort.Sort(c.data.allQueryTimes) // Needed to calculate median.
	var median time.Duration
	dataLen := len(c.data.allQueryTimes)
	if dataLen%2 == 0 { // Len is even, so take mean of middle items / 2.
		mid0 := c.data.allQueryTimes[dataLen/2]
		mid1 := c.data.allQueryTimes[(dataLen/2)-1]
		median = (mid0 + mid1) / 2
	} else {
		median = c.data.allQueryTimes[dataLen/2]
	}

	// Calculate average.
	var sum time.Duration
	for _, d := range c.data.allQueryTimes {
		sum += d
	}
	average := time.Duration(sum.Nanoseconds() / int64(dataLen))
	return median, average
}

// DataStore is a struct used for storing the collected data from this challenge.
type DataStore struct {
	numQueries          uint64         // Number of queries run.
	totalProcessingTime time.Duration  // Total processing time across all queries.
	minQueryTime        *time.Duration // The smallest query time seen.
	maxQueryTime        *time.Duration // The largest query time seen.
	allQueryTimes       DurationSlice  // All query times, used for calculating median & average.

	// A mapping of worker names (corresponds to 1 goroutine each) and number of tasks processed.
	workerOps map[string]uint64
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Worker ////////////////////////////////////////////////////////////////////////////////////////

// Worker is used to perform database queries & creates needed stats to complete this challenge.
type Worker struct {
	// This workers name. This directly corresponds to the host it queries for.
	name string

	// Worker receives work requests here.
	receiver <-chan *QueryParam

	// When worker receives a termination request, it will close this channel to
	// indicate that all of its work is finished.
	controlOut chan<- interface{}

	// The output channel used for sending metrics to the collector.
	collectorOut chan<- WorkSample

	// The prepared query to be used by this worker.
	preparedQuery *pg.Stmt
}

// Run will execute this workers execution loop.
func (w *Worker) Run() {
	for qp := range w.receiver {
		// Start tracking query time.
		start := time.Now()

		// Execute query.
		var results []*QueryModel
		if _, err := w.preparedQuery.Query(&results, qp.Host, qp.Start, qp.End); err != nil {
			log.Fatal("Error while executing query: ", err)
		}

		// Stop work timer & calculate optime.
		stop := time.Now()
		opTime := stop.Sub(start)

		// Report metrics to the collector.
		w.collectorOut <- WorkSample{w.name, opTime}
	}

	close(w.controlOut)
}

// WorkSample is a data structure for recording the metrics on a query op from a worker.
type WorkSample struct {
	// The worker this metric came from.
	Worker string

	// The amount of time spent by the worker processing its query.
	OpTime time.Duration
}

// QueryModel is the data model used for interfacing with the `cpu_usage` challange table.
type QueryModel struct {
	Bucket time.Time
	Max    float32
	Min    float32
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Support Functions /////////////////////////////////////////////////////////////////////////////

func runCli() error {
	var filepath string

	app := cli.NewApp()
	app.Name = "tscli"
	app.Usage = "A CLI implementing the TimeScaleDB interview challenge requirements."
	app.Action = func(c *cli.Context) error {
		// Read in query params as needed.
		var queryParams []*QueryParam
		if filepath == "-" {
			if err := gocsv.Unmarshal(bufio.NewReader(os.Stdin), &queryParams); err != nil {
				return err
			}
		} else {
			// Open the target CSV file.
			file, err := os.Open(filepath)
			if err != nil {
				return err
			}
			defer file.Close()

			// Load query params from file.
			if err := gocsv.UnmarshalFile(file, &queryParams); err != nil {
				return err
			}
		}

		processChallengeData(queryParams)
		return nil
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "f,file",
			Usage:       "Specify a file to load with the challenge CSV query parameters. Leave unspecified to pass data in via stdin.",
			Value:       "-",
			Destination: &filepath,
		},
	}
	return app.Run(os.Args)
}

func processChallengeData(queryParams []*QueryParam) {
	// Build a database connection.
	db := pg.Connect(&pg.Options{
		Addr:     "localhost:5432",
		User:     "postgres",
		Password: "password",
		Database: "homework",
		PoolSize: 200,
	}).WithTimeout(time.Second * 5)
	if db.PoolStats() == nil {
		log.Fatal("Unable to establish connection to database.")
	}
	defer db.Close()

	// Build a prepared statement for use by the workers.
	preparedQuery, err := db.Prepare(QUERY)
	if err != nil {
		log.Fatal("Error preparing query: ", err)
	}

	// Build our controller and get things started.
	NewController(db, queryParams, preparedQuery).Run()
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Sort Interface Impls //////////////////////////////////////////////////////////////////////////

// DurationSlice is a type alias used for implementing sort on a collection of durations.
type DurationSlice []time.Duration

func (s DurationSlice) Len() int {
	return len(s)
}

func (s DurationSlice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s DurationSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
