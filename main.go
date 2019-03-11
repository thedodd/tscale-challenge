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
type WorkerMap map[string]*WorkerHandle

// QueryParam is a data struct used for deserializing query parameters.
type QueryParam struct {
	Host  string `csv:"hostname"`
	Start string `csv:"start_time"`
	End   string `csv:"end_time"`
}

// WorkerHandle is a struct to keep track of communication channels between controller & worker.
type WorkerHandle struct {
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
	workers            []*WorkerHandle
	workerMap          WorkerMap
	collectorChan      chan WorkSample
	collectorControlIn chan interface{}
	numWorkers         uint64
	lbIndex            int
}

// NewController will build a new controller instance.
func NewController(db *pg.DB, data []*QueryParam, preparedQuery *pg.Stmt, numWorkers uint64) *Controller {
	var workers []*WorkerHandle
	workerMap := WorkerMap{}
	collectorChan := make(chan WorkSample, 200)
	collectorControlIn := make(chan interface{}, 0)
	return &Controller{db, preparedQuery, data, workers, workerMap, collectorChan, collectorControlIn, numWorkers, 0}
}

// Run will start the controller's main routine & will return once all work has finished.
func (c *Controller) Run() {
	// First order of business, build our collector.
	go NewCollector(c.collectorChan, c.collectorControlIn).Run()

	// Iterate over all query params and dispatch work.
	for _, qp := range c.data {
		if worker, ok := c.workerMap[qp.Host]; ok {
			// We already have a live worker, dispatch an event to it.
			worker.WorkOut <- qp
		} else {
			// If we've spawned maximum number of workers, then load balance the
			// new host accross currently spawned workers.
			if c.numWorkers > 0 && len(c.workers) == int(c.numWorkers) {
				channels := c.roundRobinSelect()
				c.workerMap[qp.Host] = channels
				channels.WorkOut <- qp
			} else {
				// Build communication channels.
				workChan := make(chan *QueryParam, 100)
				controlChan := make(chan interface{}, 0)

				// Build the new worker and spawn it.
				worker := &Worker{workChan, controlChan, c.collectorChan, c.preparedQuery}
				go worker.Run()

				// Send the worker its first task.
				channels := &WorkerHandle{workChan, controlChan}
				c.workerMap[qp.Host] = channels
				c.workers = append(c.workers, channels)
				workChan <- qp
			}
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

// roundRobinSelect will select an active worker from the pool of workers.
func (c *Controller) roundRobinSelect() *WorkerHandle {
	if len(c.workers) == c.lbIndex+1 {
		// We've LB'd to the end of the slice, so set back to elem 0.
		c.lbIndex = 0
		return c.workers[0] // In theory, this could cause a panic, but we do a len check before ever calling this method.
	} else {
		channels := c.workers[c.lbIndex]
		c.lbIndex++
		return channels
	}
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
	return &Collector{receiver, controlOut, &DataStore{}}
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
Total processing time:      %s
Number of queries executed: %d
Min Query Time:             %s
Max Query Time:             %s
Median Query Time:          %s
Average Query Time:         %s
`,
		c.data.totalProcessingTime.String(), c.data.numQueries,
		c.data.minQueryTime.String(), c.data.maxQueryTime.String(),
		median.String(), average.String(),
	)

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
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Worker ////////////////////////////////////////////////////////////////////////////////////////

// Worker is used to perform database queries & creates needed stats to complete this challenge.
type Worker struct {
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
		w.collectorOut <- WorkSample{opTime}
	}

	close(w.controlOut)
}

// WorkSample is a data structure for recording the metrics on a query op from a worker.
type WorkSample struct {
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
	var numWorkers uint64

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

		processChallengeData(queryParams, numWorkers)
		return nil
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "f,file",
			Usage:       "Specify a file to load with the challenge CSV query parameters. Leave unspecified to pass data in via stdin.",
			Value:       "-",
			Destination: &filepath,
		},
		cli.Uint64Flag{
			Name:        "w,workers",
			Usage:       "Specify the number of workers to use, must be >= 1 if specified. Leave unspecified to spawn a new worker per unique host.",
			Destination: &numWorkers,
		},
	}
	return app.Run(os.Args)
}

func processChallengeData(queryParams []*QueryParam, numWorkers uint64) {
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
	NewController(db, queryParams, preparedQuery, numWorkers).Run()
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
