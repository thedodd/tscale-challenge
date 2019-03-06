package main

import (
	"fmt"
	"os"
	"time"

	"github.com/go-pg/pg"
	"github.com/gocarina/gocsv"
)

/*
TODO:
- [ ] test builds without vendored deps or test out vendoring.
- [ ] dump custom stats on number of goroutines spun up.
­- [ ] Handle CSV as either STDIN or via a flag with the filename.
- [ ] Unit / functional tests.
*/

func main() {
	file, err := os.Open("./challenge-data/query_params.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Load query params from file.
	queryParams := []*QueryParam{}
	if err := gocsv.UnmarshalFile(file, &queryParams); err != nil {
		panic(err)
	}

	// Build a database connection.
	db := pg.Connect(&pg.Options{
		Addr:     "localhost:5432",
		User:     "postgres",
		Database: "cpu_usage",
		PoolSize: 200,
	})
	defer db.Close()

	// Build our controller and get things started.
	NewController(db, queryParams).Run()
}

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

//////////////////////////////////////////////////////////////////////////////////////////////////
// Controller ////////////////////////////////////////////////////////////////////////////////////

// Controller is the type which is used to delegate work to the workers & coordniate shutdown.
type Controller struct {
	db                 *pg.DB
	data               []*QueryParam
	workers            WorkerMap
	collectorChan      chan Metrics
	collectorControlIn chan interface{}
}

// NewController will build a new controller instance.
func NewController(db *pg.DB, data []*QueryParam) *Controller {
	workers := WorkerMap{}
	collectorChan := make(chan Metrics, 200)
	collectorControlIn := make(chan interface{}, 0)
	return &Controller{db, data, workers, collectorChan, collectorControlIn}
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

			// Spawn a new worker.
			go (&Worker{qp.Host, workChan, controlChan, c.collectorChan}).Run()

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

	fmt.Println("Done.")
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Collector /////////////////////////////////////////////////////////////////////////////////////

// Collector is used to receive all results from the workers and create our benchmark data.
type Collector struct {
	// The input channel used for receiving metrics from the various workers.
	// Once all workers have finished their work, the controller will close this channel and await
	// for the controlOut channel to be closed before finishing the program.
	receiver <-chan Metrics

	// When this channel is closed, the Controller will know that it is safe to shutdown.
	controlOut chan<- interface{}

	// This collectors data store.
	data *DataStore
}

// NewCollector will build a new Collector instance.
func NewCollector(receiver <-chan Metrics, controlOut chan<- interface{}) *Collector {
	return &Collector{receiver, controlOut, &DataStore{}}
}

// Run will run the collector's execution loop.
func (c *Collector) Run() {
	for sample := range c.receiver {
		fmt.Printf("Metric received from %s\n", sample.Worker)
		c.AddMetrics(sample)
	}

	fmt.Printf("Queries seen: %d\n", c.data.NumQueries)
	close(c.controlOut)
}

// AddMetrics will update the data store with the given metrics sample.
func (c *Collector) AddMetrics(metrics Metrics) {
	c.data.NumQueries++
}

// DataStore is a struct used for storing the collected data from this challenge.
type DataStore struct {
	NumQueries          uint64    // Number of queries run.
	totalProcessingTime float64   // Total processing time across all queries.
	minQueryTime        float32   // The smallest query time seen.
	maxQueryTime        float32   // The largest query time seen.
	allQueryTimes       []float32 // All query times, used for calculating median & average.
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
	collectorOut chan<- Metrics
}

// Run will execute this workers execution loop.
func (w *Worker) Run() {
	// for _ = range w.receiver {
	for range w.receiver {
		w.collectorOut <- Metrics{w.name}
	}

	close(w.controlOut)
}

// Metrics is a data structure for recording the metrics on a query op from a worker.
type Metrics struct {
	// The worker this metric came from.
	Worker string
}

// CPUUsageModel is the data model used for interfacing with the `cpu_usage` challange table.
type CPUUsageModel struct {
	Ts    time.Time
	Host  string
	Usage float32
}
