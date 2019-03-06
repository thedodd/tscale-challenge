package main

import (
	"fmt"
	"os"

	"github.com/gocarina/gocsv"
)

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

	fmt.Println("Done.")
}

// QueryParam is a data struct used for deserializing query parameters.
type QueryParam struct {
	Host  string `csv:"hostname"`
	Start string `csv:"start_time"`
	End   string `csv:"end_time"`
}
