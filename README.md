timescale challenge
===================
A CLI tool for the Timescale code challenge.

The main objective of this tool is to "measure the processing time of each query and output benchmark stats once all queries have been run. In particular, we are looking for the # of queries run, the total processing time across all queries, the minimum query time (for a single query), the median query time, the average query time, and the maximum query time.

**Benchmark output needs to include:**
- num of queries run.
- the total processing time across all queries.
- the minimum query time (for a single query).
- the median query time.
- the average query time.
- the maximum query time (for a single query).

**Queries have the following constraints:**
- Each query must return "the max cpu usage and min cpu usage of the given hostname for every minute in the time range specified by the start time and end time".
- Each query should be executed by a concurrent worker.
- Queries for the same hostname must be executed by the same worker each time (though, a single worker may execute for multiple hosts).

**The architecture of this CLI is as follows:**
- A central controller is used to parse each query param record.
- Controller dispatches the query param to the appropriate worker for the host based on a simple map. It uses channels to send the query param to the worker.
- A new worker is spun up per hostname.
- Workers report their work results to a collector which will calculate the benchmark output.

### setup
This project uses [Go modules](https://github.com/golang/go/wiki/Modules).

Data used for this challenge is [located here](https://www.dropbox.com/s/17mr38w21yhgjjl/TimescaleDB_coding_assignment-RD_eng_setup.tar.gz?dl=1). The data has been committed to this repo for ease of access under the `challenge-data` directory.

We are using Docker Compose for easily managing the lifecycle of the TimeScaleDB instance.

#### run timescaledb
If you don't already have `docker` or `docker compose` setup on your system, please follow the installation instructions outlined on [Docker's website](https://docs.docker.com/install/#supported-platforms).

Once you have docker ready to go, simply issue the following command from this repo's root directory to start the database:

```bash
# Boot the database.
docker-compose up -d
```

All of the databases data will be held in a volume called `timescaledb`. If you need to clear the data and start with a fresh volume: `docker-compose down -v`. Then simply boot the database back up again, and you should have a fresh disk to work with.

##### psql access
You can log into the timescaledb instance any time by using the following command:

```bash
# Get shell access to postgres.
docker-compose exec timescaledb psql -U postgres
```

#### setup challenge data
- First, ensure that your timescaledb instance is running as described above.
- Execute `docker-compose exec timescaledb psql -U postgres -f /etc/challenge-data/cpu_usage.sql` from the root of this repo. When docker-compose booted our timescaledb instance, it mounted the `challenge-data` to the container under `/etc`. This command will simply run the sql script.
- Lastly, execute `docker-compose exec timescaledb psql -U postgres -d homework -c "\COPY cpu_usage FROM /etc/challenge-data/cpu_usage.csv CSV HEADER"` from the root of this repo. This will populate our `cpu_usage` hypertable with the challenge data.

#### execute the cli
```bash
# This will build & run the program.
go run main.go -f challenge-data/query_params.csv

# You can also specify `-w` to specify the number of workers to spawn.
go run main.go -f challenge-data/query_params.csv -w 5

# For more help on running the program, use --help.
go run main.go --help
```

**PLEASE NOTE:** if you are having compiler issues with your dependencies, you probably need to ensure that Go modules are enabled. Check out the basic [troubleshooting info here](https://github.com/golang/go/wiki/Modules#what-are-some-general-things-i-can-spot-check-if-i-am-seeing-a-problem).

Here is an example of what the output will look like after running the program:

```bash
$ go run main.go -f challenge-data/query_params.csv -w 2

### Main Benchmarks
Total processing time:      751.64178ms
Number of queries executed: 200
Min Query Time:             2.345231ms
Max Query Time:             7.853057ms
Median Query Time:          3.079777ms
Average Query Time:         3.758208ms

Done.
```
