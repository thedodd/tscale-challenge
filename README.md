timescale challenge
===================
A CLI tool for the Timescale code challenge.

TODO:
- [ ] test builds without vendored deps or test out vendoring.
- [ ] dump custom stats on number of goroutines spun up.
­- [ ] Handle CSV as either STDIN or via a flag with the filename.
- [ ] Unit / functional tests.

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
You can execute the cli for this challenge by invoking in a few ways:
- `go run main.go -f challenge-data/query_params.csv` this will build & run the program.
