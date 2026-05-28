# UPU MongoDB cluster — Phase 3 POC

A three-node MongoDB replica set in Docker, used as the data layer for the
UPU (Emergency Unit) patient-data proof of concept.

## Files

- `docker-compose.yml` — declares the three nodes, the network and the volumes.
- `scripts/init-replica.js` — one-time replica-set initialization.

## Bring up

```powershell
docker compose up -d
docker exec -it upu-db-1 mongosh --file /scripts/init-replica.js
```

## Check status

```powershell
docker exec -it upu-db-1 mongosh --quiet --eval "rs.status().members.forEach(m => print(m.name, m.stateStr))"
```

Expected:

```
upu-db-1:27017 PRIMARY
upu-db-2:27017 SECONDARY
upu-db-3:27017 SECONDARY
```

## Tear down (keeps volumes / data)

```powershell
docker compose down
```

## Load Diabetes 130 dataset
### Manual CSV + mongoimport

Download the zip from [UCI](https://archive.ics.uci.edu/ml/machine-learning-databases/00296/dataset_diabetes.zip) or Kaggle, unzip `diabetic_data.csv`, then:

```powershell
docker cp .\diabetic_data.csv upu-db-1:/tmp/diabetic_data.csv
docker exec -it upu-db-1 mongoimport --db hospital --collection admissions --type csv --headerline --file /tmp/diabetic_data.csv
```

Dataset page on Kaggle: https://www.kaggle.com/datasets/brandao/diabetes


## Producer / Consumer benchmark scenario (Phase 4)

This scenario models an asynchronous workload on top of the diabetes admissions data:

- `scripts/producer.py` samples encounters from `hospital.admissions` and publishes events into `hospital.events_queue`.
- `scripts/consumer.py` atomically claims NEW events, simulates processing, marks them DONE, and writes per-event timing to `hospital.events_results`.
- `scripts/scenario_producer_consumer.ps1` runs both scripts and prints summary metrics.

### Install dependency

```powershell
pip install -r requirements-bench.txt
```

### Run scenario (recommended)

```powershell
.\scripts\scenario_producer_consumer.ps1
```

### Run manually in two terminals

Terminal A (consumer):

```powershell
python scripts\consumer.py --max-messages 2000 --idle-timeout-s 20 --write-concern majority
```

Terminal B (producer):

```powershell
python scripts\producer.py --messages 2000 --batch-size 100 --sleep-ms 50 --write-concern majority --reset-queue
```

### Useful result query

```powershell
docker exec upu-db-1 mongosh hospital --quiet --eval "print('DONE:', db.events_queue.countDocuments({state:'DONE'})); print('results:', db.events_results.countDocuments())"
```
