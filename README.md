# Currency Exchange ETL Pipeline (Apache Airflow + Astro CLI + MongoDB)

An ETL pipeline that extracts daily currency exchange rates from the
[Frankfurter API](https://frankfurter.dev/), transforms them into clean records, and loads
them into **MongoDB**. The pipeline is orchestrated by **Apache Airflow 3** running locally
through the **Astro CLI** (Docker). **Mongo Express** provides a web UI for the database.

## Architecture

```
                         Docker network: <project>_airflow
 ┌──────────────────────────────────────────────────────────────────┐
 │  Airflow (Astro Runtime 3.3)                                     │
 │  DAG: currency_exchange_etl  (@daily)                            │
 │                                                                  │
 │  extract_exchange_rates ──[raw_exchange_rates]──►                │
 │      transform_exchange_rates ──[transformed_exchange_rates]──►  │
 │          load_to_mongodb ──[mongodb_load_summary]                │
 │                 │                                                │
 │                 ▼                                                │
 │            mongodb:27017  ◄── volume: currency_etl_mongo_data    │
 └─────────────────┬────────────────────────────────────────────────┘
                   │  Docker network: currency_etl_mongo_net
             mongo-express:8081  ──►  http://localhost:8081
```

| Component | Purpose |
|-----------|---------|
| Airflow (Astro CLI) | Schedules and runs the three ETL tasks |
| Frankfurter API | Source of daily exchange rates (`/v2/rates`) |
| MongoDB 7.0 | Primary database (`currency_db.exchange_rates`) |
| Mongo Express 1.0.2 | Web-based MongoDB administration UI |

## ETL tasks and named XCom keys

| # | Task | Pulls XCom key | Pushes XCom key |
|---|------|----------------|-----------------|
| 1 | `extract_exchange_rates` | – | `raw_exchange_rates` |
| 2 | `transform_exchange_rates` | `raw_exchange_rates` | `transformed_exchange_rates` |
| 3 | `load_to_mongodb` | `transformed_exchange_rates` | `mongodb_load_summary` |

All tasks use `do_xcom_push=False`, so data moves **only** through the explicit named keys
(`ti.xcom_push(key=...)` / `ti.xcom_pull(task_ids=..., key=...)`), never through implicit
`return_value` XComs.

**Transformed record format** (one document per target currency per day):

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1123,
  "rate_date": "2026-11-12",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-12T16:24:08"
}
```

Key behaviours:
- **Extract** handles timeouts, HTTP errors, connection failures, invalid JSON and empty responses (task retries: 2).
- **Transform** validates every entry, supports any number of target currencies and removes duplicates.
- **Load** uses a unique index on `(base_currency, target_currency, rate_date)` with upserts, so re-running is
  idempotent, then reads every record back to verify it was stored.

## Docker setup

`docker-compose.override.yml` is merged by the Astro CLI with its own Airflow services and adds:

- **`mongodb`** – joins Astro's `airflow` network (so tasks reach `mongodb:27017`) and the private
  `currency_etl_mongo_net` network; data persisted in the named volumes `currency_etl_mongo_data`
  and `currency_etl_mongo_config`; health check via `mongosh ping`.
- **`mongo-express`** – only on `currency_etl_mongo_net` (isolated from Airflow); starts after MongoDB is healthy.
- `mlops.*` **labels** on services, volumes and networks as metadata.

## Project structure

```
├── dags/currency_exchange_etl.py   # the ETL DAG (3 tasks, named XComs)
├── docker-compose.override.yml     # MongoDB + Mongo Express, networks, volumes
├── requirements.txt                # apache-airflow-providers-mongo
├── Dockerfile                      # Astro Runtime image
├── scripts/verify_mongodb.ps1      # prints stored records from MongoDB
├── tests/dags/                     # DAG integrity tests (Astro default)
├── .env.example                    # template for credentials (copy to .env)
└── README.md
```

## How to run

Prerequisites: Docker Desktop (WSL 2) and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).

```powershell
# 1. Create your .env from the template and set your own passwords
Copy-Item .env.example .env

# 2. Start Airflow + MongoDB + Mongo Express
astro dev start --wait 5m

# 3. Open the UIs
#    Airflow:       http://localhost:8080   (port printed by astro dev start)
#    Mongo Express: http://localhost:8081   (login from ME_CONFIG_BASICAUTH_* in .env)

# 4. Trigger the DAG "currency_exchange_etl" in the Airflow UI, then check MongoDB:
powershell -ExecutionPolicy Bypass -File scripts\verify_mongodb.ps1
```

### Verification commands

```powershell
# Networking
docker network inspect currency_etl_mongo_net --format "{{range .Containers}}{{.Name}}  {{end}}"
docker exec <project>-scheduler-1 python -c "import socket; socket.create_connection(('mongodb', 27017), 3); print('CONNECTED')"

# Persistent storage
docker volume ls --filter "label=mlops.project=currency-exchange-etl"
docker rm -f currency_mongodb ; astro dev restart   # data is still there afterwards
```

> Do **not** use `astro dev kill` – it removes the volumes and deletes the MongoDB data.

## Security

Credentials live only in `.env`, which is git-ignored. `.env.example` contains placeholders only.
The DAG reads MongoDB credentials from the Airflow connection `mongo_default`
(created from the `AIRFLOW_CONN_MONGO_DEFAULT` environment variable).
