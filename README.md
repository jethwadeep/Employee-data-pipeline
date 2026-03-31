# Employee Data Pipeline

End-to-end data pipeline using Apache Spark and PostgreSQL,
running entirely in Docker containers.

## Architecture
```
employees_raw.csv → PySpark pipeline → PostgreSQL (employees_clean)
```

Services:
- **PostgreSQL 13** — target database
- **Apache Spark 3.5.1** — master + worker cluster
- All orchestrated via Docker Compose

## Project Structure
```
employee-pipeline/
├── docker-compose.yml       # service orchestration
├── .env                     # credentials and ports
├── README.md
├── data/
│   ├── generate_data.py     # generates employees_raw.csv
│   └── employees_raw.csv    # raw input data (1155 rows)
├── spark/
│   ├── pipeline.py          # main Spark cleaning job
│   ├── test_connection.py   # JDBC connectivity smoke test
│   ├── pipeline.log         # pipeline run logs
│   └── jars/
│       └── postgresql-42.7.3.jar  # JDBC driver
└── sql/
    ├── init.sql             # runs on first Postgres start
    ├── schema.sql           # creates employees_clean table
    └── verify.sql           # post-pipeline data quality checks
```

## Prerequisites

- Docker Desktop (with WSL 2 backend on Windows)
- Python 3.10+ with pip
- Git

## Setup and Run

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd employee-pipeline
```

### 2. Download the JDBC driver
```bash
curl -L -o spark/jars/postgresql-42.7.3.jar \
  https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

### 3. Start all services
```bash
docker-compose up -d
```

Wait ~30 seconds then verify:
```bash
docker-compose ps
# All three containers should show as running/healthy
```

### 4. Generate raw data
```bash
python data/generate_data.py
```

This creates `data/employees_raw.csv` with 1155 rows including
intentional data quality issues.

### 5. Run the pipeline
```bash
docker exec spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/extra/postgresql-42.7.3.jar \
  /opt/spark-apps/pipeline.py
```

Expected output:
```
Pipeline complete. 930 clean records in PostgreSQL.
```

### 6. Verify results
```bash
docker exec employee_postgres psql -U spark_user -d employee_db \
  -f /docker-entrypoint-initdb.d/verify.sql
```

Or connect via any PostgreSQL client:
- Host: `localhost`
- Port: `5432`
- Database: `employee_db`
- User: `spark_user`
- Password: `spark_pass123`

## Pipeline Stages

| Stage | Action | Rows affected |
|---|---|---|
| Load raw CSV | Read 1155 rows with schema | — |
| Dedup | Remove duplicate employee_id | -55 |
| Date filter | Remove future hire dates | -59 |
| Email validation | Null invalid emails | -111 |
| Transformations | Clean names, salary, dates | all rows |
| Enrichment | Add full_name, email_domain | all rows |
| Load | Write 930 rows to PostgreSQL | 930  |

## Data Quality Issues Handled

| Issue | Count | Treatment |
|---|---|---|
| Duplicate employee_id | 55 | Dropped |
| Future hire_date | 59 | Dropped |
| Invalid email format | 111 | Nulled → dropped |
| Salary with $/, symbols | 326 | Cleaned → decimal |
| Mixed case names | all | initcap() |
| Mixed case department | all | initcap() |
| Inconsistent status | all | Mapped to Active/Inactive/Terminated |
| Missing address | 92 | Kept as NULL |
| Missing salary | 60 | Kept as NULL |

## Spark UI

- Master: http://localhost:8080
- Worker: http://localhost:8081

## Stopping the stack
```bash
docker-compose down          # stop containers, keep data
docker-compose down -v       # stop containers, delete data volume
```

## Troubleshooting

**Spark can't connect to Postgres**
Run the connectivity test:
```bash
docker exec spark_master /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars/extra/postgresql-42.7.3.jar \
  /opt/spark-apps/test_connection.py
```

**Container not starting**
```bash
docker-compose logs postgres
docker-compose logs spark-master
```

**JDBC driver not found**
Confirm the jar is in `spark/jars/` and re-run `docker-compose up -d`.