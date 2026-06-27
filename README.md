# Maxwell Worker Consumer CDC

A lightweight Go worker that consumes Maxwell CDC events from Redis Streams and synchronizes data into a target MySQL or MariaDB database.

## Features

* Redis Streams consumer group support
* Automatic pending message recovery (`XAUTOCLAIM`)
* Insert, update, and delete synchronization
* MySQL `ON DUPLICATE KEY UPDATE` upserts
* Failed event handling with dead-letter stream
* Configurable retry mechanism
* Health and readiness endpoints
* Graceful shutdown support
* Database and table allow-lists
* Supports:

  * MySQL 8+
  * MariaDB 10.11+
  * MariaDB 11+

---

## Architecture

```text
MySQL (Source)
       │
       ▼
   Maxwell CDC
       │
       ▼
Redis Stream (maxwell:cdc:stream)
       │
       ▼
Go Worker Consumer
       │
       ▼
MySQL/MariaDB (Read Database)
```

---

## Project Structure

```text
.
├── README.md
└── worker
    ├── Dockerfile
    ├── go.mod
    ├── go.sum
    └── main.go
```

---

## How It Works

1. Maxwell captures database changes from the source database.
2. Events are written into a Redis Stream.
3. The Go worker consumes events using a Redis consumer group.
4. Events are applied to the target database:

   * `insert` → `INSERT ... ON DUPLICATE KEY UPDATE`
   * `update` → `INSERT ... ON DUPLICATE KEY UPDATE`
   * `delete` → `DELETE FROM ... WHERE id=?`
5. Failed events are moved to a dedicated failed stream.
6. Unacknowledged messages are automatically reclaimed after a configurable idle timeout.

---

## Environment Variables

| Variable             | Default                    | Description                       |
| -------------------- | -------------------------- | --------------------------------- |
| `REDIS_HOST`         | `redis-cdc`                | Redis hostname                    |
| `REDIS_PORT`         | `6379`                     | Redis port                        |
| `REDIS_PASSWORD`     | `""`                       | Redis password                    |
| `REDIS_STREAM`       | `maxwell:cdc:stream`       | Source Redis stream               |
| `REDIS_GROUP`        | `taskmanagement-read-sync` | Consumer group                    |
| `REDIS_CONSUMER`     | `consumer-1`               | Consumer name                     |
| `FAILED_STREAM`      | `maxwell:cdc:failed`       | Failed event stream               |
| `TARGET_DB_HOST`     | `127.0.0.1`                | Target database host              |
| `TARGET_DB_PORT`     | `3306`                     | Target database port              |
| `TARGET_DB_USER`     | `root`                     | Database user                     |
| `TARGET_DB_PASSWORD` | `""`                       | Database password                 |
| `TARGET_DB_NAME`     | `app_db`                   | Database name                     |
| `MAX_RETRIES`        | `5`                        | Retry attempts per event          |
| `BLOCK_SECONDS`      | `5`                        | Redis blocking read timeout       |
| `PENDING_IDLE_MS`    | `60000`                    | Pending reclaim timeout           |
| `BATCH_COUNT`        | `10`                       | Events processed per batch        |
| `STOP_ON_CRITICAL`   | `true`                     | Exit on critical schema errors    |
| `PRIMARY_KEY_MAP`    | `users:id`                 | Table primary key mappings        |
| `ALLOWED_DATABASES`  | `""`                       | Comma-separated allowed databases |
| `ALLOWED_TABLES`     | `""`                       | Comma-separated allowed tables    |
| `HEALTH_ADDR`        | `:8080`                    | Health server address             |

---

## Example Configuration

```bash
export REDIS_HOST=redis-cdc
export REDIS_STREAM=maxwell:cdc:stream

export TARGET_DB_HOST=mysql-read
export TARGET_DB_PORT=3306
export TARGET_DB_USER=sync_user
export TARGET_DB_PASSWORD=secret
export TARGET_DB_NAME=task_management

export PRIMARY_KEY_MAP="users:id,tasks:id,projects:id"
export ALLOWED_TABLES="users,tasks,projects"
```

---

## Running Locally

```bash
cd worker

go mod download

go run .
```

Build:

```bash
go build -o worker-app .
```

Run binary:

```bash
./worker-app
```

---

## Docker

Build image:

```bash
docker build -t maxwell-worker-consumer-cdc ./worker
```

Run container:

```bash
docker run \
  -e REDIS_HOST=redis \
  -e TARGET_DB_HOST=mysql \
  -e TARGET_DB_USER=root \
  -e TARGET_DB_PASSWORD=password \
  -e TARGET_DB_NAME=app \
  maxwell-worker-consumer-cdc
```

---

## Health Endpoints

### Liveness

```text
GET /healthz
```

Response:

```text
ok
```

### Readiness

```text
GET /readyz
```

Response:

```text
ready
```

or

```text
not ready
```

### Metrics

```text
GET /metrics
```

---

## Failed Events

Failed messages are automatically stored in:

```text
maxwell:cdc:failed
```

Stored metadata includes:

* Original Redis message ID
* Error message
* Full Maxwell event
* Raw Redis fields
* Failure timestamp
* Critical flag

This allows replaying or investigating failed synchronization events.

---

## Duplicate Protection

The worker uses:

```sql
INSERT ... ON DUPLICATE KEY UPDATE
```

To prevent duplicate rows, target tables must have:

```sql
PRIMARY KEY (id)
```

or a proper `UNIQUE` constraint.

---

## Graceful Shutdown

The worker handles:

```text
SIGTERM
SIGINT
```

During shutdown it:

* Marks itself as not ready
* Stops consuming new messages
* Completes in-flight work
* Closes Redis connections
* Closes database connections
* Shuts down the HTTP server cleanly
