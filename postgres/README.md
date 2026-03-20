# Logical WAL Message Demo

An end-to-end example that shows how to

1. run regular SQL using pgx, insert data, and emit custom WAL messages via `pg_logical_emit_message`, and
2. consume those messages over a logical replication slot using pglogrepl.

## Prerequisites

- PostgreSQL 15+ with logical replication enabled (set in `postgresql.conf`):
  - `wal_level = logical`
  - `max_wal_senders >= 5`
  - `max_replication_slots >= 5`
  - ensure `pg_hba.conf` allows replication connections for your user
- Go 1.22+

## Setup

```bash
git clone <repo>
cd postgres
go mod tidy
```

Export connection strings (or rely on defaults):

```bash
export PG_DSN="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
export PG_REPL_DSN="postgres://postgres:postgres@localhost:5432/postgres?replication=database"
```

Ensure the Postgres user has replication permission; on macOS/Linux with local trust you can usually reuse the same superuser.

## Producer

The producer inserts a fake order and emits a logical message with prefix `wal_demo`.

```bash
go run ./cmd/producer
# Output:
# recorded order 42 and emitted logical message under prefix "wal_demo"
```

It creates `orders` table if missing, inserts a random amount, then calls `pg_logical_emit_message(false, prefix, payload)` where the payload is JSON.

## Consumer

The consumer:

- creates the publication (`wal_demo_pub`) and logical slot (`wal_demo_slot`) if they do not exist
- starts logical replication via pglogrepl
- filters for logical decoding messages whose prefix matches `wal_demo`
- prints the JSON payload

```bash
go run ./cmd/consumer
# listening on slot wal_demo_slot for prefix "wal_demo"
# received logical message lsn=0/16B32B0 payload=map[event:order_created order_id:42 sent_at:...]
```

Leave the consumer running, execute the producer repeatedly to see new messages flow through.

## Customizing

Set the following env vars to change defaults:

| Variable | Description |
| --- | --- |
| `PG_DSN` | Regular connection string for both apps. |
| `PG_MESSAGE_PREFIX` | Logical message prefix (default `wal_demo`). |
| `PG_SLOT` | Replication slot name (default `wal_demo_slot`). |
| `PG_PUBLICATION` | Publication name (default `wal_demo_pub`). |
| `PG_REPL_DSN` | Replication connection string for consumer (needs `replication=database`). |

## Troubleshooting

- If the consumer exits with `permission denied`, ensure the user is allowed in `pg_hba.conf` for replication connections.
- If `pg_logical_emit_message` is missing, verify you are on Postgres ≥ 9.6 and the extension is available.
- For remote Postgres, ensure `wal_level=logical` and restart if you change config.
