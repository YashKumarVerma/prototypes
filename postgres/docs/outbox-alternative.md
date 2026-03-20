# Logical Message Outbox Alternative

- Instead of inserting rows into an "outbox" table and polling it, emit WAL messages directly with `pg_logical_emit_message(false, prefix, payload)` right after your business transaction.
- Messages are lightweight, never require cleanup, and stream through the same WAL replication channel your consumers already trust.
- The producer stays in plain SQL/pgx: run normal inserts/updates, wrap the payload in JSON (or binary), and call the helper to publish it—no extra tables, triggers, or cron jobs.
- Consumers subscribe via logical replication slots using `pgoutput`; filter by `prefix` to distinguish message types and decode the payloads in real time.
- Backpressure comes “for free”: if downstream falls behind, PostgreSQL retains WAL segments just like any other replication client, so ordering and durability match the database guarantees.
