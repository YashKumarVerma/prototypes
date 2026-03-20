package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	AppConnString  string
	ReplConnString string
	Slot           string
	Publication    string
	Prefix         string
}

func main() {
	ctx := context.Background()
	cfg := loadConfig()

	adminPool, err := pgxpool.New(ctx, cfg.AppConnString)
	if err != nil {
		log.Fatalf("connect admin pool: %v", err)
	}
	defer adminPool.Close()

	if err := ensurePublication(ctx, adminPool, cfg.Publication); err != nil {
		log.Fatalf("ensure publication: %v", err)
	}
	if err := ensureSlot(ctx, adminPool, cfg.Slot); err != nil {
		log.Fatalf("ensure replication slot: %v", err)
	}

	replConn, err := pgconn.Connect(ctx, cfg.ReplConnString)
	if err != nil {
		log.Fatalf("connect replication stream: %v", err)
	}
	defer replConn.Close(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, replConn)
	if err != nil {
		log.Fatalf("identify system: %v", err)
	}
	clientLSN := sysident.XLogPos
	log.Printf("connected to system %s timeline %d start LSN %s", sysident.SystemID, sysident.Timeline, sysident.XLogPos)

	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", cfg.Publication),
		"messages 'true'",
	}
	if err := pglogrepl.StartReplication(ctx, replConn, cfg.Slot, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		log.Fatalf("start replication: %v", err)
	}
	log.Printf("listening on slot %s for prefix %q", cfg.Slot, cfg.Prefix)

	standbyTimeout := 10 * time.Second
	nextStatus := time.Now().Add(standbyTimeout)

	for {
		if time.Now().After(nextStatus) {
			status := pglogrepl.StandbyStatusUpdate{WALWritePosition: clientLSN, WALFlushPosition: clientLSN, WALApplyPosition: clientLSN}
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn, status); err != nil {
				log.Fatalf("send standby status: %v", err)
			}
			nextStatus = time.Now().Add(standbyTimeout)
		}

		deadlineCtx, cancel := context.WithDeadline(ctx, nextStatus)
		rawMsg, err := replConn.ReceiveMessage(deadlineCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalf("receive replication message: %v", err)
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.ErrorResponse:
			log.Fatalf("wal error: %+v", msg)
		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				continue
			}
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Printf("parse keepalive: %v", err)
					continue
				}
				if pkm.ReplyRequested {
					nextStatus = time.Time{}
				}
				if pkm.ServerWALEnd > clientLSN {
					clientLSN = pkm.ServerWALEnd
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Printf("parse xlog: %v", err)
					continue
				}
				handleXLog(&xld, cfg.Prefix)
				clientLSN = xld.WALStart
			default:
				log.Printf("unknown copydata message %x", msg.Data[0])
			}
		default:
			log.Printf("unexpected message: %T", rawMsg)
		}
	}
}

func loadConfig() config {
	app := envOr("PG_DSN", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	repl := envOr("PG_REPL_DSN", "postgres://postgres:postgres@localhost:5432/postgres?replication=database")
	return config{
		AppConnString:  app,
		ReplConnString: repl,
		Slot:           envOr("PG_SLOT", "wal_demo_slot"),
		Publication:    envOr("PG_PUBLICATION", "wal_demo_pub"),
		Prefix:         envOr("PG_MESSAGE_PREFIX", "wal_demo"),
	}
}

func ensurePublication(ctx context.Context, pool *pgxpool.Pool, publication string) error {
	ddl := fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", quoteIdent(publication))
	_, err := pool.Exec(ctx, ddl)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42710" {
			return nil
		}
	}
	return err
}

func ensureSlot(ctx context.Context, pool *pgxpool.Pool, slot string) error {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slot).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	var created string
	var lsn string
	if err := pool.QueryRow(ctx, "SELECT slot_name, lsn FROM pg_create_logical_replication_slot($1, 'pgoutput')", slot).Scan(&created, &lsn); err != nil {
		return err
	}
	log.Printf("created replication slot %s at LSN %s", created, lsn)
	return nil
}

func handleXLog(xld *pglogrepl.XLogData, prefix string) {
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		log.Printf("parse logical msg: %v", err)
		return
	}
	if msg, ok := logicalMsg.(*pglogrepl.LogicalDecodingMessage); ok {
		if msg.Prefix != prefix {
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(msg.Content, &payload); err != nil {
			log.Printf("decode payload: %v raw=%q", err, string(msg.Content))
			return
		}
		log.Printf("received logical message lsn=%s payload=%v", xld.WALStart, payload)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func quoteIdent(id string) string {
	esc := strings.ReplaceAll(id, "\"", "\"\"")
	return "\"" + esc + "\""
}
