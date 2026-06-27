package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

type MaxwellEvent struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Type     string                 `json:"type"`
	Data     map[string]interface{} `json:"data"`
	Old      map[string]interface{} `json:"old"`
	XID      interface{}            `json:"xid,omitempty"`
	TS       interface{}            `json:"ts,omitempty"`
}

type Config struct {
	RedisAddr      string
	RedisPassword  string
	RedisStream    string
	RedisGroup     string
	RedisConsumer  string
	FailedStream   string
	MySQLDSN       string
	TargetDatabase string
	MaxRetries     int
	BlockSeconds   int
	PendingIdleMS  int64
	BatchCount     int64
	StopOnCritical bool
	PrimaryKeyMap  map[string]string
	AllowedDBs     map[string]bool
	AllowedTables  map[string]bool
	HealthAddr     string
}

var ready atomic.Bool

func getenv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func atoi(v string) int {
	var i int
	_, _ = fmt.Sscanf(v, "%d", &i)
	if i <= 0 {
		return 1
	}
	return i
}

func parseMap(s string) map[string]string {
	m := map[string]string{}

	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" || !strings.Contains(item, ":") {
			continue
		}

		parts := strings.SplitN(item, ":", 2)
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		if key != "" && val != "" {
			m[key] = val
		}
	}

	return m
}

func parseSet(s string) map[string]bool {
	m := map[string]bool{}

	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			m[item] = true
		}
	}

	return m
}

func loadConfig() Config {
	targetHost := getenv("TARGET_DB_HOST", "127.0.0.1")
	targetPort := getenv("TARGET_DB_PORT", "3306")
	targetUser := getenv("TARGET_DB_USER", "")
	targetPass := getenv("TARGET_DB_PASSWORD", "")
	targetDB := getenv("TARGET_DB_NAME", "")

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&multiStatements=false&timeout=10s&readTimeout=30s&writeTimeout=30s",
		targetUser,
		targetPass,
		targetHost,
		targetPort,
		targetDB,
	)

	return Config{
		RedisAddr:      getenv("REDIS_HOST", "redis-cdc.cdc-db-prod.svc.cluster.local") + ":" + getenv("REDIS_PORT", "6379"),
		RedisPassword:  getenv("REDIS_PASSWORD", ""),
		RedisStream:    getenv("REDIS_STREAM", "maxwell:cdc:stream"),
		RedisGroup:     getenv("REDIS_GROUP", "taskmanagement-read-sync"),
		RedisConsumer:  getenv("REDIS_CONSUMER", "consumer-1"),
		FailedStream:   getenv("FAILED_STREAM", "maxwell:cdc:failed"),
		MySQLDSN:       dsn,
		TargetDatabase: targetDB,
		MaxRetries:     atoi(getenv("MAX_RETRIES", "5")),
		BlockSeconds:   atoi(getenv("BLOCK_SECONDS", "5")),
		PendingIdleMS:  int64(atoi(getenv("PENDING_IDLE_MS", "60000"))),
		BatchCount:     int64(atoi(getenv("BATCH_COUNT", "10"))),
		StopOnCritical: strings.EqualFold(getenv("STOP_ON_CRITICAL", "true"), "true"),
		PrimaryKeyMap:  parseMap(getenv("PRIMARY_KEY_MAP", "users:id")),
		AllowedDBs:     parseSet(getenv("ALLOWED_DATABASES", "")),
		AllowedTables:  parseSet(getenv("ALLOWED_TABLES", "")),
		HealthAddr:     getenv("HEALTH_ADDR", ":8080"),
	}
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func pkFor(cfg Config, table string) string {
	if pk, ok := cfg.PrimaryKeyMap[table]; ok && pk != "" {
		return pk
	}
	return "id"
}

func redisClient(cfg Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           0,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})
}

func mysqlDB(cfg Config) (*sql.DB, error) {
	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func ensureGroup(ctx context.Context, rdb *redis.Client, cfg Config) error {
	err := rdb.XGroupCreateMkStream(ctx, cfg.RedisStream, cfg.RedisGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func parseEvent(values map[string]interface{}) (MaxwellEvent, error) {
	var raw string

	for _, key := range []string{"message", "data", "event"} {
		if v, ok := values[key]; ok {
			raw = fmt.Sprint(v)
			break
		}
	}

	if raw == "" {
		b, _ := json.Marshal(values)
		raw = string(b)
	}

	var ev MaxwellEvent
	if err := json.Unmarshal([]byte(raw), &ev); err != nil {
		return ev, err
	}

	return ev, nil
}

func isCriticalError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())

	patterns := []string{
		"unknown column",
		"foreign key constraint fails",
		"cannot add or update a child row",
		"doesn't exist",
		"duplicate column",
		"no such table",
	}

	for _, p := range patterns {
		if strings.Contains(msg, p) {
			return true
		}
	}

	return false
}

func saveFailed(ctx context.Context, rdb *redis.Client, cfg Config, msg redis.XMessage, ev MaxwellEvent, applyErr error, critical bool) error {
	eventJSON, _ := json.Marshal(ev)
	fieldsJSON, _ := json.Marshal(msg.Values)

	return rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: cfg.FailedStream,
		Values: map[string]interface{}{
			"original_id": msg.ID,
			"critical":    fmt.Sprintf("%t", critical),
			"error":       applyErr.Error(),
			"event":       string(eventJSON),
			"fields":      string(fieldsJSON),
			"failed_at":   time.Now().UTC().Format(time.RFC3339),
		},
	}).Err()
}

func ackMessage(ctx context.Context, rdb *redis.Client, cfg Config, msgID string) error {
	return rdb.XAck(ctx, cfg.RedisStream, cfg.RedisGroup, msgID).Err()
}

func saveFailedAndAck(ctx context.Context, rdb *redis.Client, cfg Config, msg redis.XMessage, ev MaxwellEvent, err error, critical bool) error {
	if saveErr := saveFailed(ctx, rdb, cfg, msg, ev, err, critical); saveErr != nil {
		return fmt.Errorf("failed to write failed-stream before ack: %w", saveErr)
	}

	if ackErr := ackMessage(ctx, rdb, cfg, msg.ID); ackErr != nil {
		return fmt.Errorf("failed to ack message after failed-stream save: %w", ackErr)
	}

	log.Printf("[FAILED_ACK] id=%s critical=%t error=%v", msg.ID, critical, err)
	return nil
}

func applyEvent(ctx context.Context, db *sql.DB, cfg Config, ev MaxwellEvent) error {
	if ev.Table == "" {
		return nil
	}

	if len(cfg.AllowedDBs) > 0 && !cfg.AllowedDBs[ev.Database] {
		return nil
	}

	if len(cfg.AllowedTables) > 0 && !cfg.AllowedTables[ev.Table] {
		return nil
	}

	switch ev.Type {
	case "insert", "bootstrap-insert", "update":
		return upsert(ctx, db, cfg, ev.Table, ev.Data)
	case "delete":
		return deleteRow(ctx, db, cfg, ev.Table, ev.Data)
	default:
		return nil
	}
}

func upsert(ctx context.Context, db *sql.DB, cfg Config, table string, data map[string]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	pk := pkFor(cfg, table)

	cols := make([]string, 0, len(data))
	vals := make([]interface{}, 0, len(data))
	placeholders := make([]string, 0, len(data))

	for k, v := range data {
		cols = append(cols, k)
		vals = append(vals, v)
		placeholders = append(placeholders, "?")
	}

	insertCols := make([]string, 0, len(cols))
	updateParts := make([]string, 0, len(cols))

	for _, c := range cols {
		insertCols = append(insertCols, quoteIdent(c))

		if c != pk {
			updateParts = append(
				updateParts,
				fmt.Sprintf("%s=VALUES(%s)", quoteIdent(c), quoteIdent(c)),
			)
		}
	}

	if len(updateParts) == 0 {
		updateParts = append(
			updateParts,
			fmt.Sprintf("%s=VALUES(%s)", quoteIdent(pk), quoteIdent(pk)),
		)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		quoteIdent(table),
		strings.Join(insertCols, ","),
		strings.Join(placeholders, ","),
		strings.Join(updateParts, ","),
	)

	_, err := db.ExecContext(ctx, query, vals...)
	return err
}

func deleteRow(ctx context.Context, db *sql.DB, cfg Config, table string, data map[string]interface{}) error {
	pk := pkFor(cfg, table)

	v, ok := data[pk]
	if !ok {
		return fmt.Errorf("delete event missing primary key %s for table %s", pk, table)
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s = ?",
		quoteIdent(table),
		quoteIdent(pk),
	)

	_, err := db.ExecContext(ctx, query, v)
	return err
}

func retrySleep(ctx context.Context, attempt int) bool {
	timer := time.NewTimer(time.Duration(attempt*3) * time.Second)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func processMessage(ctx context.Context, rdb *redis.Client, db *sql.DB, cfg Config, msg redis.XMessage) error {
	ev, err := parseEvent(msg.Values)
	if err != nil {
		if failErr := saveFailedAndAck(ctx, rdb, cfg, msg, ev, err, false); failErr != nil {
			return failErr
		}
		return nil
	}

	for attempt := 1; attempt <= cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err = applyEvent(ctx, db, cfg, ev)
		if err == nil {
			if ackErr := ackMessage(ctx, rdb, cfg, msg.ID); ackErr != nil {
				return ackErr
			}

			log.Printf("[ACK] id=%s db=%s table=%s type=%s", msg.ID, ev.Database, ev.Table, ev.Type)
			return nil
		}

		log.Printf(
			"[ERROR] id=%s attempt=%d db=%s table=%s type=%s error=%v",
			msg.ID,
			attempt,
			ev.Database,
			ev.Table,
			ev.Type,
			err,
		)

		if isCriticalError(err) {
			if failErr := saveFailedAndAck(ctx, rdb, cfg, msg, ev, err, true); failErr != nil {
				return failErr
			}

			if cfg.StopOnCritical {
				return fmt.Errorf("critical CDC apply error after failed-stream save and ack: %w", err)
			}

			return nil
		}

		if attempt < cfg.MaxRetries {
			if ok := retrySleep(ctx, attempt); !ok {
				return ctx.Err()
			}
		}
	}

	if err == nil {
		err = errors.New("max retries exceeded")
	} else {
		err = fmt.Errorf("max retries exceeded: %w", err)
	}

	if failErr := saveFailedAndAck(ctx, rdb, cfg, msg, ev, err, false); failErr != nil {
		return failErr
	}

	return nil
}

func reclaimPending(ctx context.Context, rdb *redis.Client, cfg Config) ([]redis.XMessage, error) {
	res := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   cfg.RedisStream,
		Group:    cfg.RedisGroup,
		Consumer: cfg.RedisConsumer,
		MinIdle:  time.Duration(cfg.PendingIdleMS) * time.Millisecond,
		Start:    "0-0",
		Count:    cfg.BatchCount,
	})

	msgs, _, err := res.Result()
	return msgs, err
}

func readNew(ctx context.Context, rdb *redis.Client, cfg Config) ([]redis.XMessage, error) {
	streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    cfg.RedisGroup,
		Consumer: cfg.RedisConsumer,
		Streams:  []string{cfg.RedisStream, ">"},
		Count:    cfg.BatchCount,
		Block:    time.Duration(cfg.BlockSeconds) * time.Second,
	}).Result()

	if err == redis.Nil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	var msgs []redis.XMessage
	for _, s := range streams {
		msgs = append(msgs, s.Messages...)
	}

	return msgs, nil
}

func serveHealth(ctx context.Context, cfg Config) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("# minimal metrics endpoint enabled\n"))
	})

	srv := &http.Server{
		Addr:              cfg.HealthAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("[HTTP] health server listening on %s", cfg.HealthAddr)

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[HTTP_ERROR] %v", err)
		}
	}()

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("[HTTP_SHUTDOWN_ERROR] %v", err)
		}
	}()

	return srv
}

func main() {
	cfg := loadConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveHealth(ctx, cfg)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sig
		log.Println("[SHUTDOWN] signal received")
		ready.Store(false)
		cancel()
	}()

	rdb := redisClient(cfg)
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("[REDIS_CLOSE_ERROR] %v", err)
		}
	}()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[REDIS_CONNECT_FAILED] %v", err)
	}

	if err := ensureGroup(ctx, rdb, cfg); err != nil {
		log.Fatalf("[REDIS_GROUP_FAILED] %v", err)
	}

	db, err := mysqlDB(cfg)
	if err != nil {
		log.Fatalf("[MYSQL_CONNECT_FAILED] %v", err)
	}
	defer db.Close()

	ready.Store(true)

	log.Println("[STARTED] Maxwell Redis Stream consumer")
	log.Printf("[CONFIG] stream=%s group=%s consumer=%s failed_stream=%s batch=%d pending_idle_ms=%d",
		cfg.RedisStream,
		cfg.RedisGroup,
		cfg.RedisConsumer,
		cfg.FailedStream,
		cfg.BatchCount,
		cfg.PendingIdleMS,
	)

	for ctx.Err() == nil {
		pending, err := reclaimPending(ctx, rdb, cfg)
		if err != nil {
			if ctx.Err() != nil {
				break
			}

			log.Printf("[PENDING_ERROR] %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if len(pending) > 0 {
			for _, msg := range pending {
				if ctx.Err() != nil {
					break
				}

				if err := processMessage(ctx, rdb, db, cfg, msg); err != nil {
					ready.Store(false)
					log.Fatalf("[FATAL] %v", err)
				}
			}

			continue
		}

		msgs, err := readNew(ctx, rdb, cfg)
		if err != nil {
			if ctx.Err() != nil {
				break
			}

			log.Printf("[READ_ERROR] %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, msg := range msgs {
			if ctx.Err() != nil {
				break
			}

			if err := processMessage(ctx, rdb, db, cfg, msg); err != nil {
				ready.Store(false)
				log.Fatalf("[FATAL] %v", err)
			}
		}
	}

	ready.Store(false)
	log.Println("[STOPPED] graceful shutdown complete")
}
