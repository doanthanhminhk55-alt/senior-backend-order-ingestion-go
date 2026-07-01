// Command api is the order ingestion service entrypoint.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	appdb "github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/db"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/monitoring"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/queue"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/repository"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/service"
	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	postgresDSN, err := requiredEnv("POSTGRES_DSN")
	if err != nil {
		return err
	}
	redisAddr, err := requiredEnv("REDIS_ADDR")
	if err != nil {
		return err
	}
	workerCount, err := positiveIntEnv("WORKER_COUNT", 4)
	if err != nil {
		return err
	}
	readCount, err := positiveIntEnv("WORKER_READ_COUNT", 10)
	if err != nil {
		return err
	}
	reclaimIntervalSeconds, err := positiveIntEnv("RECLAIM_INTERVAL_SECONDS", 5)
	if err != nil {
		return err
	}
	reclaimMinIdleSeconds, err := positiveIntEnv("RECLAIM_MIN_IDLE_SECONDS", 30)
	if err != nil {
		return err
	}
	reclaimCount, err := positiveIntEnv("RECLAIM_COUNT", readCount)
	if err != nil {
		return err
	}
	addr := envOrDefault("HTTP_ADDR", ":8080")
	consumerPrefix := envOrDefault("REDIS_CONSUMER_PREFIX", "app")
	metricsCollector := monitoring.NewCollector()

	databasePool, err := appdb.NewPool(ctx, postgresDSN)
	if err != nil {
		return err
	}
	defer databasePool.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() {
		_ = redisClient.Close()
	}()

	streamQueue := queue.NewRedisStreamQueue(
		redisClient,
		envOrDefault("REDIS_STREAM", "order-events"),
		envOrDefault("REDIS_CONSUMER_GROUP", "order-processors"),
	)
	if err := streamQueue.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("ensure Redis consumer group: %w", err)
	}

	orderRepository := repository.NewPostgresOrderRepository(databasePool)
	processor := service.NewProcessor(orderRepository)
	workerPool, err := worker.NewPool(
		streamQueue,
		processor,
		worker.Config{
			WorkerCount:    workerCount,
			ConsumerPrefix: consumerPrefix,
			ReadCount:      int64(readCount),
			Block:          2 * time.Second,
		},
		log.Default(),
		metricsCollector,
	)
	if err != nil {
		return fmt.Errorf("create worker pool: %w", err)
	}

	reclaimer, err := worker.NewReclaimer(
		streamQueue,
		processor,
		worker.ReclaimerConfig{
			ConsumerName: consumerPrefix + "-reclaimer",
			Interval: time.Duration(reclaimIntervalSeconds) *
				time.Second,
			MinIdle: time.Duration(reclaimMinIdleSeconds) *
				time.Second,
			Count: int64(reclaimCount),
		},
		log.Default(),
		metricsCollector,
	)
	if err != nil {
		return fmt.Errorf("create pending message reclaimer: %w", err)
	}

	var background sync.WaitGroup
	background.Add(2)
	backgroundDone := make(chan struct{})
	go func() {
		defer background.Done()
		workerPool.Run(ctx)
	}()
	go func() {
		defer background.Done()
		reclaimer.Run(ctx)
	}()
	go func() {
		background.Wait()
		close(backgroundDone)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle(
		"/stats",
		monitoring.NewStatsHandler(
			metricsCollector,
			streamQueue,
			workerCount,
		),
	)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		// TODO: Expose application, Redis Stream, worker, and database metrics.
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("# Monitoring metrics will be added in a later step.\n"))
	})

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
		}
	}()

	log.Printf(
		"API listening on %s with %d order workers",
		server.Addr,
		workerCount,
	)
	serveErr := server.ListenAndServe()
	stop()
	<-backgroundDone

	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("API server failed: %w", serveErr)
	}

	return nil
}

func requiredEnv(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func envOrDefault(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func positiveIntEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}
