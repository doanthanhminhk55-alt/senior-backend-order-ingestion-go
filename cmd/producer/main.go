// Command producer generates order events for load and recovery testing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/doanthanhminhk55-alt/senior-backend-order-ingestion-go/internal/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	config, err := parseConfig(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	if err := run(ctx, config, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func parseConfig(args []string, output io.Writer) (producerConfig, error) {
	config := producerConfig{}
	flags := flag.NewFlagSet("producer", flag.ContinueOnError)
	flags.SetOutput(output)

	flags.IntVar(
		&config.Total,
		"total",
		100_000,
		"total number of events to publish",
	)
	flags.Float64Var(
		&config.DuplicateRatio,
		"duplicate-ratio",
		0.05,
		"fraction of published events that reuse an event ID",
	)
	flags.Float64Var(
		&config.OutOfOrderRatio,
		"out-of-order-ratio",
		0.03,
		"fraction of published events intentionally sent before prerequisites",
	)
	flags.Float64Var(
		&config.InvalidRatio,
		"invalid-ratio",
		0.01,
		"fraction of published events containing invalid transitions",
	)
	flags.StringVar(
		&config.RedisAddr,
		"redis-addr",
		envOrDefault("REDIS_ADDR", "localhost:6379"),
		"Redis server address",
	)
	flags.StringVar(
		&config.Stream,
		"stream",
		envOrDefault("REDIS_STREAM", "order-events"),
		"Redis Stream name",
	)
	flags.StringVar(
		&config.Group,
		"group",
		envOrDefault("REDIS_CONSUMER_GROUP", "order-processors"),
		"Redis consumer group",
	)
	flags.Int64Var(
		&config.Seed,
		"seed",
		1,
		"deterministic generation seed",
	)
	flags.IntVar(
		&config.BatchSize,
		"batch-size",
		100,
		"maximum generated events buffered before publishing",
	)

	if err := flags.Parse(args); err != nil {
		return producerConfig{}, err
	}
	if flags.NArg() != 0 {
		return producerConfig{}, fmt.Errorf(
			"unexpected positional arguments: %v",
			flags.Args(),
		)
	}
	if err := config.validate(); err != nil {
		return producerConfig{}, err
	}

	return config, nil
}

func run(
	ctx context.Context,
	config producerConfig,
	output io.Writer,
) error {
	client := redis.NewClient(&redis.Options{Addr: config.RedisAddr})
	defer func() {
		_ = client.Close()
	}()

	streamQueue := queue.NewRedisStreamQueue(
		client,
		config.Stream,
		config.Group,
	)
	if err := streamQueue.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("ensure Redis consumer group: %w", err)
	}

	summary, err := publishGenerated(
		ctx,
		streamQueue,
		config,
		output,
	)
	if err != nil {
		return err
	}

	printSummary(output, summary)
	return nil
}

func envOrDefault(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
