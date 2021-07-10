package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/omaskery/outboxen/pkg/fake"
	"github.com/omaskery/outboxen/pkg/outbox"
	"go.uber.org/zap"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/omaskery/outboxen-gorm/pkg/storage"

	"golang.org/x/sync/errgroup"
	"gorm.io/driver/mysql"
)

// not a great logger, but will do for example purposes
type gormLogger struct {
	Logger logr.Logger
}

func (g *gormLogger) Printf(s string, i ...interface{}) {
	g.Logger.Info(fmt.Sprintf(s, i...))
}

var _ gormlogger.Writer = (*gormLogger)(nil)

func main() {
	zapLogger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	logger := zapr.NewLogger(zapLogger).WithName("example")

	logger.Info("starting mysql example")
	defer logger.Info("exiting mysql example")

	dsn := "user:password@tcp(localhost:3306)/example"
	publisher := /* some messaging implementation would go here */ &fake.Publisher{
		Logger: logger.WithName("publisher"),
	}

	// uniquely identify this running process
	processorID, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	logger.Info("connecting to database", "dsn", dsn)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: gormlogger.New(&gormLogger{logger.WithName("gorm")}, gormlogger.Config{
			LogLevel: gormlogger.Info,
		}),
	})
	if err != nil {
		panic(err)
	}

	logger.Info("creating outbox & outbox storage")
	storageImpl := storage.New(db)
	if err := storageImpl.AutoMigrate(); err != nil {
		panic(err)
	}

	ob, err := outbox.New(outbox.Config{
		Storage:     storageImpl,
		Publisher:   publisher,
		ProcessorID: processorID,
		BatchSize:   1, // for demonstration purposes only, forces many database queries
		Logger:      logger.WithName("outbox"),
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start the outbox's processing task in the background
	group := errgroup.Group{}
	group.Go(func() error {
		l := logger.WithName("outbox-goroutine")
		l.Info("outbox processing goroutine starting")
		defer l.Info("outbox processing goroutine exiting")

		return ob.StartProcessing(ctx)
	})
	defer func() {
		cancel()
		logger.Info("waiting for outbox processing goroutine to exit")
		if err := group.Wait(); err != nil {
			panic(err)
		}
	}()

	go func() {
		exitSignal := make(chan os.Signal, 1)
		signal.Notify(exitSignal, syscall.SIGINT, syscall.SIGTERM)

		logger.Info("listening for exit signal")
		select {
		case <-ctx.Done():
		case <-exitSignal:
			logger.Info("exit signal received")
			cancel()
		}
	}()

	publishedCount := 0

outer:
	for {
		logger.Info("writing to outbox")
		err = db.Transaction(func(tx *gorm.DB) error {
			// update your internal state using this transaction
			// and create the messages you want to publish

			var messages []outbox.Message

			count := rand.Intn(50) + 1
			for i := 0; i < count; i++ {
				messages = append(messages, outbox.Message{
					Key:     []byte("example-message-key"),
					Payload: []byte("example-message-payload"),
				})
			}
			logger.Info("queuing messages", "count", len(messages))

			// write your messages to the outbox as part of the same
			// transaction, will be published later by the outbox
			return ob.Publish(ctx, tx, messages...)
		})
		if err != nil {
			logger.Error(err, "failed to write to outbox")
		}

		logger.Info("signalling outbox that it should check for messages")
		ob.WakeProcessor()

		sleepDuration := 5 * time.Second
		logger.Info("sleeping", "duration", sleepDuration)
		select {
		case <-time.After(sleepDuration):
		case <-ctx.Done():
			break outer
		}

		published := publisher.Clear()
		publishedCount += len(published)
		logger.Info("published so far", "count", publishedCount)
	}
}
