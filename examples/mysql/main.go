package main

import (
	"context"

	"github.com/omaskery/outboxen/pkg/fake"
	"github.com/omaskery/outboxen/pkg/outbox"
	"gorm.io/gorm"

	"github.com/omaskery/outboxen-gorm/pkg/storage"

	"golang.org/x/sync/errgroup"
	"gorm.io/driver/mysql"
)

func main() {
	dsn := /* some DSN, e.g.: */ "user:password@tcp(localhost:4000)/database?key=value"
	var publisher outbox.Publisher = /* some messaging implementation would go here */ &fake.Publisher{}
	processorID := /* uniquely identify this running process */ "actually-unique-processor-id"

	// https://github.com/go-sql-driver/mysql
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	storageImpl := storage.New(db)
	ob, err := outbox.New(outbox.Config{
		Storage:     storageImpl,
		Publisher:   publisher,
		ProcessorID: processorID,
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start the outbox's processing task in the background
	group := errgroup.Group{}
	group.Go(func() error {
		return ob.StartProcessing(ctx)
	})
	defer func() {
		cancel()
		if err := group.Wait(); err != nil {
			panic(err)
		}
	}()

	err = db.Transaction(func(tx *gorm.DB) error {
		// update your internal state using this transaction
		// and create the messages you want to publish
		messages := []outbox.Message{
			{
				Key:     []byte("example-message-key"),
				Payload: []byte("example-message-payload"),
			},
		}

		// write your messages to the outbox as part of the same
		// transaction, will be published later by the outbox
		return storageImpl.Queue(context.TODO(), tx, messages...)
	})
	if err != nil {
		panic(err)
	}
}
