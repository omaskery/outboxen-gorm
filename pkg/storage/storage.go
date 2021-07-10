package storage

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/omaskery/outboxen/pkg/outbox"
	"gorm.io/gorm"
)

// IDGenerator allows for customising how outbox entry IDs are generated
type IDGenerator interface {
	// GenerateID is called to generate a unique outbox entry ID
	GenerateID(clock clockwork.Clock, message outbox.Message) string
}

// Storage implements outboxen's outbox.ProcessorStorage interface, allowing the outbox.Outbox to
// process outbox entries and reliably publish them to some external messaging system
type Storage struct {
	IDGenerator IDGenerator
	Clock       clockwork.Clock

	GetTxn func(ctx context.Context) *gorm.DB

	db *gorm.DB
}

// UUIDGenerator generates IDs by producing random UUIDs
type UUIDGenerator struct{}

// GenerateID implements the IDGenerator interface
func (u *UUIDGenerator) GenerateID(clockwork.Clock, outbox.Message) string {
	return uuid.NewString()
}

// New constructs a new Storage instance with default settings
func New(db *gorm.DB) *Storage {
	return &Storage{
		IDGenerator: &UUIDGenerator{},
		Clock:       clockwork.NewRealClock(),
		db:          db,
	}
}

// ClaimEntries atomically claims all claimable entries for the specified processor
// Will claim any unclaimed entries (empty processor ID or lack of processing deadline)
//            entries previously claimed by this processor (same processor ID)
//            entries whose claim is now out of date
func (s *Storage) ClaimEntries(ctx context.Context, processorID string, claimDeadline time.Time) error {
	now := s.Clock.Now()
	return s.db.Transaction(func(tx *gorm.DB) error {
		return s.db.WithContext(ctx).
			Model(OutboxEntry{}).
			Where(`processor_id = "" OR processor_id = ? OR processing_deadline IS NULL OR processing_deadline < ?`, processorID, now).
			Updates(&OutboxEntry{
				ProcessorID:        processorID,
				ProcessingDeadline: &claimDeadline,
			}).
			Error
	})
}

// GetClaimedEntries retrieves entries claimed by this processor, up to the specified batch size
func (s *Storage) GetClaimedEntries(ctx context.Context, processorID string, batchSize int) ([]outbox.ClaimedEntry, error) {
	var rows []OutboxEntry

	result := s.db.WithContext(ctx).
		Model(OutboxEntry{}).
		Select("id", "key", "payload").
		Where("processor_id = ?", processorID).
		Limit(batchSize).
		Find(&rows)
	if result.Error != nil {
		return nil, result.Error
	}

	results := make([]outbox.ClaimedEntry, 0, len(rows))
	for _, r := range rows {
		results = append(results, outbox.ClaimedEntry{
			ID:      r.ID,
			Key:     r.Key,
			Payload: r.Payload,
		})
	}

	return results, nil
}

// DeleteEntries deletes all entries specified (by their ID)
func (s *Storage) DeleteEntries(ctx context.Context, entryIDs ...string) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		return s.db.WithContext(ctx).Delete(OutboxEntry{}, "id IN ?", entryIDs).Error
	})
}

// Publish will write messages to the outbox for later publishing, using the transaction stored in the context
func (s *Storage) Publish(ctx context.Context, txn interface{}, messages ...outbox.Message) error {
	tx, ok := txn.(*gorm.DB)
	if !ok {
		return errors.New("failed to obtain database transaction from context")
	}

	return s.publish(ctx, tx, messages...)
}

var _ outbox.ProcessorStorage = (*Storage)(nil)

// AutoMigrate runs GORM's auto migrator on the outbox table
func (s *Storage) AutoMigrate() error {
	return s.db.AutoMigrate(OutboxEntry{})
}

// publish writes the provided messages to the outbox as part of the provided transaction.
// Once the transaction is committed the messages will be eventually processed by the outbox
// and published.
func (s *Storage) publish(ctx context.Context, txn *gorm.DB, messages ...outbox.Message) error {
	rows := make([]OutboxEntry, 0, len(messages))
	for _, m := range messages {
		rows = append(rows, OutboxEntry{
			ID:      s.IDGenerator.GenerateID(s.Clock, m),
			Key:     m.Key,
			Payload: m.Payload,
		})
	}

	return txn.WithContext(ctx).Create(&rows).Error
}

// OutboxEntry is the internal representation of an outbox entry
type OutboxEntry struct {
	// ID uniquely identifies this outbox entry
	ID string
	// Key that will be passed to the outbox's publisher
	Key []byte
	// Payload that will be passed to the outbox's publisher
	Payload []byte
	// ProcessorID identifies the processor that currently has a claim on this outbox entry
	ProcessorID string
	// ProcessingDeadline is when any outstanding claim to this outbox entry expires
	ProcessingDeadline *time.Time
}
