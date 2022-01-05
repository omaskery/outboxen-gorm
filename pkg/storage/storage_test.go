package storage_test

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/omaskery/outboxen/pkg/outbox"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/omaskery/outboxen-gorm/pkg/storage"
)

const (
	PrimaryProcessorID   = "primary-processor-id"
	SecondaryProcessorID = "secondary-processor-id"

	TestClaimDuration = 10 * time.Second

	testNamespace = "test-namespace"
)

var _ = Describe("Storage", func() {
	var clock clockwork.FakeClock
	var store *storage.Storage
	var ctx context.Context
	var tempDir string
	var db *gorm.DB
	var err error

	BeforeEach(func() {
		log.SetOutput(GinkgoWriter)
		ctx = context.Background()

		clock = clockwork.NewFakeClock()

		tempDir, err = ioutil.TempDir("", "tests-")
		Expect(err).To(Succeed())
		log.Printf("created temporary directory file://%s", tempDir)

		dbPath := path.Join(tempDir, "test.db")
		log.Printf("creating test database at file://%s", dbPath)
		db, err = gorm.Open(sqlite.Open(dbPath), &gorm.Config{
			Logger: logger.New(log.New(GinkgoWriter, "\r\n", log.LstdFlags), logger.Config{
				SlowThreshold: 100,
				Colorful:      false,
				LogLevel:      logger.Info,
			}),
		})
		Expect(err).To(Succeed())

		Expect(db.WithContext(ctx).Exec("pragma busy_timeout = 30000;").Error).To(Succeed())

		store = storage.New(db)
		store.Clock = clock
	})

	AfterEach(func() {
		store = nil

		log.Printf("closing database")
		rawDB, err := db.DB()
		Expect(err).To(Succeed())
		Expect(rawDB.Close()).To(Succeed())

		log.Printf("removing temporary directory file://%s", tempDir)
		Expect(os.RemoveAll(tempDir)).To(Succeed())
	})

	When("the storage tables have been migrated", func() {
		JustBeforeEach(func() {
			log.Printf("running auto migration")
			err = store.AutoMigrate()
		})

		It("succeeds at the auto migration", func() {
			Expect(err).To(Succeed())
		})

		When("there are no outbox entries", func() {
			JustBeforeEach(func() {
				log.Printf("claiming entries")
				Expect(store.ClaimEntries(ctx, PrimaryProcessorID, clock.Now().Add(TestClaimDuration))).To(Succeed())
			})

			It("returns no claimed entries", func() {
				Expect(store.GetClaimedEntries(ctx, PrimaryProcessorID, 10)).To(BeEmpty())
			})
		})

		When("a single outbox entry is written", func() {
			JustBeforeEach(func() {
				log.Printf("queuing message")
				err = db.Transaction(func(tx *gorm.DB) error {
					ctx := outbox.WithNamespace(ctx, testNamespace)
					return store.Publish(ctx, tx, outbox.Message{
						Key:     []byte("test-key"),
						Payload: []byte("test-payload"),
					})
				})
			})

			It("successfully writes the outbox entry", func() {
				Expect(err).To(Succeed())
			})

			It("returns no claimed entries prior to claiming", func() {
				Expect(store.GetClaimedEntries(ctx, PrimaryProcessorID, 10)).To(BeEmpty())
			})

			When("the entries are claimed", func() {
				JustBeforeEach(func() {
					Expect(store.ClaimEntries(ctx, PrimaryProcessorID, clock.Now().Add(TestClaimDuration))).To(Succeed())
				})

				It("returns a claimed entry", func() {
					claimed, err := store.GetClaimedEntries(ctx, PrimaryProcessorID, 10)
					Expect(err).To(Succeed())
					Expect(claimed).To(HaveLen(1))
					Expect(claimed[0].Namespace).To(Equal(testNamespace))
					Expect(claimed[0].Key).To(Equal([]byte("test-key")))
					Expect(claimed[0].Payload).To(Equal([]byte("test-payload")))
				})

				It("continues to return the claimed entry if not deleted", func() {
					Expect(store.GetClaimedEntries(ctx, PrimaryProcessorID, 10)).To(HaveLen(1))
					Expect(store.GetClaimedEntries(ctx, PrimaryProcessorID, 10)).To(HaveLen(1))
				})

				It("prevents other processors from claiming at the same time", func() {
					Expect(store.ClaimEntries(ctx, SecondaryProcessorID, clock.Now().Add(TestClaimDuration))).To(Succeed())
					Expect(store.GetClaimedEntries(ctx, SecondaryProcessorID, 10)).To(BeEmpty())
				})

				When("the claim expires", func() {
					JustBeforeEach(func() {
						clock.Advance(TestClaimDuration * 2)
					})

					It("allows other processors to claim expired entries", func() {
						Expect(store.ClaimEntries(ctx, SecondaryProcessorID, clock.Now().Add(TestClaimDuration))).To(Succeed())
						Expect(store.GetClaimedEntries(ctx, SecondaryProcessorID, 10)).To(HaveLen(1))
					})
				})

				When("the entries are deleted", func() {
					JustBeforeEach(func() {
						claimed, err := store.GetClaimedEntries(ctx, PrimaryProcessorID, 10)
						Expect(err).To(Succeed())

						ids := make([]string, 0, len(claimed))
						for _, c := range claimed {
							ids = append(ids, c.ID)
						}

						err = store.DeleteEntries(ctx, ids...)
					})

					It("successfully deletes the entries", func() {
						Expect(err).To(Succeed())
					})

					It("no longer returns claimed entries", func() {
						Expect(store.GetClaimedEntries(ctx, PrimaryProcessorID, 10)).To(BeEmpty())
					})
				})
			})
		})
	})
})
