module mysql

go 1.17

replace github.com/omaskery/outboxen-gorm => ../../

require (
	github.com/go-logr/logr v0.4.0
	github.com/go-logr/zapr v0.4.0
	github.com/omaskery/outboxen v0.4.0
	github.com/omaskery/outboxen-gorm v0.3.1
	go.uber.org/zap v1.19.1
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	gorm.io/driver/mysql v1.2.2
	gorm.io/gorm v1.24.4
)

require (
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/go-sql-driver/mysql v1.6.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.4 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
)
