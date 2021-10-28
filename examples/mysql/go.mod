module mysql

go 1.16

replace github.com/omaskery/outboxen-gorm => ../../

require (
	github.com/go-logr/logr v0.4.0
	github.com/go-logr/zapr v0.4.0
	github.com/omaskery/outboxen v0.3.2
	github.com/omaskery/outboxen-gorm v0.3.1
	go.uber.org/zap v1.19.1
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	gorm.io/driver/mysql v1.1.3
	gorm.io/gorm v1.21.15
)
