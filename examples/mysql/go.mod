module mysql

go 1.16

replace github.com/omaskery/outboxen-gorm => ../../

require (
	github.com/go-logr/logr v0.4.0
	github.com/go-logr/zapr v0.4.0
	github.com/omaskery/outboxen v0.3.0
	github.com/omaskery/outboxen-gorm v0.0.0-20210522125804-9bb8492d3fd5
	go.uber.org/zap v1.16.0
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	gorm.io/driver/mysql v1.1.0
	gorm.io/gorm v1.21.10
)
