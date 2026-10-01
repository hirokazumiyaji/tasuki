module github.com/hirokazumiyaji/tasuki/backend/mysql

go 1.27

require (
	github.com/go-sql-driver/mysql v1.9.2
	github.com/hirokazumiyaji/tasuki v0.0.0-00010101000000-000000000000
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel v1.35.0 // indirect
	go.opentelemetry.io/otel/metric v1.35.0 // indirect
	go.opentelemetry.io/otel/trace v1.35.0 // indirect
)

replace github.com/hirokazumiyaji/tasuki => ../..
