PATH_CLUSTER = github.com/SimonRichardson/cluster

UNAME_S := $(shell uname -s)
SED ?= sed -i
ifeq ($(UNAME_S),Darwin)
	SED += '' --
endif

.PHONY: all
all:
	go get github.com/Masterminds/glide
	glide install --strip-vendor

pkg/ingester/mocks/queue.go:
	mockgen -package=mocks -destination=pkg/ingester/mocks/queue.go ${PATH_CLUSTER}/pkg/ingester Queue
	$(SED) 's/github.com\/SimonRichardson\/cluster\/vendor\///g' ./pkg/ingester/mocks/queue.go

pkg/members/mocks/members.go:
	mockgen -package=mocks -destination=pkg/members/mocks/members.go ${PATH_CLUSTER}/pkg/members Members,MemberList,Member
	$(SED) 's/github.com\/SimonRichardson\/cluster\/vendor\///g' ./pkg/members/mocks/members.go

pkg/metrics/mocks/metrics.go:
	mockgen -package=mocks -destination=pkg/metrics/mocks/metrics.go ${PATH_CLUSTER}/pkg/metrics Gauge,HistogramVec,Counter
	$(SED) 's/github.com\/SimonRichardson\/cluster\/vendor\///g' ./pkg/metrics/mocks/metrics.go

pkg/metrics/mocks/observer.go:
	mockgen -package=mocks -destination=pkg/metrics/mocks/observer.go github.com/prometheus/client_golang/prometheus Observer

.PHONY: build-mocks
build-mocks: FORCE
	$(MAKE) pkg/ingester/mocks/queue.go
	$(MAKE) pkg/members/mocks/members.go
	$(MAKE) pkg/metrics/mocks/metrics.go
	$(MAKE) pkg/metrics/mocks/observer.go

.PHONY: clean-mocks
clean-mocks: FORCE
	rm -f pkg/members/mocks/members.go
	rm -f pkg/metrics/mocks/metrics.go
	rm -f pkg/metrics/mocks/observer.go

FORCE:

.PHONY: integration-tests
integration-tests:
	docker-compose run cluster go test -v -tags=integration ./cmd/... ./pkg/...