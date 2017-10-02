PATH_CLUSTER = github.com/SimonRichardson/cluster

UNAME_S := $(shell uname -s)
SED ?= sed -i
ifeq ($(UNAME_S),Darwin)
	SED += '' --
endif

.PHONY: install
install:
	go get github.com/Masterminds/glide
	glide install --strip-vendor

pkg/clients/mocks/client.go:
	mockgen -package=mocks -destination=pkg/clients/mocks/client.go ${PATH_CLUSTER}/pkg/clients Client,Response
	$(SED) 's/github.com\/SimonRichardson\/clients\/vendor\///g' ./pkg/clients/mocks/client.go

pkg/cluster/mocks/cluster.go:
	mockgen -package=mocks -destination=pkg/cluster/mocks/cluster.go ${PATH_CLUSTER}/pkg/cluster Peer
	$(SED) 's/github.com\/SimonRichardson\/cluster\/vendor\///g' ./pkg/cluster/mocks/cluster.go

pkg/members/mocks/members.go:
	mockgen -package=mocks -destination=pkg/members/mocks/members.go ${PATH_CLUSTER}/pkg/members Members,MemberList,Member
	$(SED) 's/github.com\/SimonRichardson\/cluster\/vendor\///g' ./pkg/members/mocks/members.go

pkg/metrics/mocks/metrics.go:
	mockgen -package=mocks -destination=pkg/metrics/mocks/metrics.go ${PATH_CLUSTER}/pkg/metrics Gauge,HistogramVec,Counter
	$(SED) 's/github.com\/SimonRichardson\/cluster\/vendor\///g' ./pkg/metrics/mocks/metrics.go

pkg/metrics/mocks/observer.go:
	mockgen -package=mocks -destination=pkg/metrics/mocks/observer.go github.com/prometheus/client_golang/prometheus Observer

pkg/queue/mocks/queue.go:
	mockgen -package=mocks -destination=pkg/queue/mocks/queue.go ${PATH_CLUSTER}/pkg/queue Queue
	$(SED) 's/github.com\/SimonRichardson\/cluster\/vendor\///g' ./pkg/queue/mocks/queue.go

.PHONY: build-mocks
build-mocks: FORCE
	$(MAKE) pkg/clients/mocks/client.go
	$(MAKE) pkg/cluster/mocks/cluster.go
	$(MAKE) pkg/members/mocks/members.go
	$(MAKE) pkg/metrics/mocks/metrics.go
	$(MAKE) pkg/metrics/mocks/observer.go
	$(MAKE) pkg/queue/mocks/queue.go

.PHONY: clean-mocks
clean-mocks: FORCE
	rm -f pkg/clients/mocks/client.go
	rm -f pkg/cluster/mocks/cluster.go
	rm -f pkg/members/mocks/members.go
	rm -f pkg/metrics/mocks/metrics.go
	rm -f pkg/metrics/mocks/observer.go
	rm -f pkg/queue/mocks/queue.go

FORCE:

.PHONY: integration-tests
integration-tests:
	docker-compose run cluster go test -v -tags=integration ./cmd/... ./pkg/...
