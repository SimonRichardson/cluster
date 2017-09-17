PATH_SNOWY = github.com/SimonRichardson/cluster

UNAME_S := $(shell uname -s)
SED ?= sed -i
ifeq ($(UNAME_S),Darwin)
	SED += '' --
endif

.PHONY: all
all:
	go get github.com/Masterminds/glide
	glide install --strip-vendor

pkg/members/mocks/members.go:
	mockgen -package=mocks -destination=pkg/members/mocks/members.go ${PATH_SNOWY}/pkg/members Members,MemberList,Member
	$(SED) 's/github.com\/SimonRichardson\/cluster\/vendor\///g' ./pkg/members/mocks/members.go

.PHONY: build-mocks
build-mocks: FORCE
	$(MAKE) pkg/members/mocks/members.go

.PHONY: clean-mocks
clean-mocks: FORCE
	rm -f pkg/members/mocks/members.go

FORCE:

.PHONY: integration-tests
integration-tests:
	docker-compose run cluster go test -v -tags=integration ./cmd/... ./pkg/...