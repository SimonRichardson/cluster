
.PHONY: all
all:
	go get github.com/Masterminds/glide
	glide install --strip-vendor
