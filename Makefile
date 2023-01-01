export GOPATH := $(shell pwd)

.PHONY: all clean test

PKG  = wkk/common/crc128
PKG += wkk/common/perm
PKG += wkk/common/siphash
PKG += wkk/network
PKG += wkk/rubiks/api
PKG += wkk/rubiks/client
PKG += wkk/rubiks/orm-example
PKG += wkk/rubiks/rubiks-cli
PKG += wkk/rubiks/rubiks-perf
PKG += wkk/rubiks/rubiks-orm

all:
	@go version
	@go install -v $(PKG)

clean:
	@rm -rfv ./bin ./pkg

test:
	@go test -v $(PKG)


fmt:
	@go fmt wkk/...
