default: build

build: export GO111MODULE=on
build:
ifeq ($(TAGS),)
	$(CGO_FLAGS) go build -o bin/tcli ./*.go
else
	$(CGO_FLAGS) go build -tags "$(TAGS)" -o bin/tcli ./*.go
endif

check:
	golint -set_exit_status .

