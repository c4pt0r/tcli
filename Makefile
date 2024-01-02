default: build

build: export GO111MODULE=on
build:
ifeq ($(TAGS),)
	$(CGO_FLAGS) go build -o bin/tcli ./cli/*.go
else
	$(CGO_FLAGS) go build -tags "$(TAGS)" -o bin/tcli ./cli/*.go
endif

check:
	golint -set_exit_status .

prepare-llama-cpp:
	cd thirdparty/go-llama.cpp; git submodule update --init --recursive
	cd thirdparty/go-llama.cpp; make libbinding.a

build-with-llama: prepare-llama-cpp
	LIBRARY_PATH=./thirdparty/go-llama.cpp C_INCLUDE_PATH=./thirdparty/go-llama.cpp $(CGO_FLAGS) go build -tags "llama,$(TAGS)" -o bin/tcli ./cli/*.go
