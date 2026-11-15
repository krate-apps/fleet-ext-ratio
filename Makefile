.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/ratio-worker ./cmd/ratio-worker

pack:
	bash scripts/pack.sh dist
