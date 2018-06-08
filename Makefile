SHELL := /bin/bash
$(eval SHA := $(shell git rev-parse HEAD))

.PHONY: all

all: clean vendor observer

vendor:
	npm install

clean:
	rm -rf node_modules/
	rm -rf bin/

observer: 
	pkg src/observer.js --targets=linux-x64 --out-path=./bin/$(SHA)