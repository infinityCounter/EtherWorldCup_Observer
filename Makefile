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
	$(eval DIR :="$(PWD)")
	pkg src/observer.js --targets=linux-x64 --out-path=./bin/$(SHA)
	cp $(DIR)/node_modules/sha3/build/Release/sha3.node ./bin/$(SHA)/sha3.node
	cp $(DIR)/node_modules/scrypt/build/Release/scrypt.node ./bin/$(SHA)/scrypt.node
	cp $(DIR)/node_modules/websocket/build/Release/bufferutil.node ./bin/$(SHA)/bufferutil.node
	cp $(DIR)/node_modules/websocket/build/Release/validation.node ./bin/$(SHA)/validation.node