ifneq (,$(wildcard ./.env))
	include .env
	export
else
	$(error You haven't setup your .env file. Please copy .env.tpl to .env and fill in the values.)
endif

.PHONY: test-retrieval
test-retrieval:
	go test -v ./... -run ^TestRetrieval$
