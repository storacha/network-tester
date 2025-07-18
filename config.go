package main

import (
	"net/url"
	"os"

	"github.com/storacha/go-ucanto/did"
)

var IndexingServicePrincipal = Must(did.Parse("did:web:indexer.storacha.network"))
var IndexingServiceURL = Deref(Must(url.Parse("https://indexer.storacha.network")))

func init() {
	if os.Getenv("NETWORK") == "staging-warm" {
		IndexingServicePrincipal = Must(did.Parse("did:web:indexer.warm.storacha.network"))
		IndexingServiceURL = Deref(Must(url.Parse("https://indexer.warm.storacha.network")))
	}
}

func Deref[T any](ref *T) T {
	return *ref
}

func Must[T any](ret T, err error) T {
	if err != nil {
		panic(err)
	}
	return ret
}
