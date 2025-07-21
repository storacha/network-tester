package main

import (
	"net/url"
	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"
)

var log = logging.Logger("network-tester")

var IndexingServicePrincipal did.DID
var IndexingServiceURL url.URL

func init() {
	switch os.Getenv("NETWORK") {
	case "", "hot":
		IndexingServicePrincipal = Must(did.Parse("did:web:indexer.storacha.network"))
		IndexingServiceURL = Deref(Must(url.Parse("https://indexer.storacha.network")))
	case "staging":
		IndexingServicePrincipal = Must(did.Parse("did:web:staging.indexer.storacha.network"))
		IndexingServiceURL = Deref(Must(url.Parse("https://staging.indexer.storacha.network")))
	case "staging-warm":
		IndexingServicePrincipal = Must(did.Parse("did:web:staging.indexer.warm.storacha.network"))
		IndexingServiceURL = Deref(Must(url.Parse("https://staging.indexer.warm.storacha.network")))
	default:
		panic("unknown network: " + os.Getenv("NETWORK"))
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
