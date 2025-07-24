package config

import (
	"net/url"
	"os"

	"github.com/storacha/go-ucanto/did"
)

var IndexingServicePrincipal did.DID
var IndexingServiceURL *url.URL
var Region = os.Getenv("REGION")

func init() {
	switch os.Getenv("NETWORK") {
	case "", "hot":
		IndexingServicePrincipal = Must(did.Parse("did:web:indexer.storacha.network"))
		IndexingServiceURL = Must(url.Parse("https://indexer.storacha.network"))
	case "staging":
		IndexingServicePrincipal = Must(did.Parse("did:web:staging.indexer.storacha.network"))
		IndexingServiceURL = Must(url.Parse("https://staging.indexer.storacha.network"))
	case "staging-warm":
		IndexingServicePrincipal = Must(did.Parse("did:web:staging.indexer.warm.storacha.network"))
		IndexingServiceURL = Must(url.Parse("https://staging.indexer.warm.storacha.network"))
	default:
		panic("unknown network: " + os.Getenv("NETWORK"))
	}

	if Region == "" {
		Region = "unknown"
	}
}

func Must[T any](ret T, err error) T {
	if err != nil {
		panic(err)
	}
	return ret
}
