package config

import (
	"fmt"
	"net/url"
	"os"

	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/network-tester/pkg/util"
)

var IndexingServicePrincipal did.DID
var IndexingServiceURL *url.URL
var Region = os.Getenv("REGION")

var UploadServicePrincipal did.DID
var UploadServiceURL *url.URL
var UploadServiceConnection client.Connection

// Replicas is the total number of replicas to request in replication testing.
// The value is inclusive of the original upload.
var Replicas int

func init() {
	switch os.Getenv("NETWORK") {
	case "", "hot":
		IndexingServicePrincipal = Must(did.Parse("did:web:indexer.storacha.network"))
		IndexingServiceURL = Must(url.Parse("https://indexer.storacha.network"))
		UploadServicePrincipal = Must(did.Parse("did:web:up.storacha.network"))
		UploadServiceURL = Must(url.Parse("https://up.storacha.network"))
		Replicas = 1
	case "staging":
		IndexingServicePrincipal = Must(did.Parse("did:web:staging.indexer.storacha.network"))
		IndexingServiceURL = Must(url.Parse("https://staging.indexer.storacha.network"))
		UploadServicePrincipal = Must(did.Parse("did:web:staging.up.storacha.network"))
		UploadServiceURL = Must(url.Parse("https://staging.up.storacha.network"))
		Replicas = 1
	case "staging-warm":
		IndexingServicePrincipal = Must(did.Parse("did:web:staging.indexer.warm.storacha.network"))
		IndexingServiceURL = Must(url.Parse("https://staging.indexer.warm.storacha.network"))
		UploadServicePrincipal = Must(did.Parse("did:web:staging.up.warm.storacha.network"))
		UploadServiceURL = Must(url.Parse("https://staging.up.warm.storacha.network"))
		Replicas = 3
	default:
		panic("unknown network: " + os.Getenv("NETWORK"))
	}

	channel := http.NewHTTPChannel(UploadServiceURL)
	UploadServiceConnection = Must(client.NewConnection(UploadServicePrincipal, channel))

	if Region == "" {
		Region = "unknown"
	}
}

func ID() principal.Signer {
	return Must(ed25519.Parse(MustGetenv("PRIVATE_KEY")))
}

func Proof() delegation.Delegation {
	return Must(util.ParseProof(MustGetenv("PROOF")))
}

func Must[T any](ret T, err error) T {
	if err != nil {
		panic(err)
	}
	return ret
}

func MustGetenv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(fmt.Errorf("missing environment variable: %s", name))
	}
	return value
}
