package util

import (
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/delegation"
	gdlg "github.com/storacha/guppy/pkg/delegation"
)

// ParseProof is a helper to parse a proof from a proof archive that has been
// embedded in an identity multihash and wrapped in a CID. It has fallbacks to
// parsing from legacy encodings.
//
// It first tries to parse the string as a CAR CID with an identity multihash.
// If parsing fails, it decodes the string as base64 and attempts to extract the
// proof from that.
//
// See [ExtractProof] for extraction fallback details.
func ParseProof(s string) (delegation.Delegation, error) {
	cid, err := cid.Parse(s)
	if err != nil {
		// At one point we recommended piping output directly to base64 encoder:
		// `w3 delegation create did:key... --can 'store/add' | base64`
		bytes, _ := base64.StdEncoding.DecodeString(s)
		return gdlg.ExtractProof(bytes)
	}

	if multicodec.Code(cid.Prefix().Codec) != multicodec.Car {
		return nil, errors.New("unexpected IPLD codec, must be CAR")
	}

	digestInfo, err := multihash.Decode(cid.Hash())
	if err != nil {
		return nil, fmt.Errorf("decoding multihash: %w", err)
	}

	if digestInfo.Code != multibase.Identity {
		return nil, errors.New("unexpected multihash code, must be identity. Fetching of remote proof CARs is not supported")
	}

	return gdlg.ExtractProof(digestInfo.Digest)
}
