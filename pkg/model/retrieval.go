package model

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/did"
)

type Multihash struct {
	multihash.Multihash
}

func (mh Multihash) MarshalJSON() ([]byte, error) {
	if len(mh.Multihash) == 0 {
		return json.Marshal("")
	}
	str, err := multibase.Encode(multibase.Base58BTC, mh.Multihash)
	if err != nil {
		return nil, fmt.Errorf("multibase encoding: %w", err)
	}
	return json.Marshal(str)
}

func (mh *Multihash) UnmarshalJSON(b []byte) error {
	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return fmt.Errorf("parsing string: %w", err)
	}
	if str == "" {
		return nil
	}
	_, bytes, err := multibase.Decode(str)
	if err != nil {
		return fmt.Errorf("multibase decoding: %w", err)
	}
	digest, err := multihash.Cast(bytes)
	if err != nil {
		return fmt.Errorf("decoding multihash: %w", err)
	}
	*mh = Multihash{digest}
	return nil
}

type DID struct {
	did.DID
}

func (id DID) MarshalJSON() ([]byte, error) {
	if id.DID == did.Undef {
		return json.Marshal("")
	}
	return json.Marshal(id.String())
}

func (id *DID) UnmarshalJSON(b []byte) error {
	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return fmt.Errorf("parsing string: %w", err)
	}
	if str == "" {
		return nil
	}
	d, err := did.Parse(str)
	if err != nil {
		return fmt.Errorf("parsing DID: %w", err)
	}
	*id = DID{d}
	return nil
}

type Retrieval struct {
	ID      uuid.UUID `json:"id"`
	Region  string    `json:"region"`
	Source  uuid.UUID `json:"source"`
	Upload  uuid.UUID `json:"upload"`
	Node    DID       `json:"node"`
	Shard   Multihash `json:"shard"`
	Slice   Multihash `json:"slice"`
	Size    int       `json:"size"`
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended"`
	Status  int       `json:"status"`
	Error   Error     `json:"error"`
}
