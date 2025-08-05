package model

import (
	"net/url"
	"time"

	"github.com/google/uuid"
)

type Shard struct {
	ID                 Link      `json:"id"`
	Source             uuid.UUID `json:"source"`
	Upload             uuid.UUID `json:"upload"`
	LocationCommitment Link      `json:"locationCommitment"`
	Node               DID       `json:"node"`
	Size               int       `json:"size"`
	URL                url.URL   `json:"url"`
	Started            time.Time `json:"started"`
	Ended              time.Time `json:"ended"`
	Error              Error     `json:"error"`
}
