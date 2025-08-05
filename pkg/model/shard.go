package model

import (
	"encoding/json"
	"fmt"

	"net/url"
	"time"

	"github.com/google/uuid"
)

type URL url.URL

func (u URL) MarshalJSON() ([]byte, error) {
	url := url.URL(u)
	return json.Marshal((&url).String())
}

func (u *URL) UnmarshalJSON(b []byte) error {
	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return fmt.Errorf("parsing string: %w", err)
	}
	if str == "" {
		return nil
	}
	parsed, err := url.Parse(str)
	if err != nil {
		return fmt.Errorf("parsing URL: %w", err)
	}
	*u = URL(*parsed)
	return nil
}

func (u URL) String() string {
	url := url.URL(u)
	return (&url).String()
}

type Shard struct {
	ID                 Link      `json:"id"`
	Source             uuid.UUID `json:"source"`
	Upload             uuid.UUID `json:"upload"`
	LocationCommitment Link      `json:"locationCommitment"`
	Node               DID       `json:"node"`
	Size               int       `json:"size"`
	URL                URL       `json:"url"`
	Started            time.Time `json:"started"`
	Ended              time.Time `json:"ended"`
	Error              Error     `json:"error"`
}
