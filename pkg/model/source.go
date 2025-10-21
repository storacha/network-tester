package model

import (
	"time"

	"github.com/google/uuid"
)

type Source struct {
	ID      uuid.UUID `json:"id"`
	Region  string    `json:"region"`
	Type    string    `json:"type"`
	Count   int       `json:"count"`
	Size    int       `json:"size"`
	Created time.Time `json:"created"`
}
