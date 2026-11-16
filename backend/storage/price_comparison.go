package storage

import "time"

// PriceComparisonConfig is an admin-only, singleton model. Secrets never leave
// the server and use the same APP_SECRET encryption as upstream credentials.
type PriceComparisonConfig struct {
	ID           uint      `gorm:"primaryKey" json:"-"`
	Endpoint     string    `gorm:"size:1024" json:"endpoint"`
	Model        string    `gorm:"size:256" json:"model"`
	APIKeyCipher string    `gorm:"type:text" json:"-"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// A draft and its saved strategy share an immutable server-generated result.
// Saving starts a 30-minute application window; repeated saves cannot extend it.
type PriceComparisonStrategy struct {
	ID            string     `gorm:"primaryKey;size:64" json:"id"`
	Search        string     `gorm:"size:256" json:"search"`
	Specification string     `gorm:"size:1024" json:"specification"`
	Currency      string     `gorm:"size:8" json:"currency"`
	Unit          string     `gorm:"size:64" json:"unit"`
	Requirements  string     `gorm:"type:text" json:"requirements"`
	Analysis      string     `gorm:"type:text" json:"analysis"`
	ItemsJSON     string     `gorm:"type:text" json:"-"`
	SavedAt       *time.Time `json:"saved_at"`
	ExpiresAt     time.Time  `gorm:"index" json:"expires_at"`
	CreatedAt     time.Time  `json:"created_at"`
}
