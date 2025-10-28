package usage

import (
	"time"
)

// Resource represents the resources table
type Resource struct {
	ID                  string `gorm:"primaryKey"`
	ResourceProvider    string `gorm:"not null"`
	CapacityProvider    string
	ResourceName        string `gorm:"not null"`
	ResourceType        string
	ResourceDef         string
	ResourceNature      string
	Status              string
	ResourceGrpName     string
	Pricing             []string `gorm:"type:text[]"`
	ResourceTags        []string `gorm:"type:text[]"`
	CPUCapacity         int
	MemoryCapacity      int
	Storage             int
	Region              string
	Country             string
	OperatingSystem     string
	City                string
	EnergyType          string
	EnergyConsumptionDM string
	AccessSensors       string
	Device              string
	Mobility            string
	PoweredType         string
	OrchestrationID     string
	NetworkType         string
	Bandwidth           string
	CIDRBlock           string
	StorageType         string
	StorageCapacity     int
	UsedStorage         int
	LastHeartbeat       time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
	AllocatedAt         time.Time
	DeallocatedAt       *time.Time
}

// Metadata represents the metadata table
/*
type DataCatalog struct {
	ID               string `gorm:"primaryKey"`
	Author           string
	MetadataType     string
	Component        string
	Behaviour        string
	Relationships    string
	AssociatedID     string
	Name             string
	Description      string
	Tags             []string `gorm:"type:text[]"`
	Status           string
	CreatedBy        string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	RelatedIDs       []string `gorm:"type:text[]"`
	Priority         string
	SchedulingInfo   string `gorm:"type:json"`
	SLAConstraints   string `gorm:"type:json"`
	OwnershipDetails string `gorm:"type:json"`
	AuditTrail       string `gorm:"type:json"`
}

*/
// Metadata represents the metadata table
type DataCatalog struct {
	ID               string `gorm:"primaryKey"`
	Author           string
	MetadataType     string
	Component        string
	Behaviour        string
	Relationships    string
	AssociatedID     string
	Name             string
	Description      string
	Tags             []string `gorm:"type:text[]"`
	Status           string
	CreatedBy        string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	RelatedIDs       []string `gorm:"type:text[]"`
	Priority         string
	SchedulingInfo   string `gorm:"type:json"`
	SLAConstraints   string `gorm:"type:json"`
	OwnershipDetails string `gorm:"type:json"`
	AuditTrail       string `gorm:"type:json"`

	// ADD CONTEXTUAL METADATA FIELDS
	ContextualPurpose     string   `gorm:"type:text"`
	ContextualVariables   []string `gorm:"type:text[]"`
	ContextualKeywords    []string `gorm:"type:text[]"`
	ContextualDataType    string
	ContextualDescription string   `gorm:"type:text"`
	ContextualUnits       []string `gorm:"type:text[]"`
	ContextualGeneratedAt time.Time
}
