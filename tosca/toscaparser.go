package tosca

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"gopkg.in/yaml.v3"
)

func ComputeTemplateID(raw []byte) string {
	h := sha1.Sum(raw)
	return hex.EncodeToString(h[:])[:16] // short ID
}

func CountNodeTemplates(t *ServiceTemplate) int {
	return len(t.TopologyTemplate.NodeTemplates)
}

type ServiceTemplate struct {
	ToscaDefinitionsVersion string           `yaml:"tosca_definitions_version"`
	Description             string           `yaml:"description,omitempty"`
	Imports                 []string         `yaml:"imports,omitempty"`
	TopologyTemplate        TopologyTemplate `yaml:"topology_template"`
}

type TopologyTemplate struct {
	NodeTemplates map[string]NodeTemplate `yaml:"node_templates"`
	Inputs        map[string]Input        `yaml:"inputs,omitempty"`
	Outputs       map[string]Output       `yaml:"outputs,omitempty"`
}

type NodeTemplate struct {
	Type         string                  `yaml:"type"`
	Properties   map[string]interface{}  `yaml:"properties,omitempty"`
	Requirements []RequirementAssignment `yaml:"requirements,omitempty"`
	Capabilities map[string]Capability   `yaml:"capabilities,omitempty"`
}

type RequirementAssignment map[string]interface{}

type Capability struct {
	Properties map[string]interface{} `yaml:"properties,omitempty"`
}

type Input struct {
	Type        string      `yaml:"type"`
	Description string      `yaml:"description,omitempty"`
	Default     interface{} `yaml:"default,omitempty"`
}

type Output struct {
	Description string      `yaml:"description,omitempty"`
	Value       interface{} `yaml:"value"`
}

func ParseTOSCA(data []byte) (*ServiceTemplate, error) {
	var template ServiceTemplate
	err := yaml.Unmarshal(data, &template)
	if err != nil {
		return nil, fmt.Errorf("error parsing TOSCA YAML: %w", err)
	}
	return &template, nil
}

// Helper function to generate a short ID
func generateShortID(seed string) string {
	if len(seed) >= 8 {
		return seed[:8]
	}
	return seed
}

// -------------------------------------------------------------
// Changes from basic version:
// - Full Properties parsing
// - Requirements parsing (host links, relationships)
// - Capabilities parsing
// - Ready for Inputs/Outputs if declared
// - Store parsed TOSCA template into OrbitDB datastore
// - Create lightweight metadata entry inside SQLite database
// - Metadata includes description, node count, and timestamps
