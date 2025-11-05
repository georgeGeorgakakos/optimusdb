# TOSCA Files in Swarmchestrate Knowledge Base (OptimusDB)

## Overview

Based on the Swarmchestrate architecture document, the decentralized Knowledge Base (KB) system uses TOSCA (Topology and Orchestration Specification for Cloud Applications) as the primary format for describing various aspects of the orchestration ecosystem. This document identifies the different types of TOSCA files that exist in the system and provides sample templates.

---

## Types of TOSCA Files in Swarmchestrate

According to the document analysis, there are **FIVE (5) distinct types of TOSCA files** used across different datastores in the Knowledge Base:

### 1. **Application Description TOSCA Files (ADT Datastore)**
**Purpose:** Store full application descriptions based on TOSCA YAML

**Context:** Orchestration System (Ingress & Egress)

**Description:** These files contain the complete application topology, including all components, their relationships, requirements, and capabilities. They define what needs to be deployed.

---

### 2. **Capacity Description TOSCA Files (Capacity Descriptions Datastore)**
**Purpose:** Describe available resources and infrastructure capacity

**Context:**
- Capacity Provider (Ingress)
- Orchestration System (Egress)

**Description:** These TOSCA templates describe the physical and virtual infrastructure resources available in the swarm, including CPU, memory, storage, network proximity, and resource status. They represent the "supply" side of the orchestration equation.

---

### 3. **OpenTofu/TOSCA Templates (OpenTofu/TOSCA Templates Datastore)**
**Purpose:** Store raw or partially processed orchestrator templates and swarm status information

**Context:**
- Orchestration System (Ingress)
- Orchestration System (Egress)

**Description:** These are hybrid templates that may include both TOSCA and OpenTofu (Terraform) elements, used for infrastructure provisioning and configuration management. They contain swarm status information and deployment configurations.

---

### 4. **Deployment/Release Plans TOSCA Files (Deployment/Release Plans Datastore)**
**Purpose:** Store prepared deployment plans parsed from TOSCA with capacity status information

**Context:**
- Orchestration System (Ingress & Egress)

**Description:** These files represent the "execution plan" - they are processed TOSCA files that have been matched against available capacity and contain specific deployment instructions, including resource allocations and release schedules.

---

### 5. **Application Requirement TOSCA Files**
**Purpose:** Workload specifications submitted by application owners

**Context:** Application Owner submits to Orchestration System

**Description:** These are TOSCA-based descriptions of resource requirements and workload specifications that define what an application needs to run (requirements side).

---

## Summary Table

| # | TOSCA File Type | Datastore | Direction | Primary Role |
|---|----------------|-----------|-----------|--------------|
| 1 | Application Description | ADT Datastore | Ingress/Egress | Orchestration System |
| 2 | Capacity Description | Capacity Descriptions Datastore | Ingress/Egress | Capacity Provider / Orchestrator |
| 3 | OpenTofu/TOSCA Templates | OpenTofu/TOSCA Templates Datastore | Ingress/Egress | Orchestration System |
| 4 | Deployment/Release Plans | Deployment/Release Plans Datastore | Ingress/Egress | Orchestration System |
| 5 | Application Requirements | N/A (Submitted) | Ingress | Application Owner |

---

## Key Characteristics

### Data Flow
- **Ingress:** TOSCA files are pushed into the KB by various components
- **Egress:** TOSCA files are retrieved from the KB for decision-making and execution

### Format Considerations
- Files are **base64-encoded** and embedded in JSON for transfers
- All use **YAML** format as the base TOSCA representation
- Integrated with **RESTful payloads** for middleware communication

### Integration Points
The KB interfaces with:
- **Orchestration System** (producer/consumer)
- **Capacity Provider** (producer)
- **Event Monitoring System (EMS)** (producer/consumer)
- **Identity & Role Management (IDM)** system

---

## Technical Architecture Notes

The decentralized KB uses:
- **IPFS** for content-addressed storage
- **libp2p** for P2P communication
- **CRDT-based replication** for consistency
- **Coordinator/Follower agent architecture** for decentralized operations
- **SQL and document store** backends for different query patterns