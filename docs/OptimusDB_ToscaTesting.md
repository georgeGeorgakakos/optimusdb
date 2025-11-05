# Swarmchestrate TOSCA OptimusDB Testing

## 📋 Overview

This suite provides comprehensive tools for managing TOSCA files in the Swarmchestrate decentralized Knowledge Base (OptimusDB). It includes sample TOSCA files representing all five datastore types and PowerShell scripts for upload, query, and retrieval operations.

---

## 📦 Package Contents

### TOSCA Sample Files (5 Files)

| File | Type | Datastore | Description |
|------|------|-----------|-------------|
| **sample_1_application_description.yaml** | Application Description | ADT Datastore | E-commerce web application with frontend, backend API, database, and cache components |
| **sample_2_capacity_description.yaml** | Capacity Description | Capacity Descriptions | Edge cluster with 32 CPU cores, 128GB RAM, NVMe storage, GPU, and Kubernetes runtime |
| **sample_3_opentofu_tosca_template.yaml** | OpenTofu/TOSCA Template | OpenTofu/TOSCA Templates | Hybrid infrastructure with Kubernetes, Istio, Prometheus, and swarm status |
| **sample_4_deployment_release_plan.yaml** | Deployment/Release Plan | Deployment/Release Plans | Executable deployment with capacity matching, workflows, and rollback procedures |
| **sample_5_application_requirements.yaml** | Application Requirements | N/A (Submitted) | ML training workload requiring GPUs, high-performance storage, and RDMA networking |

### PowerShell Scripts (3 Scripts)

| Script | Purpose | Key Features |
|--------|---------|--------------|
| **TOSCA-Upload-Query.ps1** | Upload & query single files | 3 discovery modes, OrbitDB/SQLite queries, peer discovery, logging |
| **Batch-Upload-TOSCA-Files.ps1** | Batch upload all samples | Sequential upload, delay control, results tracking, JSON export |
| **TOSCA-Query.ps1** | Advanced query & retrieval | 8 query types, statistics, date ranges, export results |

### Documentation (2 Documents)

| Document | Content |
|----------|---------|
| **TOSCA_Files_in_Swarmchestrate.md** | Architecture overview, datastore types, technical details |
| **TOSCA-PowerShell-Usage-Guide.md** | Complete usage guide, examples, troubleshooting, best practices |

---

## 🚀 Quick Start

### Prerequisites

1. **PowerShell 5.1+** or **PowerShell Core 7+**
2. **kubectl** - Kubernetes CLI tool
3. **Access to Kubernetes cluster** with OptimusDB deployed

### Installation

```powershell
# 1. Clone or download all files to a directory
# 2. Navigate to the directory
cd C:\swarmchestrate-tosca

# 3. Verify kubectl access
kubectl get pods -n default | Select-String "optimusdb"

# 4. Check PowerShell execution policy
Get-ExecutionPolicy

# If needed, temporarily allow script execution
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process
```

### Basic Usage

#### Upload a Single TOSCA File
```powershell
.\TOSCA-Upload-Query.ps1 -ToscaFile "sample_1_application_description.yaml" -ToscaType ApplicationDescription
```

#### Upload All Sample Files
```powershell
.\Batch-Upload-TOSCA-Files.ps1
```

#### Query TOSCA Files
```powershell
# Get recent uploads
.\TOSCA-Query.ps1 -QueryType Recent -Limit 10

# Search by filename
.\TOSCA-Query.ps1 -QueryType ByFilename -SearchValue "sample_1_application_description.yaml"

# Get statistics
.\TOSCA-Query.ps1 -QueryType Statistics
```

---

## 📖 Detailed Documentation

### TOSCA File Types

The Swarmchestrate Knowledge Base uses **5 distinct TOSCA file types** across different datastores:

#### 1️⃣ Application Description (ADT Datastore)
**Purpose:** Complete application topology definition

**Use Case:** Define all components, relationships, and requirements for deployment

**Sample File:** `sample_1_application_description.yaml`

**Key Sections:**
- Node templates (frontend, backend, database, cache)
- Container runtime specifications
- Resource requirements (CPU, memory, storage)
- Scaling and monitoring policies
- Outputs and endpoints

#### 2️⃣ Capacity Description (Capacity Descriptions Datastore)
**Purpose:** Available infrastructure resource descriptions

**Use Case:** Describe physical and virtual resources in the swarm

**Sample File:** `sample_2_capacity_description.yaml`

**Key Sections:**
- Compute node specifications
- Storage resources (NVMe, SSD)
- Network interfaces and proximity
- GPU/accelerator information
- Container runtime capabilities
- Cost policies

#### 3️⃣ OpenTofu/TOSCA Template (OpenTofu/TOSCA Templates Datastore)
**Purpose:** Hybrid orchestrator templates with infrastructure provisioning

**Use Case:** Combine TOSCA service descriptions with OpenTofu/Terraform for infrastructure

**Sample File:** `sample_3_opentofu_tosca_template.yaml`

**Key Sections:**
- OpenTofu provider configuration
- Kubernetes resources (namespaces, ConfigMaps, secrets)
- Helm chart deployments (Ingress, Istio, Prometheus)
- Swarm status information
- Resource quotas and policies

#### 4️⃣ Deployment/Release Plan (Deployment/Release Plans Datastore)
**Purpose:** Executable deployment plans with capacity matching

**Use Case:** Detailed execution instructions with resource allocations

**Sample File:** `sample_4_deployment_release_plan.yaml`

**Key Sections:**
- Capacity matching results
- Resource allocation details
- Deployment instructions per component
- Health checks and monitoring
- Deployment workflows
- Rollback procedures

#### 5️⃣ Application Requirements (Submitted by Application Owner)
**Purpose:** Workload specifications and resource requirements

**Use Case:** Define what an application needs to run

**Sample File:** `sample_5_application_requirements.yaml`

**Key Sections:**
- Compute requirements (CPU, memory, GPU)
- Storage requirements (types, IOPS, throughput)
- Network requirements (bandwidth, latency, RDMA)
- Placement constraints
- Performance requirements
- Security and compliance requirements

---

## 🔧 PowerShell Scripts Guide

### Script 1: TOSCA-Upload-Query.ps1

**Purpose:** Upload and query individual TOSCA files

**Key Features:**
- 3 discovery modes (LoadBalancer, Pod IP, Headless DNS)
- Base64 encoding for file upload
- OrbitDB decentralized queries
- SQLite structured queries
- Peer discovery and topology
- Agent log retrieval
- Advanced analytics

**Discovery Modes:**

| Mode | Use Case | Target Format |
|------|----------|---------------|
| **lb** | Production, external access | `http://node-ip:service-port` |
        | **pod** | Testing, direct access | `http://pod-ip:8089` |
            | **headless** | Internal cluster | `http://pod-name.optimusdb-headless.ns.svc.cluster.local:8089` |

                    **Usage Examples:**
                    ```powershell
                    # LoadBalancer mode (recommended)
                    .\TOSCA-Upload-Query.ps1 `
                    -ToscaFile "sample_1_application_description.yaml" `
                    -Mode lb `
                    -ToscaType ApplicationDescription

                    # Pod IP mode (direct access)
                    .\TOSCA-Upload-Query.ps1 `
                    -ToscaFile "sample_2_capacity_description.yaml" `
                    -Mode pod `
                    -ContainerPort 8089 `
                    -ToscaType CapacityDescription

                    # Headless DNS mode (cluster-internal)
                    .\TOSCA-Upload-Query.ps1 `
                    -ToscaFile "sample_4_deployment_release_plan.yaml" `
                    -Mode headless `
                    -ToscaType DeploymentPlan

                    # Custom namespace
                    .\TOSCA-Upload-Query.ps1 `
                    -ToscaFile "mytosca.yaml" `
                    -Namespace swarmkb-production `
                    -ToscaType ApplicationDescription
                    ```

                    **What It Does:**
                    1. ✓ Discovers KB agent endpoints
                    2. ✓ Tests connectivity to each agent
                    3. ✓ Uploads TOSCA file (Base64 encoded)
                    4. ✓ Queries OrbitDB by template_id
                    5. ✓ Queries SQLite by filename
                    6. ✓ Queries SQLite by template_id
                    7. ✓ Lists connected peers
                    8. ✓ Fetches agent logs
                    9. ✓ Runs advanced analytics

                    ### Script 2: Batch-Upload-TOSCA-Files.ps1

                    **Purpose:** Upload all sample TOSCA files in one operation

                    **Key Features:**
                    - Sequential upload with configurable delays
                    - Progress tracking and status reporting
                    - Success/failure tracking
                    - JSON results export
                    - Verification query generation

                    **Usage Examples:**
                    ```powershell
                    # Basic batch upload
                    .\Batch-Upload-TOSCA-Files.ps1

                    # Custom namespace and delay
                    .\Batch-Upload-TOSCA-Files.ps1 `
                    -Namespace swarmkb-production `
                    -DelayBetweenUploads 5

                    # Pod mode with custom directory
                    .\Batch-Upload-TOSCA-Files.ps1 `
                    -Mode pod `
                    -ToscaDirectory "C:\my-tosca-files" `
                    -DelayBetweenUploads 2
                    ```

                    **Output:**
                    - Real-time upload status
                    - Detailed summary (success/fail counts)
                    - JSON results file: `batch-upload-results-YYYYMMDD-HHmmss.json`
                    - SQL verification queries

                    ### Script 3: TOSCA-Query.ps1

                    **Purpose:** Advanced query and retrieval without uploading

                    **Query Types:**

                    | Query Type | Description | Required Parameters |
                    |------------|-------------|---------------------|
                    | **ByFilename** | Search by exact filename | SearchValue |
                    | **ByTemplateId** | Search by template ID | SearchValue |
                    | **ByType** | Search by TOSCA type | ToscaType |
                    | **ByUploader** | Search by uploader username | SearchValue |
                    | **ByDateRange** | Search within date range | StartDate, EndDate |
                    | **Recent** | Get most recent uploads | Limit (optional) |
                    | **Statistics** | Get storage statistics | None |
                    | **All** | Get all files | Limit (optional) |

                    **Usage Examples:**
                    ```powershell
                    # Query by filename
                    .\TOSCA-Query.ps1 `
                    -QueryType ByFilename `
                    -SearchValue "sample_1_application_description.yaml"

                    # Query by template ID
                    .\TOSCA-Query.ps1 `
                    -QueryType ByTemplateId `
                    -SearchValue "template-abc123"

                    # Get recent uploads
                    .\TOSCA-Query.ps1 -QueryType Recent -Limit 20

                    # Get statistics
                    .\TOSCA-Query.ps1 -QueryType Statistics

                    # Query by TOSCA type
                    .\TOSCA-Query.ps1 `
                    -QueryType ByType `
                    -ToscaType ApplicationDescription

                    # Query by date range
                    .\TOSCA-Query.ps1 `
                    -QueryType ByDateRange `
                    -StartDate "2025-11-01" `
                    -EndDate "2025-11-05"

                    # Query by uploader
                    .\TOSCA-Query.ps1 `
                    -QueryType ByUploader `
                    -SearchValue "admin"
                    ```

                    ---

                    ## 📊 Complete Workflow Example

                    ### Scenario: Deploy ML Training Application

                    ```powershell
                    # Step 1: Upload application requirements
                    .\TOSCA-Upload-Query.ps1 `
                    -ToscaFile "sample_5_application_requirements.yaml" `
                    -ToscaType ApplicationRequirements

                    # Step 2: Upload available capacity
                    .\TOSCA-Upload-Query.ps1 `
                    -ToscaFile "sample_2_capacity_description.yaml" `
                    -ToscaType CapacityDescription

                    # Step 3: Upload application description
                    .\TOSCA-Upload-Query.ps1 `
                    -ToscaFile "sample_1_application_description.yaml" `
                    -ToscaType ApplicationDescription

                    # Step 4: Upload infrastructure template
                    .\TOSCA-Upload-Query.ps1 `
                    -ToscaFile "sample_3_opentofu_tosca_template.yaml" `
                    -ToscaType OpenTofuTemplate

                    # Step 5: Upload deployment plan
                    .\TOSCA-Upload-Query.ps1 `
                    -ToscaFile "sample_4_deployment_release_plan.yaml" `
                    -ToscaType DeploymentPlan

                    # Step 6: Verify all uploads
                    .\TOSCA-Query.ps1 -QueryType Recent -Limit 10

                    # Step 7: Get statistics
                    .\TOSCA-Query.ps1 -QueryType Statistics
                    ```

                    ---

                    ## 🔍 Architecture Overview

                    ### Knowledge Base Components

                    ```
                    ┌─────────────────────────────────────────────────────────┐
                    │                  Application Layer                      │
                    │         (PowerShell Scripts, kubectl, Users)            │
                    └────────────────────┬────────────────────────────────────┘
                    │
                    ┌────────────────────┴────────────────────────────────────┐
                    │              KB Agent API Layer                         │
                    │  (REST API: /upload, /command, /peers, /log)            │
                    └────────────────────┬────────────────────────────────────┘
                    │
                    ┌────────────┴────────────┐
                    │                         │
                    ┌───────▼────────┐      ┌────────▼────────┐
                    │  Coordinator   │      │   Follower      │
                    │  Agent (KBc)   │◄────►│  Agents (KBf)   │
                    └───────┬────────┘      └────────┬────────┘
                    │                        │
                    ┌───────┴────────────────────────┴──────────┐
                    │          Data Store Layer                 │
                    │  ┌──────────┐  ┌──────────┐  ┌─────────┐  │
                    │  │ OrbitDB  │  │ SQLite   │  │  IPFS   │  │
                    │  │(Document)│  │(Metadata)│  │(Storage)│  │
                    │  └──────────┘  └──────────┘  └─────────┘  │
                    └───────────────────────────────────────────┘
                    │
                    ┌───────┴─────────────────────────────────┐
                    │        Network Layer (libp2p)           │
                    │  (Pub/Sub, P2P, DHT, Peer Discovery)    │
                    └─────────────────────────────────────────┘
                    ```

                    ### Data Flow

                    1. **Upload:** User → Script → KB Agent → OrbitDB + SQLite + IPFS
                    2. **Query:** User → Script → KB Agent → SQLite/OrbitDB → Response
                    3. **Replication:** Coordinator ↔ Followers (IPFS + libp2p)

                    ---

                    ## 🛠️ Troubleshooting

                    ### Common Issues

                    #### Issue: "kubectl not found"
                    ```powershell
                    # Install kubectl
                    # Windows: choco install kubernetes-cli
                    # Or download from: https://kubernetes.io/docs/tasks/tools/

                    # Verify installation
                    kubectl version --client
                    ```

                    #### Issue: "No OptimusDB services found"
                    ```powershell
                    # Check all namespaces
                    kubectl get svc --all-namespaces | Select-String "optimusdb"

                    # Use correct namespace (replace with your actual namespace)
                    .\TOSCA-Upload-Query.ps1 -Namespace your-namespace-here
                    ```

                    #### Issue: "Connection timeout"
                    ```powershell
                    # Check pod status
                    kubectl get pods -n default

                    # Check pod logs
                    kubectl logs optimusdb-0 -n default

                    # Try different mode
                    .\TOSCA-Upload-Query.ps1 -Mode pod
                    ```

                    #### Issue: "Execution Policy Error"
                    ```powershell
                    # Temporarily bypass
                    Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process

                    # Or run with bypass
                    powershell -ExecutionPolicy Bypass -File .\TOSCA-Upload-Query.ps1
                    ```

                    ---

                    ## 📈 Best Practices

                    ### 1. File Management
                    - ✓ Use descriptive filenames with versions
                    - ✓ Include TOSCA type in filename
                    - ✓ Keep original files as backup

                    ### 2. Upload Strategy
                    - ✓ Use batch script for initial setup
                    - ✓ Use single upload for updates
                    - ✓ Set appropriate delays for batch uploads

                    ### 3. Query Optimization
                    - ✓ Use specific queries (by ID) when possible
                    - ✓ Limit result sets appropriately
                    - ✓ Export results for analysis

                    ### 4. Monitoring
                    - ✓ Check peer lists regularly
                    - ✓ Review agent logs for errors
                    - ✓ Monitor storage statistics

                    ### 5. Security
                    - ✓ Use kubectl authentication
                    - ✓ Encrypt sensitive data in TOSCA files
                    - ✓ Use LoadBalancer mode in production

                    ---

                    ## 📚 Additional Resources

                    ### Documentation Files
                    - **TOSCA_Files_in_Swarmchestrate.md** - Architecture and datastore types
                    - **TOSCA-PowerShell-Usage-Guide.md** - Complete usage guide

                    ### Sample Files
                    - All 5 TOSCA sample files with detailed comments
                    - Realistic configurations for production use

                    ### Support
                    - Review D3.1 deliverable document for architecture details
                    - Check OptimusDB documentation for API reference
                    - TOSCA specification: https://docs.oasis-open.org/tosca/

                    ---

                    ## 📝 Summary

                    This suite provides everything needed to manage TOSCA files in the Swarmchestrate Knowledge Base:

                    ✅ **5 Sample TOSCA files** covering all datastore types
                    ✅ **3 PowerShell scripts** for upload, batch, and query operations
                    ✅ **2 Documentation files** with detailed guides and examples
                    ✅ **Multiple discovery modes** for different deployment scenarios
                    ✅ **Advanced queries** for search, filter, and analysis
                    ✅ **Complete workflows** for real-world scenarios

                    **Get Started:**
                    ```powershell
                    # Upload all samples
                    .\Batch-Upload-TOSCA-Files.ps1

                    # Query recent uploads
                    .\TOSCA-Query.ps1 -QueryType Recent -Limit 10

                    # Get statistics
                    .\TOSCA-Query.ps1 -QueryType Statistics
                    ```

                    ---

                    **Version:** 2.0
                    **Date:** November 2025
                    **Platform:** PowerShell 5.1+ / PowerShell Core 7+
                    **License:** As per Swarmchestrate project