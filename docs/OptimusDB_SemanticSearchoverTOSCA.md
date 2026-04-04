# OptimusDB — Semantic Search over TOSCA 2.0 Artifacts
## Swarmchestrate Academic Demonstration Scenario

> **Context:** This scenario demonstrates OptimusDB as a semantic knowledge base for
> Swarmchestrate TOSCA 2.0 artifacts. The OptimusDB cluster is already deployed and
> running. TOSCA 2.0 files — topology templates, node type definitions, application
> descriptors, capacity profiles, deployment plans, policies — are inserted as documents.
> Afterwards, a user can search through them using plain English sentences, without
> knowing the TOSCA structure, field names, or exact terminology used in the files.

---

## The idea in one sentence

Instead of grepping YAML files or reading through topology templates manually, you ask
OptimusDB: *"Which applications need a GPU?"* — and it finds the right TOSCA
descriptors across the distributed cluster.

---

## Setup

| Variable | Value |
|---|---|
| `base_url` | `http://193.225.250.240` |
| `context` | `swarmkb` |
| `dataStore` | `dsswres` |

Structured insert/query operations use `POST {{base_url}}/optimusdbN/{{context}}/command`.
Semantic index/search operations use `POST/GET {{base_url}}/optimusdbN/api/v1/semantic/`.

---

## Step 1 — Insert TOSCA 2.0 artifacts

Five TOSCA artifacts are inserted across the three nodes, each with a different
`document_type` reflecting their role in the Swarmchestrate orchestration lifecycle.

### Document 1 — WebApp Application Description (node 1)

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"argcnt": 10000, "cmd": "crudput"},
"args": ["tosca_webapp_microservicesapplication_v1_0_0", "application_description"],
"dstype": "dsswres",
"sqlselect": "",
"criteria": [{
"_id": "tosca_webapp_microservicesapplication_v1_0_0",
"document_type": "application_description",
"tosca_version": "tosca_simple_yaml_1_3",
"metadata": {
"template_name": "WebApp-MicroservicesApplication",
"template_author": "Swarmchestrate Orchestrator",
"template_version": "1.0.0",
"kb_datastore": "ADT",
"creation_timestamp": "2025-11-05T10:30:00Z"
},
"description": "E-commerce web application with frontend, backend API, and database components",
"node_types": [
"tosca.nodes.Container.Application.Docker",
"tosca.nodes.Database.PostgreSQL",
"tosca.nodes.Database.Redis",
"tosca.nodes.Container.Runtime.Docker"
],
"policy_types": [
"tosca.policies.Scaling",
"tosca.policies.Placement",
"tosca.policies.Monitoring"
],
"groups": ["frontend_tier", "backend_tier", "data_tier"],
"created_at": "2025-11-05T10:30:00Z",
"kbagent": "swarmchestrate_orchestrator"
}]
}'
```

### Document 2 — EdgeCluster Capacity Profile (node 2)

```bash
curl -s -X POST http://193.225.250.240/optimusdb2/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"argcnt": 10000, "cmd": "crudput"},
"args": ["tosca_edgecluster_capacityprofile_v1_0_0", "capacity_description"],
"dstype": "dsswres",
"sqlselect": "",
"criteria": [{
"_id": "tosca_edgecluster_capacityprofile_v1_0_0",
"document_type": "capacity_description",
"tosca_version": "tosca_simple_yaml_1_3",
"metadata": {
"template_name": "EdgeCluster-CapacityProfile",
"template_author": "Capacity Provider",
"template_version": "1.0.0",
"kb_datastore": "Capacity_Descriptions",
"region": "eu-central-1",
"status": "available"
},
"description": "Available capacity at edge location including compute, storage, and network resources",
"node_types": [
"tosca.nodes.Compute.Physical",
"tosca.nodes.Storage.BlockStorage",
"tosca.nodes.Compute.GPU",
"tosca.nodes.Container.Runtime.Kubernetes"
],
"topology": {
"edge_compute_node_01": {
"properties": {
"available_cpu_cores": 24,
"available_memory": "96 GB"
}
},
"gpu_accelerator_01": {
"properties": {
"gpu_model": "NVIDIA A100",
"gpu_memory": "40 GB",
"available": true
}
},
"local_storage_01": {
"properties": {
"available_size": "1.5 TB"
}
}
},
"created_at": "2025-11-05T10:30:00Z",
"kbagent": "capacity_provider"
}]
}'
```

### Document 3 — HybridInfrastructure OpenTofu/TOSCA Template (node 1)

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"argcnt": 10000, "cmd": "crudput"},
"args": ["tosca_hybridinfrastructure_swarmdeployment_v1_0_0", "opentofu_tosca_template"],
"dstype": "dsswres",
"sqlselect": "",
"criteria": [{
"_id": "tosca_hybridinfrastructure_swarmdeployment_v1_0_0",
"document_type": "opentofu_tosca_template",
"tosca_version": "tosca_simple_yaml_1_3",
"metadata": {
"template_name": "HybridInfrastructure-SwarmDeployment",
"template_author": "Swarmchestrate Orchestrator",
"template_version": "1.0.0",
"kb_datastore": "OpenTofu_TOSCA_Templates",
"creation_timestamp": "2025-11-05T10:30:00Z"
},
"description": "Hybrid infrastructure template combining TOSCA service descriptions with OpenTofu infrastructure provisioning",
"node_types": [
"tosca.nodes.Container.Runtime.Kubernetes.Namespace",
"tosca.nodes.LoadBalancer.Nginx",
"tosca.nodes.ServiceMesh.Istio",
"tosca.nodes.Monitoring.Prometheus"
],
"groups": ["infrastructure_layer", "observability_layer", "security_layer"],
"swarm_status": {
"total_nodes": 12,
"active_nodes": 11
},
"opentofu_config": {
"providers": {
"kubernetes": {"source": "hashicorp/kubernetes"}
}
},
"created_at": "2025-11-05T10:30:00Z",
"kbagent": "swarmchestrate_orchestrator"
}]
}'
```

### Document 4 — Deployment Release Plan (node 1)

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"argcnt": 10000, "cmd": "crudput"},
"args": ["tosca_deploymentplan_webapp_release_v1_0_0", "deployment_release_plan"],
"dstype": "dsswres",
"sqlselect": "",
"criteria": [{
"_id": "tosca_deploymentplan_webapp_release_v1_0_0",
"document_type": "deployment_release_plan",
"tosca_version": "tosca_simple_yaml_1_3",
"metadata": {
"template_name": "DeploymentPlan-WebApp-Release",
"template_author": "Swarmchestrate Orchestrator",
"template_version": "1.0.0",
"execution_status": "ready_for_deployment"
},
"description": "Executable deployment plan with specific resource assignments and orchestration instructions",
"capacity_matching": {
"status": "successful",
"match_score": 0.92
},
"workflows": ["deployment_workflow", "rollback_workflow"],
"created_at": "2025-11-05T10:30:45Z",
"kbagent": "swarmchestrate_orchestrator"
}]
}'
```

### Document 5 — ML Training Workload Requirements (node 1)

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"argcnt": 10000, "cmd": "crudput"},
"args": ["tosca_applicationrequirements_mltrainingworkload_v1_0_0", "application_requirements"],
"dstype": "dsswres",
"sqlselect": "",
"criteria": [{
"_id": "tosca_applicationrequirements_mltrainingworkload_v1_0_0",
"document_type": "application_requirements",
"tosca_version": "tosca_simple_yaml_1_3",
"metadata": {
"template_name": "ApplicationRequirements-MLTrainingWorkload",
"template_author": "Application Owner",
"template_version": "1.0.0",
"priority": "high",
"sla_tier": "gold"
},
"description": "Machine Learning training workload requirements for computer vision model",
"node_types": [
"tosca.nodes.Application.Requirements",
"tosca.nodes.Compute.GPU.Requirements",
"tosca.nodes.Storage.Requirements"
],
"policy_types": [
"tosca.policies.Placement.Requirements",
"tosca.policies.Performance.Requirements"
],
"topology": {
"gpu_requirements": {
"properties": {
"gpu_count_min": 2,
"gpu_memory_min": "32 GB"
}
},
"placement_constraints": {
"properties": {
"geographical_region": ["eu-central", "eu-west"]
}
}
},
"created_at": "2025-11-05T09:45:00Z",
"kbagent": "application_owner"
}]
}'
```

---

## Step 2 — Verify all documents were inserted

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": []
}' | python3 -m json.tool
```

Expected: 5 documents returned across the cluster.

---

## Step 3 — Structured queries (exact field matching)

These queries use the structured fields of the TOSCA documents directly.

### Find all application descriptions from the ADT datastore

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"argcnt": 10000, "cmd": "query"},
"args": ["*", "application_description"],
"dstype": "dsswres",
"sqlselect": "",
"criteria": [{
"document_type": "application_description",
"metadata.kb_datastore": "ADT"
}],
"options": {
"strategy": "LOCAL_THEN_REMOTE_MERGE",
"time_budget_ms": 1000,
"annotate_source": true
}
}' | python3 -m json.tool
```

Expected: `tosca_webapp_microservicesapplication_v1_0_0`

### Find available capacity in EU region

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 10000},
"dstype": "dsswres",
"criteria": [{
"metadata.region": "eu-central-1",
"metadata.status": "available"
}]
}' | python3 -m json.tool
```

Expected: `tosca_edgecluster_capacityprofile_v1_0_0`

### Find GPU capacity with at least 20 CPU cores

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 10000},
"dstype": "dsswres",
"criteria": [{
"document_type": "capacity_description",
"metadata.status": "available",
"node_types": {"$contains": "tosca.nodes.Compute.GPU"},
"topology.edge_compute_node_01.properties.available_cpu_cores": {"$gte": 20}
}]
}' | python3 -m json.tool
```

Expected: `tosca_edgecluster_capacityprofile_v1_0_0`

### Complex query — EU capacity with GPU and sufficient CPU

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 10000},
"dstype": "dsswres",
"criteria": [{
"$and": [
{"document_type": "capacity_description"},
{"metadata.status": "available"},
{"metadata.region": {"$regex": "eu-.*"}},
{"topology.gpu_accelerator_01.properties.available": true},
{"topology.edge_compute_node_01.properties.available_cpu_cores": {"$gte": 20}}
]
}]
}' | python3 -m json.tool
```

Expected: `tosca_edgecluster_capacityprofile_v1_0_0` — capacity matching use case.

### Find high priority application requirements

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 10000},
"dstype": "dsswres",
"criteria": [{
"document_type": "application_requirements",
"metadata.priority": "high"
}]
}' | python3 -m json.tool
```

Expected: `tosca_applicationrequirements_mltrainingworkload_v1_0_0`

### Find deployment plans ready to deploy with high match score

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 10000},
"dstype": "dsswres",
"criteria": [{
"document_type": "deployment_release_plan",
"metadata.execution_status": "ready_for_deployment",
"capacity_matching.status": "successful",
"capacity_matching.match_score": {"$gte": 0.9}
}]
}' | python3 -m json.tool
```

Expected: `tosca_deploymentplan_webapp_release_v1_0_0`

---

## Step 3.5 — Index all documents for Semantic Search

> **Important:** Before running semantic queries, each document must be indexed.
> This step sends the document content to TinyLlama-1.1B, generates a 2048-dimension
> embedding, and stores it in the local sqlite-vec table. The embedding is also pinned
> to IPFS for cross-node availability via GossipSub.

### Index Document 1 — WebApp Application Description

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{
"store": "dsswres",
"doc_id": "tosca_webapp_microservicesapplication_v1_0_0",
"fields": {
"name": "WebApp-MicroservicesApplication",
"description": "E-commerce web application with frontend, backend API, and database components",
"tags": "docker postgresql redis frontend backend database scaling placement monitoring",
"type": "application_description"
}
}'
```

Expected: `{"doc_id": "tosca_webapp_microservicesapplication_v1_0_0", "status": "indexed"}`

### Index Document 2 — EdgeCluster Capacity Profile

```bash
curl -s -X POST http://193.225.250.240/optimusdb2/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{
"store": "dsswres",
"doc_id": "tosca_edgecluster_capacityprofile_v1_0_0",
"fields": {
"name": "EdgeCluster-CapacityProfile",
"description": "Available capacity at edge location including compute, storage, and network resources",
"tags": "GPU NVIDIA A100 edge compute kubernetes storage eu-central available capacity 24 CPU 96GB",
"type": "capacity_description",
"region": "eu-central-1"
}
}'
```

Expected: `{"doc_id": "tosca_edgecluster_capacityprofile_v1_0_0", "status": "indexed"}`

### Index Document 3 — HybridInfrastructure Template

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{
"store": "dsswres",
"doc_id": "tosca_hybridinfrastructure_swarmdeployment_v1_0_0",
"fields": {
"name": "HybridInfrastructure-SwarmDeployment",
"description": "Hybrid infrastructure template combining TOSCA service descriptions with OpenTofu infrastructure provisioning",
"tags": "kubernetes nginx istio prometheus monitoring observability security opentofu hybrid infrastructure",
"type": "opentofu_tosca_template"
}
}'
```

Expected: `{"doc_id": "tosca_hybridinfrastructure_swarmdeployment_v1_0_0", "status": "indexed"}`

### Index Document 4 — Deployment Release Plan

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{
"store": "dsswres",
"doc_id": "tosca_deploymentplan_webapp_release_v1_0_0",
"fields": {
"name": "DeploymentPlan-WebApp-Release",
"description": "Executable deployment plan with specific resource assignments and orchestration instructions",
"tags": "deployment release ready orchestration rollback workflow capacity matching successful",
"type": "deployment_release_plan",
"status": "ready_for_deployment"
}
}'
```

Expected: `{"doc_id": "tosca_deploymentplan_webapp_release_v1_0_0", "status": "indexed"}`

### Index Document 5 — ML Training Workload Requirements

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{
"store": "dsswres",
"doc_id": "tosca_applicationrequirements_mltrainingworkload_v1_0_0",
"fields": {
"name": "ApplicationRequirements-MLTrainingWorkload",
"description": "Machine Learning training workload requirements for computer vision model",
"tags": "GPU machine learning training computer vision requirements 32GB high priority gold SLA eu-central eu-west",
"type": "application_requirements"
}
}'
```

Expected: `{"doc_id": "tosca_applicationrequirements_mltrainingworkload_v1_0_0", "status": "indexed"}`

---

## Step 4 — Search with free text sentences

Now the same artifacts can be found using plain English — no knowledge of field names
or document structure required. The semantic search understands the *intent* of the
question and matches it against the indexed content.

The semantic search endpoint is:
```
GET http://193.225.250.240/optimusdbN/api/v1/semantic/search?q=...&top_k=5
```

### "Which applications need a GPU?"

```bash
curl -s "http://193.225.250.240/optimusdb1/api/v1/semantic/search\
?q=which+applications+need+a+GPU&top_k=5" | python3 -m json.tool
```

Expected: `tosca_applicationrequirements_mltrainingworkload_v1_0_0` and
`tosca_edgecluster_capacityprofile_v1_0_0` ranked highest.

---

### "Show me available infrastructure in Europe"

```bash
curl -s "http://193.225.250.240/optimusdb1/api/v1/semantic/search\
?q=available+infrastructure+in+Europe&top_k=5" | python3 -m json.tool
```

Expected: `tosca_edgecluster_capacityprofile_v1_0_0` ranked first.

---

### "I need to deploy a web application with a database"

```bash
curl -s "http://193.225.250.240/optimusdb1/api/v1/semantic/search\
?q=deploy+web+application+with+database&top_k=5" | python3 -m json.tool
```

Expected: `tosca_webapp_microservicesapplication_v1_0_0` ranked first.

---

### "What is ready to be deployed right now?"

```bash
curl -s "http://193.225.250.240/optimusdb1/api/v1/semantic/search\
?q=ready+to+be+deployed+right+now&top_k=5" | python3 -m json.tool
```

Expected: `tosca_deploymentplan_webapp_release_v1_0_0` ranked first.

---

### "Find me something that uses Kubernetes and monitoring"

```bash
curl -s "http://193.225.250.240/optimusdb1/api/v1/semantic/search\
?q=Kubernetes+and+monitoring&top_k=5" | python3 -m json.tool
```

Expected: `tosca_hybridinfrastructure_swarmdeployment_v1_0_0` ranked first.

---

### "What do I need for a machine learning training job?"

```bash
curl -s "http://193.225.250.240/optimusdb1/api/v1/semantic/search\
?q=machine+learning+training+job+requirements&top_k=5" | python3 -m json.tool
```

Expected: `tosca_applicationrequirements_mltrainingworkload_v1_0_0` ranked first.

---

### Cross-node query — ask from node 3, find documents stored on nodes 1 and 2

The capacity profile was indexed on node 2. Querying from node 3 retrieves it via
GossipSub — node 3 fans out the query, node 2 responds with its local ANN result,
and node 3 merges and returns the final ranking.

```bash
curl -s "http://193.225.250.240/optimusdb3/api/v1/semantic/search\
?q=edge+cluster+GPU+available+capacity&top_k=5" | python3 -m json.tool
```

The `source_node` field in the result for `tosca_edgecluster_capacityprofile_v1_0_0`
will point to node 2's peer ID — confirming cross-node retrieval via GossipSub.

---

## Why this matters for Swarmchestrate

The structured queries in Step 3 require the user to know the exact field names
(`metadata.region`, `topology.gpu_accelerator_01.properties.available`, etc.).
The semantic queries in Step 4 require nothing — just intent expressed in natural language.

Both approaches return the same artifacts. The semantic search layer adds a
conversational interface on top of the existing structured knowledge base, which is
directly relevant to the Swarmchestrate vision of intelligent, self-describing
orchestration across the computing continuum.

| User intent | Structured query approach | Semantic approach |
|---|---|---|
| Find EU capacity with GPU | `$and` with 5 nested criteria | *"available infrastructure in Europe with GPU"* |
| Find ML workload needs | Filter on `document_type` + `metadata.priority` | *"machine learning training requirements"* |
| Find ready deployments | Filter on `execution_status` + `match_score` | *"what is ready to be deployed right now"* |

---

## Cleanup

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "cruddelete", "argcnt": 1},
"dstype": "dsswres",
"criteria": [{"_id": {"$regex": "^tosca_.*"}}]
}'
echo "Cleanup complete"
```

---

## References

- Swarmchestrate project: EU Horizon Europe Grant #101135012
- TOSCA Version 2.0: OASIS Standard, March 2023