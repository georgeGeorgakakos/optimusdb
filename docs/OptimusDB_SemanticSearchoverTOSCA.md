# OptimusDB — Semantic Search over TOSCA 2.0 Artifacts
## Swarmchestrate Academic Demonstration Scenario

> **Context:** This scenario demonstrates OptimusDB as a semantic knowledge base for
> Swarmchestrate TOSCA 2.0 artifacts. The OptimusDB cluster is already deployed and
> running. TOSCA 2.0 files — topology templates, node type definitions, application
> descriptors, policies — are inserted as documents. Afterwards, a user can search
> through them using plain English sentences, without knowing the TOSCA structure,
> field names, or exact terminology used in the files.

---

## The idea in one sentence

Instead of grepping YAML files or reading through topology templates manually, you ask
OptimusDB: *"Which applications need a storage volume?"* — and it finds the right TOSCA
descriptors across the distributed cluster.

---

## Prerequisites

OptimusDB cluster is running with all three nodes healthy:

```bash
for node in optimusdb1 optimusdb2 optimusdb3; do
curl -s http://193.225.250.240/${node}/swarmkb/agent/status | python3 -m json.tool
done
```

---

## Step 1 — Insert TOSCA 2.0 artifacts as documents

Each TOSCA artifact is inserted as a document with its key fields extracted as
searchable text. The `description` field is what the semantic model primarily
indexes — write it as a human-readable summary of what the TOSCA artifact does.

### Application 1 — Web application with database backend

```bash
curl -s -X PUT http://193.225.250.240/optimusdb1/swarmkb/crudput \
-H "Content-Type: application/json" \
-d '{
"store": "tosca_artifacts",
"records": [{
"id": "tosca_app_webshop",
"name": "E-Commerce Web Application",
"description": "A three-tier web application consisting of a frontend web server, an application server running business logic, and a relational database backend. Requires persistent block storage for the database. Exposes an HTTP endpoint on port 443. Scales horizontally at the web tier.",
"tosca_type": "topology_template",
"version": "2.0",
"components": "web_server, app_server, database",
"requires_storage": "true",
"exposes_endpoint": "HTTP 443",
"scalable": "true",
"tags": "web, ecommerce, three-tier, database, storage"
}]
}'
```

### Application 2 — AI inference pipeline

```bash
curl -s -X PUT http://193.225.250.240/optimusdb2/swarmkb/crudput \
-H "Content-Type: application/json" \
-d '{
"store": "tosca_artifacts",
"records": [{
"id": "tosca_app_inference",
"name": "AI Inference Pipeline",
"description": "A machine learning inference service that loads a large language model from object storage and serves predictions over a REST API. Requires GPU compute resources and high-memory nodes. Depends on an S3-compatible object store for model weights. Exposes a REST endpoint on port 8080.",
"tosca_type": "topology_template",
"version": "2.0",
"components": "inference_server, object_storage, load_balancer",
"requires_gpu": "true",
"requires_storage": "true",
"exposes_endpoint": "REST 8080",
"tags": "AI, machine learning, GPU, inference, LLM, REST"
}]
}'
```

### Application 3 — IoT data ingestion pipeline

```bash
curl -s -X PUT http://193.225.250.240/optimusdb3/swarmkb/crudput \
-H "Content-Type: application/json" \
-d '{
"store": "tosca_artifacts",
"records": [{
"id": "tosca_app_iot",
"name": "IoT Sensor Data Pipeline",
"description": "A real-time data ingestion pipeline for IoT sensor streams. Consumes MQTT messages from edge devices, processes them through a stream processor, and stores time-series data in a distributed database. Designed for low-latency edge deployments with intermittent connectivity.",
"tosca_type": "topology_template",
"version": "2.0",
"components": "mqtt_broker, stream_processor, timeseries_db",
"requires_storage": "true",
"edge_compatible": "true",
"exposes_endpoint": "MQTT 1883",
"tags": "IoT, MQTT, edge, streaming, sensor, time-series"
}]
}'
```

### Node type — Compute node with GPU

```bash
curl -s -X PUT http://193.225.250.240/optimusdb1/swarmkb/crudput \
-H "Content-Type: application/json" \
-d '{
"store": "tosca_artifacts",
"records": [{
"id": "tosca_nodetype_gpu_compute",
"name": "GPU Compute Node Type",
"description": "A TOSCA node type representing a compute resource with dedicated GPU support. Suitable for machine learning training and inference workloads. Defines capabilities for CUDA compute, high-bandwidth memory, and NVLink interconnect. Derived from the standard TOSCA compute node type.",
"tosca_type": "node_type",
"version": "2.0",
"derived_from": "tosca.nodes.Compute",
"capabilities": "gpu_compute, cuda, high_memory",
"tags": "GPU, CUDA, compute, machine learning, node type"
}]
}'
```

### Node type — Serverless function

```bash
curl -s -X PUT http://193.225.250.240/optimusdb2/swarmkb/crudput \
-H "Content-Type: application/json" \
-d '{
"store": "tosca_artifacts",
"records": [{
"id": "tosca_nodetype_serverless",
"name": "Serverless Function Node Type",
"description": "A TOSCA node type for event-driven serverless functions. Triggers on HTTP requests, message queue events, or scheduled timers. Stateless by design, auto-scales to zero when idle. Suitable for lightweight processing tasks that do not require persistent infrastructure.",
"tosca_type": "node_type",
"version": "2.0",
"derived_from": "tosca.nodes.WebApplication",
"capabilities": "http_trigger, event_trigger, auto_scaling",
"tags": "serverless, function, event-driven, auto-scale, stateless"
}]
}'
```

### Policy — EU data residency constraint

```bash
curl -s -X PUT http://193.225.250.240/optimusdb3/swarmkb/crudput \
-H "Content-Type: application/json" \
-d '{
"store": "tosca_artifacts",
"records": [{
"id": "tosca_policy_data_residency",
"name": "EU Data Residency Policy",
"description": "A TOSCA placement policy that constrains workload deployment to data centres located within the European Union. Ensures compliance with GDPR data residency requirements. Prevents data from being stored or processed outside EU jurisdiction. Applies to storage nodes and database components.",
"tosca_type": "policy",
"version": "2.0",
"policy_type": "tosca.policies.Placement",
"constraint": "region=EU",
"regulation": "GDPR",
"tags": "GDPR, EU, data residency, compliance, placement policy"
}]
}'
```

---

## Step 2 — Index documents for semantic search

```bash
sleep 5

curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{"store":"tosca_artifacts","doc_id":"tosca_app_webshop","fields":{"name":"E-Commerce Web Application","description":"A three-tier web application with a frontend web server an application server and a relational database backend. Requires persistent block storage. Exposes HTTP on port 443. Scales horizontally.","tosca_type":"topology_template","tags":"web ecommerce database storage"}}'

curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{"store":"tosca_artifacts","doc_id":"tosca_nodetype_gpu_compute","fields":{"name":"GPU Compute Node Type","description":"A TOSCA node type for compute resources with dedicated GPU support for machine learning training and inference. CUDA compute high-bandwidth memory NVLink interconnect.","tosca_type":"node_type","tags":"GPU CUDA compute machine learning"}}'

curl -s -X POST http://193.225.250.240/optimusdb2/swarmkb/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{"store":"tosca_artifacts","doc_id":"tosca_app_inference","fields":{"name":"AI Inference Pipeline","description":"Machine learning inference service loading a large language model from object storage serving predictions over REST API. Requires GPU compute and high-memory nodes.","tosca_type":"topology_template","tags":"AI GPU inference LLM REST"}}'

curl -s -X POST http://193.225.250.240/optimusdb2/swarmkb/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{"store":"tosca_artifacts","doc_id":"tosca_nodetype_serverless","fields":{"name":"Serverless Function Node Type","description":"TOSCA node type for event-driven serverless functions triggered by HTTP requests or message queue events. Stateless auto-scales to zero when idle.","tosca_type":"node_type","tags":"serverless function event-driven auto-scale"}}'

curl -s -X POST http://193.225.250.240/optimusdb3/swarmkb/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{"store":"tosca_artifacts","doc_id":"tosca_app_iot","fields":{"name":"IoT Sensor Data Pipeline","description":"Real-time ingestion pipeline for IoT sensor streams consuming MQTT messages from edge devices. Stream processing and time-series storage. Designed for edge deployments with intermittent connectivity.","tosca_type":"topology_template","tags":"IoT MQTT edge streaming sensor"}}'

curl -s -X POST http://193.225.250.240/optimusdb3/swarmkb/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{"store":"tosca_artifacts","doc_id":"tosca_policy_data_residency","fields":{"name":"EU Data Residency Policy","description":"TOSCA placement policy constraining workload deployment to European Union data centres. GDPR compliance. Prevents data from being processed outside EU jurisdiction.","tosca_type":"policy","tags":"GDPR EU compliance placement policy"}}'

echo "All TOSCA artifacts indexed"
```

---

## Step 3 — Search with free text sentences

No knowledge of TOSCA structure or field names required.

### "Which applications need a GPU?"

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search\
?q=which+applications+need+a+GPU&top_k=5&budget_ms=2000" | python3 -m json.tool
```

Expected: `tosca_app_inference` and `tosca_nodetype_gpu_compute` ranked highest.

---

### "Show me anything that stores data"

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search\
?q=show+me+anything+that+stores+data&top_k=5&budget_ms=2000" | python3 -m json.tool
```

Expected: `tosca_app_webshop`, `tosca_app_iot`, and `tosca_app_inference` — all
three require storage.

---

### "I need something that works on edge devices with poor connectivity"

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search\
?q=works+on+edge+devices+with+poor+connectivity&top_k=5&budget_ms=2000" | python3 -m json.tool
```

Expected: `tosca_app_iot` ranked first.

---

### "What can I use for a REST API that scales automatically?"

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search\
?q=REST+API+that+scales+automatically&top_k=5&budget_ms=2000" | python3 -m json.tool
```

Expected: `tosca_nodetype_serverless` and `tosca_app_inference`.

---

### "Are there any legal or compliance constraints I should know about?"

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search\
?q=legal+compliance+constraints&top_k=5&budget_ms=2000" | python3 -m json.tool
```

Expected: `tosca_policy_data_residency` ranked first — even though the query
never mentions GDPR or EU explicitly.

---

### "Find me a topology with a web server and a database"

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search\
?q=topology+with+web+server+and+database&top_k=5&budget_ms=2000" | python3 -m json.tool
```

Expected: `tosca_app_webshop` ranked first.

---

### "What infrastructure do I need for real-time sensor data?"

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search\
?q=real+time+sensor+data+infrastructure&top_k=5&budget_ms=2000" | python3 -m json.tool
```

Expected: `tosca_app_iot` ranked first.

---

### Cross-node query — documents spread across all three nodes

This query originates at node 3 but the most relevant artifacts (`tosca_app_inference`,
`tosca_nodetype_gpu_compute`) live on nodes 1 and 2 — retrieved via GossipSub:

```bash
curl -s "http://193.225.250.240/optimusdb3/swarmkb/api/v1/semantic/search\
?q=machine+learning+model+serving+infrastructure&top_k=5&budget_ms=2000" \
| python3 -m json.tool
```

The `source_node` field in each result shows which peer contributed it.
Results with a peer ID different from node 3 were retrieved from remote nodes.

---

## Why this matters for Swarmchestrate

As a Swarmchestrate orchestrator manages an increasing number of TOSCA artifacts
across a distributed computing continuum, finding the right descriptor for a given
intent becomes non-trivial. OptimusDB turns the artifact registry into a
conversational knowledge base:

| Question | What OptimusDB finds |
|---|---|
| "What workloads can run on constrained edge nodes?" | Topology templates tagged as edge-compatible |
| "Which node types support high availability?" | Node types with HA capabilities |
| "Find policies that apply to data leaving the EU" | Placement and compliance policy artifacts |
| "Show me applications that expose a message queue" | Topologies with MQTT or AMQP endpoints |
| "What do I need for a video streaming service?" | Node types and topologies with bandwidth requirements |

The key property is that none of these questions require the user to know the
internal structure of the TOSCA files — OptimusDB understands the semantic intent
and matches it against the distributed artifact store.

---

## Cleanup

```bash
for node in optimusdb1 optimusdb2 optimusdb3; do
for id in tosca_app_webshop tosca_app_inference tosca_app_iot \
tosca_nodetype_gpu_compute tosca_nodetype_serverless \
tosca_policy_data_residency; do
curl -s -X DELETE "http://193.225.250.240/${node}/swarmkb/crudput/${id}" > /dev/null
done
done
echo "Cleanup complete"
```

---

## References

- Swarmchestrate project: EU Horizon Europe Grant #101135012
- TOSCA Version 2.0: OASIS Standard, March 2023
- OptimusDB paper: Procedia Computer Science, Vol. 278, CENTERIS/ProjMAN/HCist 2025