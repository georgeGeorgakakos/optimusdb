# OptimusDB Integration with EMS

## Overview

This document describes the integration between **OptimusDB** and **EMS (Event Management System)** using ActiveMQ STOMP protocol. OptimusDB is a knowledge base database system built in Go, and this integration enables real-time event consumption from EMS via message queue subscriptions.

The integration provides automatic reconnection, durable subscriptions, message normalization, and comprehensive logging for production Kubernetes deployments.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Message Format](#message-format)
- [Usage](#usage)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Monitoring & Logging](#monitoring--logging)
- [Troubleshooting](#troubleshooting)
- [API Reference](#api-reference)

## Features

- **STOMP Protocol Support**: Connects to ActiveMQ via STOMP (Simple Text Oriented Messaging Protocol)
- **Auto-Reconnection**: Built-in reconnection logic with configurable retry intervals
- **Durable Subscriptions**: Optional durable subscriptions for guaranteed message delivery
- **Message Normalization**: Automatic conversion of Java-style `{key=value}` format to valid JSON
- **Event Persistence**: All EMS events are logged to the OptimusDB database
- **Kubernetes Native**: Service discovery via Kubernetes DNS with fallback to IP resolution
- **Health Monitoring**: Connection state tracking with callbacks for connected/disconnected events
- **Comprehensive Logging**: Integration with OptimusDB logging system for debugging

## Architecture

```
┌──────────────────┐         STOMP/ActiveMQ        ┌─────────────────┐
│                  │         (Port 61610)           │                 │
│  EMS Broker      │◄────────────────────────────►│   OptimusDB     │
│  (ActiveMQ)      │                                │  Subscriber     │
│                  │                                │                 │
└──────────────────┘                                └─────────────────┘
│                                                    │
│ Publishes to /topic/*                            │
│                                                    ▼
│                                           ┌─────────────────┐
│                                           │  Message Queue  │
│                                           │    Handler      │
│                                           └─────────────────┘
│                                                    │
│                                                    ▼
│                                           ┌─────────────────┐
│                                           │  Parse & Store  │
│                                           │   EMS Events    │
│                                           └─────────────────┘
│                                                    │
▼                                                    ▼
┌──────────────────┐                             ┌─────────────────┐
│  Kubernetes      │                             │  PostgreSQL     │
│  Service DNS     │                             │  (optimusdb)    │
└──────────────────┘                             └─────────────────┘
```

### Key Components

1. **EMS Broker**: ActiveMQ message broker exposing STOMP endpoint
2. **OptimusDB Subscriber**: Go application that subscribes to EMS topics
3. **Message Handler**: Parses and processes incoming EMS messages
4. **Event Storage**: Persists all events to PostgreSQL database
5. **Logger**: Centralized logging system for monitoring and debugging

## Prerequisites

- **Go**: Version 1.16 or higher
- **OptimusDB**: Running instance (see [github.com/georgeGeorgakakos/optimusdb](https://github.com/georgeGeorgakakos/optimusdb))
- **ActiveMQ**: EMS broker with STOMP protocol enabled (default port 61610)
- **Kubernetes**: For production deployment (optional for local development)
- **PostgreSQL**: Database backend for OptimusDB

### Go Dependencies

```bash
go get github.com/go-stomp/stomp
go get golang.org/x/net/context
```

## Configuration

### Environment Variables

Configure the EMS integration using the following environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `EMS_SERVICE_NAME` | `ems-broker.default.svc.cluster.local` | Kubernetes service name or hostname |
| `EMS_STOMP_PORT` | `61610` | STOMP protocol port |
| `EMS_TOPIC` | `/topic/>` | Topic pattern to subscribe to (use `>` for wildcard) |
| `MQ_USER` | `aaa` | ActiveMQ username |
| `MQ_PASS` | `111` | ActiveMQ password |
| `MQ_CLIENT_ID` | (auto-generated) | Client ID for durable subscriptions |
| `EMS_DURABLE` | `true` | Enable durable subscriptions |
| `EMS_SUB_NAME` | `optimusdb-ems` | Subscription name for durable mode |
| `EMS_USE_IP` | `false` | Use IP address instead of DNS |
| `EMS_NAMESPACE` | `messaging` | Kubernetes namespace for EMS broker |

### Example Configuration

#### Local Development

```bash
export EMS_SERVICE_NAME="localhost"
export EMS_STOMP_PORT="61610"
export EMS_TOPIC="/topic/events"
export MQ_USER="admin"
export MQ_PASS="admin"
export MQ_CLIENT_ID="optimusdb-local"
```

#### Kubernetes Deployment

```yaml
env:
- name: EMS_SERVICE_NAME
value: "ems-broker"
- name: EMS_NAMESPACE
value: "messaging"
- name: EMS_STOMP_PORT
value: "61610"
- name: EMS_TOPIC
value: "/topic/>"
- name: MQ_USER
valueFrom:
secretKeyRef:
name: ems-credentials
key: username
- name: MQ_PASS
valueFrom:
secretKeyRef:
name: ems-credentials
key: password
- name: MQ_CLIENT_ID
valueFrom:
fieldRef:
fieldPath: metadata.name
- name: EMS_DURABLE
value: "true"
```

## Message Format

### Expected EMS Message Structure

EMS messages should follow this JSON format:

```json
{
"action": "create",
"resource": "ticket",
"params": {
"id": "12345",
"status": "open",
"priority": "high",
"description": "System alert"
}
}
```

### Message Normalization

The integration includes automatic normalization for Java-style messages:

**Input (Java format):**
```
{action=create, resource=ticket, params={id=12345, status=open}}
```

**Output (Normalized JSON):**
```json
{
"action": "create",
"resource": "ticket",
"params": {
"id": "12345",
"status": "open"
}
}
```

### Message Processing Flow

1. **Receive**: Message arrives from EMS topic
2. **Parse**: Attempt JSON unmarshal
3. **Normalize**: If parsing fails, apply normalization and retry
4. **Store**: Persist raw message and parsed fields to database
5. **Process**: Execute domain-specific logic via `ProcessEMS()`
6. **Log**: Record event in OptimusDB logging system

## Usage

### Starting the EMS Subscriber

```go
package main

import (
"context"
"log"
"optimusdb/app"
)

func main() {
// Initialize OptimusDB
db := app.NewKnowledgeBaseDB()

// Start EMS subscriber with context
ctx := context.Background()
cleanup, err := db.StartEMSSubscriber(ctx)
if err != nil {
log.Fatalf("Failed to start EMS subscriber: %v", err)
}
defer cleanup()

// Keep application running
select {}
}
```

### Sending Messages to EMS

```go
// Send a message to EMS
message := []byte(`{"action":"update","resource":"status","params":{"value":"active"}}`)
err := db.EMSSend("/topic/updates", "application/json", message)
if err != nil {
log.Printf("Failed to send message: %v", err)
}
```

### Custom Message Handler

The default message handler can be extended by modifying `ProcessEMS`:

```go
func (db *KnowledgeBaseDB) ProcessEMS(action, resource string, params map[string]interface{}) error {
switch action {
case "create":
return db.handleCreate(resource, params)
case "update":
return db.handleUpdate(resource, params)
case "delete":
return db.handleDelete(resource, params)
default:
return fmt.Errorf("unknown action: %s", action)
}
}
```

## Kubernetes Deployment

### Service Definition

```yaml
apiVersion: v1
kind: Service
metadata:
name: ems-broker
namespace: messaging
spec:
selector:
app: activemq
ports:
- name: stomp
port: 61610
targetPort: 61610
- name: web
port: 8161
targetPort: 8161
```

### OptimusDB Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
name: optimusdb
spec:
replicas: 1
selector:
matchLabels:
app: optimusdb
template:
metadata:
labels:
app: optimusdb
spec:
containers:
- name: optimusdb
image: optimusdb:latest
env:
- name: EMS_SERVICE_NAME
value: "ems-broker"
- name: EMS_NAMESPACE
value: "messaging"
- name: EMS_STOMP_PORT
value: "61610"
- name: MQ_USER
valueFrom:
secretKeyRef:
name: ems-credentials
key: username
- name: MQ_PASS
valueFrom:
secretKeyRef:
name: ems-credentials
key: password
- name: MQ_CLIENT_ID
valueFrom:
fieldRef:
fieldPath: metadata.name
```

### Secret for Credentials

```yaml
apiVersion: v1
kind: Secret
metadata:
name: ems-credentials
namespace: default
type: Opaque
stringData:
username: "aaa"
password: "111"
```

## Monitoring & Logging

### Connection Events

The integration provides callbacks for monitoring connection state:

```go
service.OnConnected(func() {
log.Println("EMS connected successfully")
})

service.OnDisconnected(func(err error) {
log.Printf("EMS disconnected: %v", err)
})
```

### Event Logging

All EMS events are logged to two locations:

1. **EMS Events Table**: Full message details with parsed fields
- Timestamp
- Host ID
- Client ID
- Topic
- Action
- Resource
- Parameters (JSON)
- Raw message body

2. **Optimus Log**: Summarized log entries
```
INFO: EMS recv action=create resource=ticket body={...}
ERROR: EMS recv (unmarshal failed): {invalid json...}
```

### Database Queries

Query recent EMS events:

```sql
SELECT
timestamp,
action,
resource,
params,
raw_body
FROM ems_events
WHERE timestamp > NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC;
```

Query connection logs:

```sql
SELECT
timestamp,
level,
message
FROM optimus_log
WHERE source = 'ems'
ORDER BY timestamp DESC
LIMIT 100;
```

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to EMS broker

```
ERROR: EMS MQ connect failed (ems-broker.messaging.svc.cluster.local:61610): connection refused
```

**Solutions**:
1. Verify EMS broker is running: `kubectl get pods -n messaging`
2. Check service endpoints: `kubectl get endpoints ems-broker -n messaging`
3. Test connectivity: `kubectl exec -it optimusdb-pod -- nc -zv ems-broker.messaging 61610`
4. Enable IP resolution: Set `EMS_USE_IP=true`

### Message Parsing Errors

**Problem**: Messages failing to parse

```
ERROR: EMS recv (unmarshal failed): {action=create, resource=ticket}
```

**Solutions**:
1. Check message format matches expected JSON structure
2. Normalization should handle Java-style format automatically
3. Review raw messages in `ems_events` table
4. Verify EMS is sending valid data

### Durable Subscription Issues

**Problem**: Not receiving messages after restart

**Solutions**:
1. Ensure `MQ_CLIENT_ID` is set and consistent across restarts
2. Verify `EMS_DURABLE=true` is configured
3. Check ActiveMQ admin console for subscription status
4. Ensure `EMS_SUB_NAME` is unique per consumer

### High Memory Usage

**Problem**: Memory consumption increasing over time

**Solutions**:
1. Implement message processing timeouts
2. Add database connection pooling limits
3. Archive old EMS events periodically
4. Monitor goroutine leaks with pprof

## API Reference

### StartEMSSubscriber

Starts the EMS subscriber service with automatic reconnection.

```go
func (db *KnowledgeBaseDB) StartEMSSubscriber(ctx context.Context) (cleanup func() error, err error)
```

**Parameters**:
- `ctx`: Context for cancellation and timeouts

**Returns**:
- `cleanup`: Function to gracefully shutdown the subscriber
- `err`: Error if connection fails

**Example**:
```go
cleanup, err := db.StartEMSSubscriber(context.Background())
if err != nil {
return err
}
defer cleanup()
```

### EMSSend

Sends a message to an EMS destination.

```go
func (db *KnowledgeBaseDB) EMSSend(dest, contentType string, body []byte) error
```

**Parameters**:
- `dest`: Destination topic or queue (e.g., `/topic/notifications`)
- `contentType`: MIME type (e.g., `application/json`)
- `body`: Message payload as byte array

**Example**:
```go
msg := []byte(`{"status":"complete"}`)
err := db.EMSSend("/topic/status", "application/json", msg)
```

### handleEMSMessage

Internal handler for processing incoming messages (can be overridden).

```go
func (db *KnowledgeBaseDB) handleEMSMessage(body []byte) error
```

### ProcessEMS

Domain-specific message processing logic.

```go
func (db *KnowledgeBaseDB) ProcessEMS(action, resource string, params map[string]interface{}) error
```

## Contributing

Contributions are welcome! Please see the main OptimusDB repository:
https://github.com/georgeGeorgakakos/optimusdb

## License

This integration is part of the OptimusDB project. Please refer to the main repository for license information.