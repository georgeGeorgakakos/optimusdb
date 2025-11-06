# Verifiable Credentials API - Complete Documentation

## 📖 Overview

The Verifiable Credentials API implements the W3C Verifiable Credentials standard with decentralized storage using IPFS and CRUD Data stores. Store, query, verify, and manage digital credentials in a distributed, tamper-proof system.

**Base URL**: `http://localhost:18001/swarmkb/credentials`

**Storage Architecture**:
- 🗃️ **IPFS**: Immutable content-addressed storage
- 📦 **DocumentStore**: Distributed queryable database
- 📜 **EventLog**: Append-only audit trail
- ⚡ **SQLite**: Fast indexed queries

---

## 🚀 Quick Start

### 1. Store Your First Credential

```bash
curl -X POST http://localhost:18001/swarmkb/credentials \
-H "Content-Type: application/json" \
-d '{
"@context": ["https://www.w3.org/2018/credentials/v1"],
"type": ["VerifiableCredential"],
"issuer": "did:example:university",
"issuanceDate": "2025-11-06T12:00:00Z",
"credentialSubject": {
"id": "did:example:student123",
"name": "Alice Smith",
"degree": "Bachelor of Science"
}
}'
```

**Response**:
```json
{
"success": true,
"credentialId": "urn:uuid:vc-abc123def456",
"message": "Verifiable Credential stored successfully",
"ipfsHash": "QmXg9Pp2ytZ6xb9...",
"orbitDbHash": "urn:uuid:vc-abc123def456"
}
```

### 2. Retrieve the Credential

```bash
curl http://localhost:18001/swarmkb/credentials/get/urn:uuid:vc-abc123def456
```

### 3. List All Credentials

```bash
curl http://localhost:18001/swarmkb/credentials
```

---

## 📚 API Endpoints

### Summary Table

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/credentials` | Store a new credential |
| GET | `/credentials` | List all credentials (paginated) |
| GET | `/credentials/get/{id}` | Get specific credential |
| POST | `/credentials/query` | Advanced search with filters |
| GET | `/credentials/issuer/{issuerId}` | Get credentials by issuer |
| GET | `/credentials/subject/{subjectId}` | Get credentials by subject |
| POST | `/credentials/revoke` | Revoke a credential |
| POST | `/credentials/verify` | Verify credential authenticity |

---

## 1️⃣ Store Credential

Store a new verifiable credential.

**`POST /swarmkb/credentials`**

### Request Body

```json
{
"@context": ["https://www.w3.org/2018/credentials/v1"],
"id": "https://example.com/credentials/123" (optional),
"type": ["VerifiableCredential", "CustomType"],
"issuer": "did:example:issuer or URL",
"issuanceDate": "2025-11-06T12:00:00Z",
"expirationDate": "2030-11-06T12:00:00Z" (optional),
"credentialSubject": {
"id": "did:example:subject",
...custom properties
},
"proof": {
"type": "Ed25519Signature2020",
"created": "2025-11-06T12:00:00Z",
"verificationMethod": "https://example.com/keys/1",
"proofPurpose": "assertionMethod",
"proofValue": "base64encodedproof..."
}
}
```

### Examples

**Basic Credential**:
```bash
curl -X POST http://localhost:18001/swarmkb/credentials \
-H "Content-Type: application/json" \
-d '{
"@context": ["https://www.w3.org/2018/credentials/v1"],
"type": ["VerifiableCredential"],
"issuer": "did:example:company",
"issuanceDate": "2025-11-06T12:00:00Z",
"credentialSubject": {
"id": "did:example:employee",
"name": "Bob Johnson",
"role": "Engineer"
}
}'
```

**University Degree**:
```bash
curl -X POST http://localhost:18001/swarmkb/credentials \
-H "Content-Type: application/json" \
-d '{
"@context": [
"https://www.w3.org/2018/credentials/v1",
"https://www.w3.org/2018/credentials/examples/v1"
],
"type": ["VerifiableCredential", "UniversityDegreeCredential"],
"issuer": "https://university.edu",
"issuanceDate": "2024-05-15T00:00:00Z",
"credentialSubject": {
"id": "did:example:student456",
"degree": {
"type": "BachelorDegree",
"name": "Bachelor of Science in Computer Science"
}
}
}'
```

---

## 2️⃣ List All Credentials

Get paginated list of all credentials.

**`GET /swarmkb/credentials?limit={limit}&offset={offset}`**

### Query Parameters

- `limit` (optional): Results per page (default: 50, max: 100)
- `offset` (optional): Skip this many results (default: 0)

### Example

```bash
# First page (0-49)
curl "http://localhost:18001/swarmkb/credentials?limit=50&offset=0"

# Second page (50-99)
curl "http://localhost:18001/swarmkb/credentials?limit=50&offset=50"
```

### Response

```json
{
"success": true,
"count": 2,
"credentials": [
{
"credentialId": "urn:uuid:vc-123",
"issuerId": "did:example:university",
"subjectId": "did:example:student456",
"credentialType": ["VerifiableCredential", "DegreeCredential"],
"issuanceDate": "2024-05-15T00:00:00Z",
"expirationDate": null,
"status": "active",
"storedAt": "2025-11-06T14:30:00Z",
"ipfsHash": "QmXg9Pp2ytZ6xb9...",
"orbitDbHash": "urn:uuid:vc-123"
}
]
}
```

---

## 3️⃣ Get Credential by ID

Retrieve full credential details.

**`GET /swarmkb/credentials/get/{credentialId}`**

### Example

```bash
# Simple ID
curl http://localhost:18001/swarmkb/credentials/get/test-001

# URL-encoded ID
curl http://localhost:18001/swarmkb/credentials/get/urn:uuid:vc-abc123
```

### Response

```json
{
"success": true,
"credential": {
"@context": ["https://www.w3.org/2018/credentials/v1"],
"id": "urn:uuid:vc-abc123",
"type": ["VerifiableCredential"],
"issuer": "did:example:university",
"issuanceDate": "2024-05-15T00:00:00Z",
"credentialSubject": {
"id": "did:example:student456",
"name": "Alice Smith"
}
},
"metadata": {
"credentialId": "urn:uuid:vc-abc123",
"status": "active",
"storedAt": "2025-11-06T14:30:00Z",
"ipfsHash": "QmXg9...",
"orbitDbHash": "urn:uuid:vc-abc123"
}
}
```

---

## 4️⃣ Query Credentials (Advanced Search)

Search with multiple filters.

**`POST /swarmkb/credentials/query`**

### Request Body

```json
{
"issuerId": "did:example:issuer",
"subjectId": "did:example:subject",
"credentialType": "Degree",
"status": "active",
"issuedAfter": "2024-01-01T00:00:00Z",
"issuedBefore": "2025-12-31T23:59:59Z",
"limit": 20,
"offset": 0
}
```

**All fields are optional**

### Examples

**Active credentials only**:
```bash
curl -X POST http://localhost:18001/swarmkb/credentials/query \
-H "Content-Type: application/json" \
-d '{"status": "active", "limit": 10}'
```

**By issuer and type**:
```bash
curl -X POST http://localhost:18001/swarmkb/credentials/query \
-H "Content-Type: application/json" \
-d '{
"issuerId": "did:example:university",
"credentialType": "Degree",
"status": "active"
}'
```

**Date range**:
```bash
curl -X POST http://localhost:18001/swarmkb/credentials/query \
-H "Content-Type: application/json" \
-d '{
"issuedAfter": "2024-01-01T00:00:00Z",
"issuedBefore": "2024-12-31T23:59:59Z",
"limit": 50
}'
```

---

## 5️⃣ Get Credentials by Issuer

Get all credentials from a specific issuer.

**`GET /swarmkb/credentials/issuer/{issuerId}`**

### Example

```bash
curl http://localhost:18001/swarmkb/credentials/issuer/did:example:university

# URL-encoded issuer
curl http://localhost:18001/swarmkb/credentials/issuer/https%3A%2F%2Funiversity.edu
```

### Response

```json
{
"success": true,
"count": 5,
"issuerId": "did:example:university",
"credentials": [...]
}
```

---

## 6️⃣ Get Credentials by Subject

Get all credentials for a specific subject (holder).

**`GET /swarmkb/credentials/subject/{subjectId}`**

### Example

```bash
curl http://localhost:18001/swarmkb/credentials/subject/did:example:student456
```

### Response

```json
{
"success": true,
"count": 3,
"subjectId": "did:example:student456",
"credentials": [...]
}
```

---

## 7️⃣ Revoke Credential

Mark a credential as revoked.

**`POST /swarmkb/credentials/revoke`**

### Request Body

```json
{
"credentialId": "urn:uuid:vc-abc123",
"reason": "Reason for revocation"
}
```

### Example

```bash
curl -X POST http://localhost:18001/swarmkb/credentials/revoke \
-H "Content-Type: application/json" \
-d '{
"credentialId": "urn:uuid:vc-abc123",
"reason": "Credential was issued in error"
}'
```

### Response

```json
{
"success": true,
"message": "Credential revoked successfully",
"credentialId": "urn:uuid:vc-abc123",
"revokedAt": "2025-11-06T16:45:00Z"
}
```

**Notes**:
- ⚠️ Revocation is permanent
- 📝 Logged in audit trail
- ❌ Revoked credentials fail verification

---

## 8️⃣ Verify Credential

Verify authenticity and validity.

**`POST /swarmkb/credentials/verify`**

### Request Body

Send the complete credential to verify:

```json
{
"@context": ["https://www.w3.org/2018/credentials/v1"],
"id": "urn:uuid:vc-abc123",
"type": ["VerifiableCredential"],
"issuer": "did:example:university",
"issuanceDate": "2024-05-15T00:00:00Z",
"credentialSubject": {
"id": "did:example:student456",
"name": "Alice"
}
}
```

### Example

```bash
curl -X POST http://localhost:18001/swarmkb/credentials/verify \
-H "Content-Type: application/json" \
-d @credential.json
```

### Response - Valid

```json
{
"success": true,
"verified": true,
"message": "Credential is valid"
}
```

### Response - Invalid

```json
{
"success": true,
"verified": false,
"errors": [
"Credential has been revoked",
"Credential has expired"
]
}
```

**Verification Checks**:
1. ✅ W3C structure validation
2. ✅ Proof signature (placeholder)
3. ✅ Exists in database
4. ✅ Not revoked
5. ✅ Not expired
6. ✅ Valid dates

---

## 💻 Integration Examples

### JavaScript/Node.js

```javascript
const axios = require('axios');

const BASE_URL = 'http://localhost:18001/swarmkb/credentials';

// Store credential
async function storeCredential(vc) {
const response = await axios.post(BASE_URL, vc);
return response.data;
}

// Get credential
async function getCredential(id) {
const response = await axios.get(`${BASE_URL}/get/${encodeURIComponent(id)}`);
return response.data.credential;
}

// Query credentials
async function queryCredentials(filters) {
const response = await axios.post(`${BASE_URL}/query`, filters);
return response.data.credentials;
}

// Usage
(async () => {
// Store
const result = await storeCredential({
'@context': ['https://www.w3.org/2018/credentials/v1'],
type: ['VerifiableCredential'],
issuer: 'did:example:issuer',
issuanceDate: new Date().toISOString(),
credentialSubject: {
id: 'did:example:subject',
name: 'Alice'
}
});

console.log('Stored:', result.credentialId);

// Retrieve
const credential = await getCredential(result.credentialId);
console.log('Retrieved:', credential);

// Query
const active = await queryCredentials({ status: 'active' });
console.log('Active credentials:', active.length);
})();
```

### Python

```python
import requests
from datetime import datetime

BASE_URL = 'http://localhost:18001/swarmkb/credentials'

def store_credential(vc):
response = requests.post(BASE_URL, json=vc)
response.raise_for_status()
return response.json()

def get_credential(cred_id):
response = requests.get(f"{BASE_URL}/get/{cred_id}")
response.raise_for_status()
return response.json()['credential']

def query_credentials(filters):
response = requests.post(f"{BASE_URL}/query", json=filters)
response.raise_for_status()
return response.json()['credentials']

# Usage
vc = {
'@context': ['https://www.w3.org/2018/credentials/v1'],
'type': ['VerifiableCredential'],
'issuer': 'did:example:issuer',
'issuanceDate': datetime.utcnow().isoformat() + 'Z',
'credentialSubject': {
'id': 'did:example:subject',
'name': 'Alice'
}
}

result = store_credential(vc)
print(f"Stored: {result['credentialId']}")

credential = get_credential(result['credentialId'])
print(f"Retrieved: {credential['id']}")
```

### cURL Complete Example

```bash
#!/bin/bash

# 1. Store credential
CRED_ID=$(curl -s -X POST http://localhost:18001/swarmkb/credentials \
-H "Content-Type: application/json" \
-d '{
"@context": ["https://www.w3.org/2018/credentials/v1"],
"type": ["VerifiableCredential", "EmployeeCredential"],
"issuer": "did:example:company",
"issuanceDate": "2025-11-06T12:00:00Z",
"credentialSubject": {
"id": "did:example:employee",
"name": "Alice",
"position": "Engineer"
}
}' | jq -r '.credentialId')

echo "✅ Stored: $CRED_ID"

# 2. Retrieve
curl -s http://localhost:18001/swarmkb/credentials/get/$CRED_ID | jq

# 3. Query
curl -s -X POST http://localhost:18001/swarmkb/credentials/query \
-H "Content-Type: application/json" \
-d '{"status": "active", "limit": 5}' | jq

# 4. Revoke
curl -s -X POST http://localhost:18001/swarmkb/credentials/revoke \
-H "Content-Type: application/json" \
-d "{\"credentialId\": \"$CRED_ID\", \"reason\": \"Test\"}" | jq
```

---

## 🎯 Best Practices

### 1. Use Meaningful IDs

```
✅ https://university.edu/credentials/2024/12345
✅ urn:uuid:vc-550e8400-e29b-41d4-a716-446655440000
❌ credential1
```

### 2. Include All Required Fields

```json
{
"@context": ["https://www.w3.org/2018/credentials/v1"],  // Required
"type": ["VerifiableCredential"],                        // Required
"issuer": "did:example:issuer",                          // Required
"issuanceDate": "2025-11-06T12:00:00Z",                  // Required
"credentialSubject": {                                    // Required
"id": "did:example:subject"                            // Recommended
}
}
```

### 3. Set Appropriate Expirations

- **Education**: 5-10 years or permanent
- **Employment**: 1-2 years
- **Health**: 1 year
- **Temporary**: Days/weeks

### 4. Always Verify Before Using

```bash
curl -X POST http://localhost:18001/swarmkb/credentials/verify \
-H "Content-Type: application/json" \
-d @credential.json
```

### 5. Check Revocation Status

```bash
CRED=$(curl -s http://localhost:18001/swarmkb/credentials/get/$ID)
STATUS=$(echo $CRED | jq -r '.metadata.status')
[ "$STATUS" = "active" ] || echo "⚠️ Credential is $STATUS"
```

---

## 📊 Data Models

### Verifiable Credential

```typescript
{
"@context": string[],              // Required: ["https://www.w3.org/2018/credentials/v1", ...]
"id": string,                      // Optional: Unique identifier
"type": string[],                  // Required: ["VerifiableCredential", ...]
"issuer": string | object,         // Required: DID or URL
"issuanceDate": string,            // Required: RFC3339 datetime
"expirationDate"?: string,         // Optional: RFC3339 datetime
"credentialSubject": object,       // Required: Subject data
"proof"?: {                        // Optional: Cryptographic proof
"type": string,
"created": string,
"verificationMethod": string,
"proofPurpose": string,
"proofValue": string
}
}
```

### Credential Metadata

```typescript
{
"credentialId": string,
"issuerId": string,
"subjectId": string,
"credentialType": string[],
"issuanceDate": datetime,
"expirationDate": datetime | null,
"status": "active" | "revoked" | "expired",
"storedAt": datetime,
"ipfsHash": string,
"orbitDbHash": string
}
```

---

## ⚠️ Error Handling

### Error Response Format

```json
{
"success": false,
"error": "Error description"
}
```

### HTTP Status Codes

- `200` OK - Success
- `201` Created - Credential stored
- `400` Bad Request - Invalid input
- `404` Not Found - Credential not found
- `500` Internal Server Error

### Common Errors

```json
// Validation error
{
"success": false,
"error": "validation failed: @context is required"
}

// Not found
{
"success": false,
"error": "credential not found"
}

// Storage error
{
"success": false,
"error": "failed to store in IPFS: connection timeout"
}
```

---

## 🔒 Security

### Production Checklist

- [ ] Enable HTTPS/TLS
- [ ] Add authentication (API keys, OAuth, DIDs)
- [ ] Implement rate limiting
- [ ] Add request validation
- [ ] Implement cryptographic proof verification
- [ ] Set up access control
- [ ] Enable audit logging
- [ ] Regular security audits

### Privacy

- Encrypt sensitive data
- Implement selective disclosure
- Use zero-knowledge proofs where appropriate
- Comply with GDPR/privacy regulations

---

## 📖 Resources

- **W3C VC Spec**: https://www.w3.org/TR/vc-data-model/
- **DID Spec**: https://www.w3.org/TR/did-core/
- **IPFS Docs**: https://docs.ipfs.tech/

---

## 🆘 Support

### Troubleshooting

**Credentials not storing?**
```bash
# Check if OptimusDB is running
ps aux | grep optimusdb

# Check logs
tail -f optimusdb.log
```

**Cannot retrieve?**
```bash
# List all credentials first
curl http://localhost:18001/swarmkb/credentials | jq

# Verify credential ID is correct
curl http://localhost:18001/swarmkb/credentials/get/YOUR_ID | jq
```

**Query returns empty?**
```bash
# Start with broad query
curl -X POST http://localhost:18001/swarmkb/credentials/query \
-H "Content-Type: application/json" \
-d '{"limit": 100}' | jq
```

---

**Version**: 1.0.0
**Last Updated**: November 6, 2025