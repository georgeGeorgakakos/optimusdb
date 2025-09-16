# TOSCA Upload & Query Script

This repository contains a PowerShell script (`tosca-upload.ps1`) that demonstrates how to interact with **OptimusDB** via its REST API endpoints.
It allows you to:

1. Upload a TOSCA YAML file into the decentralized datastore (`tosca_imported`).
2. Query the datastore by ID using the `/command` API.
3. Inspect connected peers.
4. Retrieve logs for the current hour.
5. (Optional) Call the `/benchmarks` endpoint if benchmarking is enabled.

---

## 📌 Features
- **Connectivity Check**: Verifies reachability of the OptimusDB service before running any calls.
- **TOSCA Upload**: Encodes the YAML file as Base64 and posts it to `/upload`.
- **Datastore Query**: Queries the `tosca_imported` store using the `/command` API with the `crudget` method.
- **Peers Listing**: Retrieves connected libp2p peers.
- **Logs & Benchmarks**: Displays recent logs and benchmarking metrics (if enabled).

---

## ⚙️ Prerequisites
- Windows 10/11 with **PowerShell 5.1+** (or PowerShell Core).
- Running OptimusDB node with REST API exposed (default: `http://localhost:18001/swarmkb`).
- A valid TOSCA YAML file (default path: `C:\Users\georg\Desktop\mytosca.yaml`).

---

## 🚀 Usage

1. Clone this repository:
```powershell
git clone https://github.com/<your-org>/<your-repo>.git
        cd <your-repo>
