# OptimusDB – TOSCA Upload & Query + GitHub Workflow

This repository contains the **OptimusDB** decentralized datastore and a helper PowerShell script (`tosca-upload.ps1`) that demonstrates how to interact with the OptimusDB REST API.
It also documents how to manage this project with Git/GitHub directly from **GoLand** or the terminal.

---

## 📌 Features
- **Connectivity Check**: Verifies reachability of the OptimusDB REST service.
- **TOSCA Upload**: Encodes a YAML file as Base64 and posts it to `/upload`.
- **Datastore Query**: Queries the `tosca_imported` store using the `/command` API with the `crudget` method.
- **Peers Listing**: Retrieves connected libp2p peers.
- **Logs & Benchmarks**: Displays logs and benchmarking metrics (if enabled).
- **GitHub Workflow**: Instructions for pushing your local Go project into this repository.

---

## ⚙️ Prerequisites
- Windows 10/11 with **PowerShell 5.1+** (or PowerShell Core).
- Running OptimusDB node with REST API exposed (default: `http://localhost:18001/swarmkb`).
- A valid TOSCA YAML file (default path: `C:\Users\georg\Desktop\mytosca.yaml`).
- **Git installed** (`git --version`).
- **GoLand IDE** (JetBrains).

---

## 🚀 Step-by-Step Example Usage of `tosca-upload.ps1`

Follow these steps to upload a TOSCA file and query it back from OptimusDB.

### 1️⃣ Clone the repository
```powershell
git clone https://github.com/georgeGeorgakakos/optimusdb.git
cd optimusdb

# OptimusDB – TOSCA Upload & Query + GitHub Workflow

This repository contains the **OptimusDB** decentralized datastore and a helper PowerShell script (`tosca-upload.ps1`) that demonstrates how to interact with the OptimusDB REST API.
It also documents how to manage this project with Git/GitHub directly from **GoLand** or the terminal.

---

## 📌 Features
- **Connectivity Check**: Verifies reachability of the OptimusDB REST service.
- **TOSCA Upload**: Encodes a YAML file as Base64 and posts it to `/upload`.
- **Datastore Query**: Queries the `tosca_imported` store using the `/command` API with the `crudget` method.
- **Peers Listing**: Retrieves connected libp2p peers.
- **Logs & Benchmarks**: Displays logs and benchmarking metrics (if enabled).
- **GitHub Workflow**: Instructions for pushing your local Go project into this repository.

---

## ⚙️ Prerequisites
- Windows 10/11 with **PowerShell 5.1+** (or PowerShell Core).
- Running OptimusDB node with REST API exposed (default: `http://localhost:18001/swarmkb`).
- A valid TOSCA YAML file (default path: `C:\Users\georg\Desktop\mytosca.yaml`).
- **Git installed** (`git --version`).
- **GoLand IDE** (JetBrains).

---

## 🚀 Step-by-Step Example Usage of `tosca-upload.ps1`

Follow these steps to upload a TOSCA file and query it back from OptimusDB.

### 1️⃣ Clone the repository
```powershell
git clone https://github.com/georgeGeorgakakos/optimusdb.git
cd optimusdb
