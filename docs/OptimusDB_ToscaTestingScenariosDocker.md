# 🐳 Quick Start Guide - Docker Desktop Edition

## Your Setup

You have **OptimusDB running in Docker Desktop** with 8 containers:

```bash
optimusdb1 → localhost:18001
optimusdb2 → localhost:18002
optimusdb3 → localhost:18003
optimusdb4 → localhost:18004
optimusdb5 → localhost:18005
optimusdb6 → localhost:18006
optimusdb7 → localhost:18007
optimusdb8 → localhost:18008
```

---

## 🚀 Get Started in 3 Steps

### Step 1: Download the Docker Scripts

Download these 3 files:
- **[TOSCA-Upload-Query-Docker.ps1](computer:///mnt/user-data/outputs/TOSCA-Upload-Query-Docker.ps1)** ⬅️ Single file upload
- **[Batch-Upload-TOSCA-Files-Docker.ps1](computer:///mnt/user-data/outputs/Batch-Upload-TOSCA-Files-Docker.ps1)** ⬅️ Batch upload (USE THIS)


### Step 2: Put Scripts in Same Folder as TOSCA Files

```
C:\projectfolder....\repoScript\Tosca
├── TOSCA-Upload-Query-Docker.ps1
├── Batch-Upload-TOSCA-Files-Docker.ps1
├── sample_1_application_description.yaml
├── sample_2_capacity_description.yaml
├── sample_3_opentofu_tosca_template.yaml
├── sample_4_deployment_release_plan.yaml
└── sample_5_application_requirements.yaml
```

### Step 3: Run the Batch Upload

```powershell
cd C:\projectfolder....\repoScript\Tosca
.\Batch-Upload-TOSCA-Files-Docker.ps1
```

**That's it!** 🎉

---

## ✅ What to Expect

The script will:

1. ✅ Find your OptimusDB containers automatically
2. ✅ Discover ports (18001, 18002, etc.)
3. ✅ Upload all 5 TOSCA files
4. ✅ Query OrbitDB and SQLite
5. ✅ Show results and statistics

**Expected Output:**

```
╔════════════════════════════════════════════════════════════════╗
║   Swarmchestrate - Batch TOSCA Upload Script                  ║
║         Upload All Sample Files to Docker Containers          ║
╚════════════════════════════════════════════════════════════════╝

Configuration:
TOSCA Directory:    .
Container Pattern:  optimusdb*
Delay:              3 seconds

✓ Upload script found
✓ Docker is running
✓ Found 8 OptimusDB container(s)
- optimusdb1
- optimusdb2
- optimusdb3
- optimusdb4
- optimusdb5
- optimusdb6
- optimusdb7
- optimusdb8

╔════════════════════════════════════════════════════════════════╗
║ Starting Batch Upload                                         ║
╚════════════════════════════════════════════════════════════════╝

[1/5] Processing: sample_1_application_description.yaml
Type:        ApplicationDescription
Description: E-commerce web application with microservices
Datastore:   ADT Datastore
Uploading...
✓ Upload completed

... (continues for all 5 files)

╔════════════════════════════════════════════════════════════════╗
║ Batch Upload Summary                                          ║
╚════════════════════════════════════════════════════════════════╝

Results:
Total Files:      5
Successful:       5
Failed:           0

Detailed Results:
--------------------------------------------------------------------------------
✓ sample_1_application_description.yaml
Type:        ApplicationDescription
Status:      Success
Template ID: QmXxx...

✓ sample_2_capacity_description.yaml
Type:        CapacityDescription
Status:      Success
Template ID: QmYyy...

... (all 5 files)

✓ Batch upload completed!
```

---

## 🔍 Verify It Worked

### Method 1: Check the Script Output
Look for `✓ Upload completed` and `Template ID: QmXxx...` for each file.

### Method 2: Test API Directly
```powershell
# Test connectivity
Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/peers"

# Should return JSON with peer information
```

### Method 3: Check Results File
The script creates a JSON file: `batch-upload-results-YYYYMMDD-HHMMSS.json`

```powershell
# View results
Get-Content .\batch-upload-results-*.json | ConvertFrom-Json | Format-List
```

---

## 🛠️ Troubleshooting

### Problem: "No OptimusDB containers found"

**Solution:**
```powershell
# Check containers are running
docker ps | Select-String "optimusdb"

# If not running, start them
docker start optimusdb1 optimusdb2 optimusdb3 optimusdb4 optimusdb5 optimusdb6 optimusdb7 optimusdb8
```

### Problem: "Connection failed" or "Not reachable"

**Solution:**
```powershell
# Test one container directly
curl http://localhost:18001/swarmkb/peers

# Check container logs
docker logs optimusdb1

# Make sure container is healthy
docker ps --filter "name=optimusdb1"
```

### Problem: "Docker is not running"

**Solution:**
1. Start Docker Desktop
2. Wait for it to fully start (check system tray icon)
3. Try again

---

## 📊 Key Differences from Kubernetes Scripts

| What | Kubernetes Scripts | Docker Scripts (NEW) |
|------|-------------------|---------------------|
| **Discovery** | kubectl | docker ps |
| **Target** | Node IP + Service Port | localhost + Host Port |
| **Namespace** | Required | Not needed |
| **File Names** | `TOSCA-Upload-Query.ps1` | `TOSCA-Upload-Query-Docker.ps1` |

---

## 💡 Pro Tips

### Tip 1: Upload Single Files
```powershell
.\TOSCA-Upload-Query-Docker.ps1 `
-ToscaFile "my_custom_app.yaml" `
-ToscaType ApplicationDescription
```

### Tip 2: Use Custom Container Names
If your containers are named differently:
```powershell
.\Batch-Upload-TOSCA-Files-Docker.ps1 -ContainerPattern "mydb*"
```

### Tip 3: Faster Uploads
Reduce delay between uploads:
```powershell
.\Batch-Upload-TOSCA-Files-Docker.ps1 -DelayBetweenUploads 1
```

### Tip 4: Check Docker Network
```powershell
docker network inspect swarmnet
```

---

## 📁 Complete File List

### Docker Scripts (Use These!)
- ✅ **TOSCA-Upload-Query-Docker.ps1** - Upload & query single files
- ✅ **Batch-Upload-TOSCA-Files-Docker.ps1** - Batch upload all files
- ✅ **README-Docker.md** - Full documentation

### TOSCA Sample Files
- ✅ **sample_1_application_description.yaml** - App description
- ✅ **sample_2_capacity_description.yaml** - Capacity info
- ✅ **sample_3_opentofu_tosca_template.yaml** - Infrastructure template
- ✅ **sample_4_deployment_release_plan.yaml** - Deployment plan
- ✅ **sample_5_application_requirements.yaml** - Requirements

### Original Kubernetes Scripts (Don't Use for Docker)
- ❌ TOSCA-Upload-Query.ps1 - For Kubernetes only
- ❌ Batch-Upload-TOSCA-Files.ps1 - For Kubernetes only
- ❌ TOSCA-Query.ps1 - For Kubernetes only

---


---

Need help? Check **[README-Docker.md](computer:///mnt/user-data/outputs/README-Docker.md)** for detailed documentation.