# PowerShell Election Monitoring Guide

## 📦 Files Included

1. **Monitor-OptimusElection.ps1** - Full-featured monitoring script
2. **Simple-Monitor.ps1** - Lightweight one-line monitor

---


---

### Option : Full Dashboard (Recommended for monitoring)

```powershell
# Full interactive dashboard
.\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node.log"
```

**Shows:**
- Current role (Coordinator/Follower)
- Election status
- Heartbeat counts
- Error counts
- Health score
- Recent activity
- Debug state

---

### Option 3: Stream Mode (Live event feed)

```powershell
# Live stream of events
.\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node.log" -Mode Stream
```

**Shows:**
- Real-time election events
- Heartbeat activity
- Role changes
- Errors and warnings

---

### Option 4: Quick Check (One-time status)

```powershell
# Quick health check
.\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node.log" -Mode Quick
```

**Output:**
```
╔════════════════════════════════════════════════════════════════╗
║          OptimusDB Quick Status Check                           ║
╚════════════════════════════════════════════════════════════════╝

Election Completed: PASS
Role Assigned: PASS
Heartbeats Active: PASS
Error Count OK: PASS

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Results: 4/4 tests passed
🎉 All tests passed! System is healthy.
```

---

## 📖 Detailed Usage

### Monitor-OptimusElection.ps1

#### Parameters

```powershell
-LogFile "string"
    Required. Path to OptimusDB log file.
    Example: "C:\optimusdb\logs\node1.log"

    -RefreshInterval "int"
        Optional. Dashboard refresh interval in seconds (1-60).
        Default: 5
        Example: -RefreshInterval 10

        -Mode "string"
            Optional. Monitor mode: Dashboard, Stream, or Quick
            Default: Dashboard
            Example: -Mode Stream
            ```

            #### Examples

            **Basic Dashboard (5 second refresh):**
            ```powershell
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node.log"
            ```

            **Faster Dashboard (2 second refresh):**
            ```powershell
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node.log" -RefreshInterval 2
            ```

            **Monitor multiple nodes in different windows:**
            ```powershell
            # Window 1
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node1.log"

            # Window 2
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node2.log"

            # Window 3
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node3.log"
            ```

            **Stream mode with color-coded events:**
            ```powershell
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node.log" -Mode Stream
            ```

            **Automated health check in script:**
            ```powershell
            $result = .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node.log" -Mode Quick
            if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ Cluster is healthy"
            } else {
            Write-Host "❌ Cluster has issues"
            }
            ```

            ---

            ## 🎨 Color Coding

            The scripts use colors to make information easier to read:

            - **Green** = Success, healthy, coordinator
            - **Yellow** = Warning, follower
            - **Red** = Error, critical issues
            - **Cyan** = Information
            - **Magenta** = Headers, emphasis

            ---

            ## 📊 Understanding the Output

            ### Dashboard Sections

            #### 1. Node Status
            ```
            📊 NODE STATUS
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Role: ★ COORDINATOR (Leader) ★
            Status: ✅ OPERATIONAL
            ```

            - **Role**: Current node role (Coordinator = Leader, Follower = Worker)
            - **Status**: Current state (Operational, Election in Progress, etc.)

            #### 2. Election Info
            ```
            🗳️  ELECTION INFO
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Last Election: ✓ Completed
            Leader ID: 12D3KooWABC...
            Votes Received: 2
            ```

            - **Last Election**: When last election occurred
            - **Leader ID**: ID of current leader
            - **Votes Received**: How many votes the winner got

            #### 3. Heartbeat Status
            ```
            💓 HEARTBEAT STATUS
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Heartbeats Sent: 123
            Status: 🟢 Sending (every 5s)
            ```

            **For Coordinators:**
            - Shows count of heartbeats sent
            - Should increase by ~12 per minute

            **For Followers:**
            - Shows count of heartbeats received
            - Should match leader's sent count

            #### 4. Health Status
            ```
            🏥 HEALTH STATUS
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Errors: ✅ None
            Overall Health: 🟢 EXCELLENT
            ```

            **Health Scores:**
            - 🟢 EXCELLENT (4/4) - Everything working
            - 🟡 GOOD (3/4) - Minor issues
            - 🟠 DEGRADED (2/4) - Some problems
            - 🔴 POOR (0-1/4) - Major issues

            #### 5. Recent Activity
            ```
            📝 RECENT ACTIVITY (Last 5 events)
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            🗳️  [ELECTION] Winner: 12D3...xyz with 2 votes
            ★  [COORDINATOR] Node 12D3...xyz is now acting as leader
            💓 [HEARTBEAT] Sent heartbeat from leader: 12D3...xyz
            ```

            Shows last 5 important events with icons:
            - 🗳️  = Election events
            - 💓 = Heartbeat activity
            - ★  = Coordinator messages
            - ○  = Follower messages
            - ❌ = Errors

            ---

            ## 🔍 Interpreting Results

            ### ✅ Healthy System

            **Dashboard shows:**
            ```
            Role: ★ COORDINATOR (Leader) ★
            Status: ✅ OPERATIONAL
            Last Election: ✓ Completed
            Heartbeats Sent: 245 (increasing)
            Errors: ✅ None
            Overall Health: 🟢 EXCELLENT
            ```

            **Or for follower:**
            ```
            Role: ○ FOLLOWER
            Status: ✅ OPERATIONAL
            Last Election: ✓ Completed
            Heartbeats Received: 245 (increasing)
            Errors: ✅ None
            Overall Health: 🟢 EXCELLENT
            ```

            ### ⚠️ Warning Signs

            **Election not completing:**
            ```
            Role: ⚪ UNKNOWN (Starting...)
            Status: 🗳️  ELECTION IN PROGRESS...
            Last Election: ⏳ Waiting for first election...
            ```
            **Action:** Wait 30 seconds. If still stuck, check logs for errors.

            **Heartbeats stopped (on follower):**
            ```
            Heartbeats Received: 245 (not increasing)
            HeartbeatMissed: 2 ⚠️
            ```
            **Action:** Leader may be down. Re-election should trigger automatically.

            ### ❌ Critical Issues

            **Multiple errors:**
            ```
            Errors: ❌ 15 (investigate!)
            Overall Health: 🔴 POOR
            ```
            **Action:** Check full logs with: `Get-Content $LogFile | Select-String "ERROR"`

            **No heartbeats at all:**
            ```
            Heartbeat Status: ⏳ Not started yet
            ```
            **Action:** Wait for election to complete. If >60 seconds, check election logs.

            ---

            ## 🧪 Testing Scenarios

            ### Test 1: Normal Startup
            ```powershell
            # Start monitoring
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node.log"

            # Expected progression:
            # 0-10s:  Status: Starting, Role: Unknown
            # 10-30s: Status: Election in Progress
            # 30s+:   Status: Operational, Role: Coordinator or Follower
            ```

            ### Test 2: Leader Failure Detection
            ```powershell
            # Terminal 1: Monitor follower
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node1.log"

            # Terminal 2: Kill leader process
            Stop-Process -Name optimusdb -Force

            # Watch Terminal 1:
            # You should see:
            # - HeartbeatMissed: 1, 2, 3
            # - Status changes to "Election in Progress"
            # - New leader elected
            # - Node becomes Coordinator or stays Follower
            ```

            ### Test 3: Compare Multiple Nodes
            ```powershell
            # Open 3 PowerShell windows
            # Window 1:
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node1.log"

            # Window 2:
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node2.log"

            # Window 3:
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node3.log"

            # Verify:
            # - Only ONE node shows "Coordinator"
            # - All others show "Follower"
            # - All show same Leader ID
            # - Coordinator sends heartbeats
            # - Followers receive heartbeats
            ```

            ---

            ## 🛠️ Troubleshooting

            ### Script won't run
            ```powershell
            # Enable script execution (run as Administrator)
            Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

            # Then try again
            .\Monitor-OptimusElection.ps1 -LogFile "C:\optimusdb\logs\node.log"
            ```

            ### "File not found" error
            ```powershell
            # Check if file exists
            Test-Path "C:\optimusdb\logs\node.log"

            # If False, find your actual log file location
            # OptimusDB might log to:
            # - Current directory
            # - %APPDATA%\optimusdb\logs
            # - %USERPROFILE%\.cache\optimusdb\logs
            ```

            ### Colors not showing
            ```powershell
            # Update PowerShell to version 5.1 or higher
            $PSVersionTable.PSVersion

            # Or use Simple-Monitor.ps1 which has better compatibility
            .\Simple-Monitor.ps1 -LogFile "C:\optimusdb\logs\node.log"
            ```

            ### Log file is too large
            ```powershell
            # Monitor only recent entries
            Get-Content "C:\optimusdb\logs\node.log" -Tail 1000 | Out-File "recent.log"
            .\Monitor-OptimusElection.ps1 -LogFile "recent.log"
            ```

            ---

            ## 📋 Quick Reference

            ### Common Commands

            ```powershell
            # Basic monitoring
            .\Monitor-OptimusElection.ps1 -LogFile "node.log"

            # Fast refresh
            .\Monitor-OptimusElection.ps1 -LogFile "node.log" -RefreshInterval 1

            # Live stream
            .\Monitor-OptimusElection.ps1 -LogFile "node.log" -Mode Stream

            # Quick check
            .\Monitor-OptimusElection.ps1 -LogFile "node.log" -Mode Quick

            # Simple monitor
            .\Simple-Monitor.ps1 -LogFile "node.log"
            ```

            ### Useful PowerShell Commands

            ```powershell
            # Count errors
            (Get-Content node.log | Select-String "ERROR").Count

            # Find last election
            Get-Content node.log | Select-String "Winner:" | Select-Object -Last 1

            # Check current role
            Get-Content node.log | Select-String "is now" | Select-Object -Last 1

            # Count heartbeats
            (Get-Content node.log | Select-String "Sent heartbeat").Count
            (Get-Content node.log | Select-String "Received heartbeat").Count

            # View recent errors
            Get-Content node.log | Select-String "ERROR" | Select-Object -Last 10

            # Export logs for analysis
            Get-Content node.log | Select-String "ELECTION|ERROR" | Out-File "analysis.txt"
            ```

            ---

            ## 🎓 Tips & Best Practices

            ### 1. Monitor Multiple Nodes
            Run the dashboard in separate PowerShell windows to compare nodes side-by-side.

            ### 2. Use Quick Mode in Scripts
            Integrate the Quick mode into your deployment scripts for automated health checks.

            ### 3. Stream Mode for Debugging
            Use Stream mode when troubleshooting issues to see events in real-time.

            ### 4. Log Rotation
            Archive old logs to keep files manageable:
            ```powershell
            Move-Item node.log "node-$(Get-Date -Format 'yyyyMMdd').log"
            ```

            ### 5. Alert on Errors
            Set up a scheduled task to check for errors:
            ```powershell
            # check-health.ps1
            $errors = (Get-Content "node.log" | Select-String "ERROR").Count
            if ($errors -gt 10) {
            Send-MailMessage -To "admin@example.com" -Subject "OptimusDB Alert" -Body "Error count: $errors"
            }
            ```

            ---

            ## 📊 Example Output Explained

            ### Full Dashboard Example
            ```
            ╔════════════════════════════════════════════════════════════════╗
            ║          OptimusDB Leader Election Monitor                      ║
            ╚════════════════════════════════════════════════════════════════╝

            🔄 Last Updated: 2025-10-28 18:30:45

            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            📊 NODE STATUS
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Role: ★ COORDINATOR (Leader) ★          ← This node is the leader
            Status: ✅ OPERATIONAL                   ← Everything working

            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            🗳️  ELECTION INFO
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Last Election: ✓ Completed              ← Election finished
            Leader ID: 12D3KooWABC...                ← This is the leader's ID
            Votes Received: 2                        ← Won with 2 votes

            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            💓 HEARTBEAT STATUS
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Heartbeats Sent: 245                     ← Leader sent 245 heartbeats
            Status: 🟢 Sending (every 5s)            ← Actively sending
            Last: [HEARTBEAT] Sent heartbeat...      ← Most recent heartbeat

            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            🏥 HEALTH STATUS
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Errors: ✅ None                          ← No errors detected
            Overall Health: 🟢 EXCELLENT             ← All systems go!

            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            📝 RECENT ACTIVITY (Last 5 events)
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            💓 [HEARTBEAT] Sent heartbeat from leader
            💓 [HEARTBEAT] Sent heartbeat from leader
            🗳️  [ELECTION] Winner: 12D3...xyz with 2 votes
            ★  [COORDINATOR] Node is now acting as leader
            🗳️  [ELECTION] Vote received: ...

            Refreshing in 5 seconds... (Ctrl+C to exit)
            ```

            ---

            ## 🎯 Success Checklist

            Use this checklist to verify everything is working:

            - [ ] Script runs without errors
            - [ ] Dashboard displays correctly
            - [ ] Node role is assigned (Coordinator or Follower)
            - [ ] Election completed successfully
            - [ ] Heartbeats are active (sent or received)
            - [ ] Error count is low or zero
            - [ ] Health status shows GREEN
            - [ ] Recent activity shows current events
            - [ ] Can monitor multiple nodes simultaneously
            - [ ] Re-election works after killing leader

            ---

            ## 💡 Advanced Usage

            ### Automated Monitoring
            ```powershell
            # Create a scheduled task to check health every 5 minutes
            $action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
            -Argument "-File C:\scripts\Monitor-OptimusElection.ps1 -LogFile C:\optimusdb\logs\node.log -Mode Quick"

            $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 5)

            Register-ScheduledTask -TaskName "OptimusDB-Health" -Action $action -Trigger $trigger
            ```

            ### Export to CSV
            ```powershell
            # Create a monitoring log
            while ($true) {
            $status = .\Monitor-OptimusElection.ps1 -LogFile "node.log" -Mode Quick
            $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

            "$timestamp,$status" | Out-File "health-log.csv" -Append
            Start-Sleep -Seconds 300  # Every 5 minutes
            }
            ```

            ---

            ## 📞 Need Help?

            If you encounter issues:

            1. **Check log file exists**: `Test-Path "C:\path\to\node.log"`
            2. **Verify PowerShell version**: `$PSVersionTable.PSVersion` (need 5.1+)
            3. **Enable script execution**: `Set-ExecutionPolicy RemoteSigned`
            4. **Try Simple-Monitor first**: More compatible, easier to debug
            5. **Check file permissions**: Ensure you can read the log file

            ---