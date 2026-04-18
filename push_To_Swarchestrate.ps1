# ============================================
# Git Push Script - OptimusDB (Swarmchestrate)
# ============================================

$EXPECTED_DIR    = "C:\Users\georg\GolandProjects\optimusdb-lsa"
$REMOTE_NAME     = "origin"
$REMOTE_URL      = "https://github.com/Swarmchestrate/optimusdb.git"

Write-Host "🚀 OptimusDB - Swarmchestrate Git Push Script" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# ── Directory check ──────────────────────────────────────────────────────────
$currentDir = Get-Location
Write-Host "📁 Current directory: $currentDir" -ForegroundColor Yellow

if ($currentDir.Path -ne $EXPECTED_DIR) {
    Write-Host "⚠️  Not in the expected project directory!" -ForegroundColor Yellow
    Write-Host "   Expected: $EXPECTED_DIR" -ForegroundColor White
    Write-Host "   Current:  $currentDir" -ForegroundColor White
    Write-Host ""

    $changeDir = Read-Host "Change to the correct directory? (y/n)"
    if ($changeDir -eq "y") {
        if (Test-Path $EXPECTED_DIR) {
            Set-Location $EXPECTED_DIR
            Write-Host "✅ Changed to: $EXPECTED_DIR" -ForegroundColor Green
            Write-Host ""
        } else {
            Write-Host "❌ Directory does not exist: $EXPECTED_DIR" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "❌ Aborted. Please run this script from: $EXPECTED_DIR" -ForegroundColor Red
        exit 1
    }
}

Write-Host "✅ Running from correct directory: $EXPECTED_DIR" -ForegroundColor Green
Write-Host ""

# ── Prerequisites ─────────────────────────────────────────────────────────────
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Host "❌ Git is not installed or not in PATH" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path .git)) {
    Write-Host "❌ Not a git repository. Run 'git init' first." -ForegroundColor Red
    exit 1
}

# ── Remote check / auto-configure ────────────────────────────────────────────
$remotes = git remote
if ([string]::IsNullOrWhiteSpace($remotes)) {
    Write-Host "⚠️  No remote repository configured." -ForegroundColor Yellow
    $addRemote = Read-Host "Add '$REMOTE_NAME' pointing to $REMOTE_URL ? (y/n)"
    if ($addRemote -eq "y") {
        git remote add $REMOTE_NAME $REMOTE_URL
        Write-Host "✅ Remote '$REMOTE_NAME' added." -ForegroundColor Green
        Write-Host ""
    } else {
        Write-Host "❌ Cannot push without a remote. Aborting." -ForegroundColor Red
        exit 1
    }
} else {
    # Verify the remote URL matches expectation
    $configuredUrl = git remote get-url $REMOTE_NAME 2>$null
    if ($configuredUrl -ne $REMOTE_URL) {
        Write-Host "⚠️  Remote '$REMOTE_NAME' URL mismatch:" -ForegroundColor Yellow
        Write-Host "   Configured : $configuredUrl" -ForegroundColor White
        Write-Host "   Expected   : $REMOTE_URL" -ForegroundColor White
        Write-Host ""
        $fixRemote = Read-Host "Update remote URL to expected value? (y/n)"
        if ($fixRemote -eq "y") {
            git remote set-url $REMOTE_NAME $REMOTE_URL
            Write-Host "✅ Remote URL updated." -ForegroundColor Green
            Write-Host ""
        }
    }
}

# ── Branch info ───────────────────────────────────────────────────────────────
$currentBranch = git branch --show-current
Write-Host "📍 Current branch: $currentBranch" -ForegroundColor Yellow
Write-Host ""

# ── Status ────────────────────────────────────────────────────────────────────
Write-Host "📊 Git Status:" -ForegroundColor Cyan
git status --short
Write-Host ""

$changes = git status --porcelain
if ([string]::IsNullOrWhiteSpace($changes)) {
    Write-Host "✅ Nothing to commit — working tree clean." -ForegroundColor Green

    # Still offer to push if there are unpushed commits
    $unpushed = git log $REMOTE_NAME/$currentBranch..HEAD --oneline 2>$null
    if (-not [string]::IsNullOrWhiteSpace($unpushed)) {
        Write-Host ""
        Write-Host "📤 Unpushed commits found:" -ForegroundColor Yellow
        Write-Host $unpushed -ForegroundColor White
        Write-Host ""
        $pushAnyway = Read-Host "Push them now? (y/n)"
        if ($pushAnyway -eq "y") {
            goto PushStep
        }
    }
    exit 0
}

# ── Stage changes ─────────────────────────────────────────────────────────────
$confirm = Read-Host "Stage all changes? (y/n)"
if ($confirm -ne "y") {
    Write-Host "❌ Aborted by user." -ForegroundColor Red
    exit 0
}

Write-Host "📦 Staging all changes..." -ForegroundColor Cyan
git add .

Write-Host ""
Write-Host "📋 Files to be committed:" -ForegroundColor Cyan
git diff --cached --name-status
Write-Host ""

# ── Commit ────────────────────────────────────────────────────────────────────
$commitMessage = Read-Host "Enter commit message"
if ([string]::IsNullOrWhiteSpace($commitMessage)) {
    Write-Host "❌ Commit message cannot be empty." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "💾 Committing changes..." -ForegroundColor Cyan
git commit -m "$commitMessage"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Commit failed." -ForegroundColor Red
    exit 1
}

# ── Push ──────────────────────────────────────────────────────────────────────
Write-Host ""
$pushConfirm = Read-Host "Push to $REMOTE_URL ? (y/n)"
if ($pushConfirm -ne "y") {
    Write-Host "✅ Committed locally. Run 'git push' manually when ready." -ForegroundColor Green
    exit 0
}

:PushStep

# Check whether the branch already tracks a remote
$trackingBranch = git rev-parse --abbrev-ref --symbolic-full-name "@{u}" 2>$null

Write-Host ""
Write-Host "🚀 Pushing to $REMOTE_NAME/$currentBranch ..." -ForegroundColor Cyan

if ([string]::IsNullOrWhiteSpace($trackingBranch)) {
    # First push — set upstream
    Write-Host "   (No upstream set — using --set-upstream)" -ForegroundColor DarkGray
    git push --set-upstream $REMOTE_NAME $currentBranch
} else {
    git push $REMOTE_NAME $currentBranch
}

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "✅ Successfully pushed to GitHub!" -ForegroundColor Green
    Write-Host "🎉 https://github.com/Swarmchestrate/optimusdb" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "❌ Push failed." -ForegroundColor Red
    Write-Host ""
    Write-Host "Common fixes:" -ForegroundColor Yellow
    Write-Host "  • Check credentials / PAT token scope (needs 'repo' access)" -ForegroundColor White
    Write-Host "  • Remote may have newer commits — run 'git pull --rebase' first" -ForegroundColor White
    Write-Host "  • Confirm you have write access to Swarmchestrate/optimusdb" -ForegroundColor White
    exit 1
}