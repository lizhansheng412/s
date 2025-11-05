# ============================================================================
# Cleanup Migration Temporary Files
# Clean up all temporary files created during table migrations
# ============================================================================

param(
    [Parameter(Mandatory=$false)]
    [switch]$DeleteAll,
    
    [Parameter(Mandatory=$false)]
    [switch]$DeleteLogs,
    
    [Parameter(Mandatory=$false)]
    [switch]$DryRun
)

Write-Host "============================================================================" -ForegroundColor Yellow
Write-Host "Migration Files Cleanup Tool" -ForegroundColor Yellow
Write-Host "============================================================================" -ForegroundColor Yellow
Write-Host ""

# Define temporary file patterns
$tempPatterns = @(
    "*_schema.sql",
    "*_schema.dump", 
    "*_data.dump",
    "*_indexes.sql",
    "*_indexes.dump",
    "*_migration.log",
    "*_migration_*.log",
    "*_migration",
    "*_migration_*",
    "uuid_mapping",
    "pg_migration_temp"
)

$filesToDelete = @()
$filePathsAdded = @{}  # Track added files to avoid duplicates
$totalSize = 0

# Scan for files in D drive
Write-Host "Scanning for temporary files on D:\ ..." -ForegroundColor Cyan
Write-Host ""

foreach ($pattern in $tempPatterns) {
    $files = Get-ChildItem -Path "D:\$pattern" -ErrorAction SilentlyContinue
    foreach ($file in $files) {
        # Skip if already added (avoid duplicates)
        if ($filePathsAdded.ContainsKey($file.FullName)) {
            continue
        }
        
        # Skip log files if not deleting logs
        if (-not $DeleteLogs -and ($file.Name -like "*.log" -or $file.Extension -eq ".log")) {
            continue
        }
        
        # Add to list
        $filesToDelete += $file
        $filePathsAdded[$file.FullName] = $true
        
        if (-not $file.PSIsContainer) {
            $totalSize += $file.Length
        }
    }
}

if ($filesToDelete.Count -eq 0) {
    Write-Host "No temporary files found." -ForegroundColor Green
    exit 0
}

# Display files found
Write-Host "Found $($filesToDelete.Count) temporary file(s):" -ForegroundColor Yellow
Write-Host ""

foreach ($file in $filesToDelete) {
    if ($file.PSIsContainer) {
        $itemType = "[DIR]"
        $size = "-"
    }
    else {
        $itemType = "[FILE]"
        $sizeGB = [math]::Round($file.Length / 1GB, 2)
        $sizeMB = [math]::Round($file.Length / 1MB, 2)
        if ($sizeGB -gt 0.1) {
            $size = "${sizeGB} GB"
        }
        else {
            $size = "${sizeMB} MB"
        }
    }
    
    Write-Host "  $itemType $($file.FullName) - $size" -ForegroundColor Gray
}

$totalSizeGB = [math]::Round($totalSize / 1GB, 2)
$totalSizeMB = [math]::Round($totalSize / 1MB, 2)

Write-Host ""
if ($totalSizeGB -gt 0.1) {
    Write-Host "Total size: $totalSizeGB GB" -ForegroundColor Cyan
}
else {
    Write-Host "Total size: $totalSizeMB MB" -ForegroundColor Cyan
}
Write-Host ""

# Dry run mode
if ($DryRun) {
    Write-Host "[DRY RUN] No files will be deleted" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "To actually delete these files, run:" -ForegroundColor Yellow
    Write-Host "  .\cleanup_migration_files.ps1 -DeleteAll" -ForegroundColor White
    exit 0
}

# Confirm deletion
if (-not $DeleteAll) {
    Write-Host "WARNING: This will permanently delete all listed files!" -ForegroundColor Red
    Write-Host ""
    $confirm = Read-Host "Are you sure you want to delete these files? (yes/no)"
    
    if ($confirm -ne "yes") {
        Write-Host "Cleanup cancelled." -ForegroundColor Yellow
        exit 0
    }
}

# Delete files
Write-Host ""
Write-Host "Deleting files..." -ForegroundColor Yellow

$deletedCount = 0
$failedCount = 0

foreach ($file in $filesToDelete) {
    try {
        # Check if file/directory still exists before attempting to delete
        if (-not (Test-Path $file.FullName)) {
            Write-Host "[SKIP] Already deleted: $($file.Name)" -ForegroundColor Gray
            $deletedCount++
            continue
        }
        
        if ($file.PSIsContainer) {
            Remove-Item -Path $file.FullName -Recurse -Force -ErrorAction Stop
            Write-Host "[OK] Deleted directory: $($file.Name)" -ForegroundColor Green
        }
        else {
            Remove-Item -Path $file.FullName -Force -ErrorAction Stop
            Write-Host "[OK] Deleted: $($file.Name)" -ForegroundColor Green
        }
        $deletedCount++
    }
    catch {
        Write-Host "[FAILED] Could not delete: $($file.Name) - $_" -ForegroundColor Red
        $failedCount++
    }
}

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Green
Write-Host "Cleanup completed!" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Deleted: $deletedCount file(s)" -ForegroundColor White
Write-Host "Failed: $failedCount file(s)" -ForegroundColor White
if ($totalSizeGB -gt 0.1) {
    Write-Host "Space freed: ~$totalSizeGB GB" -ForegroundColor White
}
else {
    Write-Host "Space freed: ~$totalSizeMB MB" -ForegroundColor White
}
Write-Host ""

