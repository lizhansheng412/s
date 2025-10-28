# ============================================================================
# PostgreSQL Abstracts Table Migration Script - OPTIMIZED FOR SPEED
# Migrate abstracts table from M3 drive (s2orc_d3) to local database (s2orc_d0)
# 
# Optimization Strategy:
# 1. Separate export: schema -> data -> indexes/constraints
# 2. Import data first, then create indexes (5-10x faster)
# 3. Parallel import and index creation
# ============================================================================

# Set error handling
$ErrorActionPreference = "Stop"

# ============================================================================
# Configuration Parameters
# ============================================================================

# Source database config (M3 drive)
$SOURCE_HOST = "localhost"
$SOURCE_PORT = "5433"
$SOURCE_DB = "s2orc_d3"
$SOURCE_USER = "postgres"
$SOURCE_PASSWORD = "grained"

# Target database config (local)
$TARGET_HOST = "localhost"
$TARGET_PORT = "5430"
$TARGET_DB = "s2orc_d0"
$TARGET_USER = "postgres"
$TARGET_PASSWORD = "grained"

# Export file paths
$DUMP_SCHEMA = "D:\abstracts_schema.sql"
$DUMP_DATA = "D:\abstracts_data.dump"
$DUMP_INDEXES = "D:\abstracts_indexes.sql"
$LOG_FILE = "D:\abstracts_migration.log"

# Performance parameters
$PARALLEL_JOBS = 8
$COMPRESSION_LEVEL = 6

# PostgreSQL bin path
$PG_BIN = "D:\person_data\postgresql\bin"
$script:PG_DUMP_PATH = Join-Path $PG_BIN "pg_dump.exe"
$script:PG_RESTORE_PATH = Join-Path $PG_BIN "pg_restore.exe"
$script:PSQL_PATH = Join-Path $PG_BIN "psql.exe"

# ============================================================================
# Function Definitions
# ============================================================================

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] $Message"
    Write-Host $logMessage -ForegroundColor Cyan
    if (Test-Path $LOG_FILE) {
        Add-Content -Path $LOG_FILE -Value $logMessage
    }
}

function Write-Success-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] SUCCESS: $Message"
    Write-Host $logMessage -ForegroundColor Green
    if (Test-Path $LOG_FILE) {
        Add-Content -Path $LOG_FILE -Value $logMessage
    }
}

function Write-Warning-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] WARNING: $Message"
    Write-Host $logMessage -ForegroundColor Yellow
    if (Test-Path $LOG_FILE) {
        Add-Content -Path $LOG_FILE -Value $logMessage
    }
}

# ============================================================================
# Start Migration
# ============================================================================

Write-Host "============================================================================" -ForegroundColor Yellow
Write-Host "PostgreSQL Abstracts Table Migration - Optimized Version" -ForegroundColor Yellow
Write-Host "============================================================================" -ForegroundColor Yellow
Write-Host ""

# Initialize log
if (Test-Path $LOG_FILE) { Remove-Item $LOG_FILE }
New-Item -Path $LOG_FILE -ItemType File -Force | Out-Null

Write-Log "Migration started"
Write-Log "Source: s2orc_d3 @ port 5433"
Write-Log "Target: s2orc_d0 @ port 5430"
Write-Log "Table: abstracts"

# ============================================================================
# Step 1: Check Source Table
# ============================================================================

Write-Host ""
Write-Host "Step 1/7: Analyze Source Table" -ForegroundColor Yellow
Write-Host "----------------------------------------"

$env:PGPASSWORD = $SOURCE_PASSWORD
Write-Log "Getting table info (fast estimate)..."
$estimateQuery = "SELECT COALESCE(c.reltuples, 0)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'abstracts' AND n.nspname = 'public';"
$sourceRowCountRaw = & $script:PSQL_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c $estimateQuery 2>&1
$sourceRowCount = [long]($sourceRowCountRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })
Write-Log "Source row count (estimate): $sourceRowCount"

$tableSizeRaw = & $script:PSQL_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c "SELECT pg_size_pretty(pg_total_relation_size('abstracts'));" 2>&1
$tableSize = ($tableSizeRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })
Write-Log "Source table size: $tableSize"
Write-Success-Log "Source table analyzed"

# Check target
Write-Log "Checking target database..."
$env:PGPASSWORD = $TARGET_PASSWORD
$targetTableExists = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'abstracts');" 2>&1

if ($targetTableExists.Trim() -eq "t") {
    Write-Warning-Log "abstracts table already exists in target!"
    $choice = Read-Host "Drop existing table and re-import? (y/n)"
    if ($choice -eq "y") {
        & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "DROP TABLE IF EXISTS abstracts CASCADE;" 2>&1 | Out-Null
        Write-Success-Log "Existing table dropped"
    } else {
        Write-Log "Migration cancelled"
        exit 0
    }
}

# ============================================================================
# Step 2: Export Table Structure
# ============================================================================

Write-Host ""
Write-Host "Step 2/7: Export Table Structure" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_SCHEMA) { Remove-Item $DUMP_SCHEMA -Force }

Write-Log "Exporting table structure (no indexes)..."
$env:PGPASSWORD = $SOURCE_PASSWORD
& $script:PG_DUMP_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB `
    -t abstracts --section=pre-data --no-owner --no-privileges -f $DUMP_SCHEMA 2>&1 | Out-Null

# Remove indexes from schema
$schemaContent = Get-Content $DUMP_SCHEMA -Raw -Encoding UTF8
$schemaContent = $schemaContent -replace 'ALTER TABLE ONLY.*ADD CONSTRAINT.*PRIMARY KEY.*;\s*', ''
$schemaContent = $schemaContent -replace 'ALTER TABLE.*ADD CONSTRAINT.*;\s*', ''
$schemaContent = $schemaContent -replace 'CREATE.*INDEX.*ON.*;\s*', ''
Set-Content -Path $DUMP_SCHEMA -Value $schemaContent -NoNewline -Encoding UTF8

Write-Success-Log "Table structure exported"

# ============================================================================
# Step 3: Export Data
# ============================================================================

Write-Host ""
Write-Host "Step 3/7: Export Data (Estimated 5-15 minutes)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_DATA) { Remove-Item $DUMP_DATA -Force }

Write-Log "Exporting data..."
Write-Host "Progress will be shown below:" -ForegroundColor Cyan
Write-Host ""

$env:PGPASSWORD = $SOURCE_PASSWORD
$env:PGCLIENTENCODING = "UTF8"
$exportStart = Get-Date

# Use process control
$pinfo = New-Object System.Diagnostics.ProcessStartInfo
$pinfo.FileName = $script:PG_DUMP_PATH
$pinfo.RedirectStandardError = $true
$pinfo.RedirectStandardOutput = $true
$pinfo.UseShellExecute = $false
$pinfo.Arguments = "-h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t public.abstracts --data-only --no-sync -Fc -Z $COMPRESSION_LEVEL -f $DUMP_DATA"
$pinfo.EnvironmentVariables["PGPASSWORD"] = $SOURCE_PASSWORD
$pinfo.EnvironmentVariables["PGCLIENTENCODING"] = "UTF8"

$p = New-Object System.Diagnostics.Process
$p.StartInfo = $pinfo
$p.Start() | Out-Null
$stdout = $p.StandardOutput.ReadToEnd()
$stderr = $p.StandardError.ReadToEnd()
$p.WaitForExit()

if ($stdout) { $stdout | Add-Content -Path $LOG_FILE }
if ($stderr) { $stderr | Add-Content -Path $LOG_FILE }

$exportEnd = Get-Date
$exportDuration = $exportEnd - $exportStart

if (Test-Path $DUMP_DATA) {
    $dataSize = (Get-Item $DUMP_DATA).Length
    $dataSizeGB = [math]::Round($dataSize / 1GB, 2)
    Write-Host ""
    Write-Success-Log "Data export completed!"
    Write-Log "Duration: $($exportDuration.ToString('hh\:mm\:ss'))"
    Write-Log "File size: ${dataSizeGB} GB"
} else {
    Write-Host "ERROR: Export failed!" -ForegroundColor Red
    exit 1
}

# ============================================================================
# Step 4: Export Indexes
# ============================================================================

Write-Host ""
Write-Host "Step 4/7: Export Indexes" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_INDEXES) { Remove-Item $DUMP_INDEXES -Force }

$env:PGPASSWORD = $SOURCE_PASSWORD
& $script:PG_DUMP_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB `
    -t abstracts --section=post-data --no-owner --no-privileges -f $DUMP_INDEXES 2>&1 | Out-Null

Write-Success-Log "Indexes exported"

# ============================================================================
# Step 5: Create Table Structure
# ============================================================================

Write-Host ""
Write-Host "Step 5/7: Create Table Structure" -ForegroundColor Yellow
Write-Host "----------------------------------------"

$env:PGPASSWORD = $TARGET_PASSWORD
& $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -f $DUMP_SCHEMA 2>&1 | Out-Null

Write-Success-Log "Table structure created"

# ============================================================================
# Step 6: Import Data
# ============================================================================

Write-Host ""
Write-Host "Step 6/7: Import Data (Estimated 5-15 minutes)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

Write-Log "Importing data with $PARALLEL_JOBS parallel workers..."
Write-Host "Progress will be shown below:" -ForegroundColor Cyan
Write-Host ""

$importStart = Get-Date

# Use process control
$pinfo = New-Object System.Diagnostics.ProcessStartInfo
$pinfo.FileName = $script:PG_RESTORE_PATH
$pinfo.RedirectStandardError = $true
$pinfo.RedirectStandardOutput = $true
$pinfo.UseShellExecute = $false
$pinfo.Arguments = "-h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB --data-only --no-owner --no-privileges -j $PARALLEL_JOBS $DUMP_DATA"
$pinfo.EnvironmentVariables["PGPASSWORD"] = $TARGET_PASSWORD
$pinfo.EnvironmentVariables["PGCLIENTENCODING"] = "UTF8"

$p = New-Object System.Diagnostics.Process
$p.StartInfo = $pinfo
$p.Start() | Out-Null
$stdout = $p.StandardOutput.ReadToEnd()
$stderr = $p.StandardError.ReadToEnd()
$p.WaitForExit()

if ($stdout) { $stdout | Add-Content -Path $LOG_FILE }
if ($stderr) { $stderr | Add-Content -Path $LOG_FILE }

$importEnd = Get-Date
$importDuration = $importEnd - $importStart

Write-Host ""
Write-Log "Import completed with exit code: $($p.ExitCode)"
Write-Log "Duration: $($importDuration.ToString('hh\:mm\:ss'))"

# Quick verification
$env:PGPASSWORD = $TARGET_PASSWORD
$quickCheckRaw = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT COUNT(*) FROM abstracts LIMIT 1;" 2>&1
$quickCheck = ($quickCheckRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })

if ($quickCheck -and $quickCheck -match "^\d+$" -and [int]$quickCheck -gt 0) {
    Write-Success-Log "Data import successful! (Quick check: $quickCheck rows)"
} else {
    Write-Host "WARNING: Could not verify data import" -ForegroundColor Yellow
}

# ============================================================================
# Step 7: Create Indexes
# ============================================================================

Write-Host ""
Write-Host "Step 7/7: Create Indexes (Estimated 5-15 minutes)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

Write-Log "Creating primary key and indexes..."
$indexStart = Get-Date

$env:PGPASSWORD = $TARGET_PASSWORD
$indexOutput = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -f $DUMP_INDEXES 2>&1

$indexOutput | Add-Content -Path $LOG_FILE
$indexOutput | ForEach-Object {
    $line = $_.ToString()
    if ($line -match "ERROR|error|FATAL|fatal") {
        Write-Host $line -ForegroundColor Red
    } elseif ($line -match "CREATE INDEX|ALTER TABLE|PRIMARY KEY") {
        Write-Host $line -ForegroundColor Green
    }
}

$indexEnd = Get-Date
$indexDuration = $indexEnd - $indexStart

Write-Host ""
Write-Success-Log "Indexes created!"
Write-Log "Duration: $($indexDuration.ToString('hh\:mm\:ss'))"

# ANALYZE
Write-Log "Running ANALYZE..."
& $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "ANALYZE abstracts;" 2>&1 | Out-Null
Write-Success-Log "ANALYZE completed"

# ============================================================================
# Final Verification
# ============================================================================

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Green
Write-Host "FINAL VERIFICATION" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Green
Write-Host ""

$env:PGPASSWORD = $TARGET_PASSWORD

# Row count
Write-Host "1. Row Count:" -ForegroundColor Cyan
$finalCountRaw = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT COUNT(*) FROM abstracts;" 2>&1
$finalCount = ($finalCountRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })
Write-Host "   Target: $finalCount rows" -ForegroundColor White

# Table size
Write-Host ""
Write-Host "2. Table Size:" -ForegroundColor Cyan
$finalSizeRaw = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT pg_size_pretty(pg_total_relation_size('abstracts'));" 2>&1
$finalSize = ($finalSizeRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })
Write-Host "   Total: $finalSize" -ForegroundColor White

# Indexes
Write-Host ""
Write-Host "3. Indexes:" -ForegroundColor Cyan
& $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "SELECT indexname FROM pg_indexes WHERE tablename = 'abstracts';"

# ============================================================================
# Summary
# ============================================================================

$totalDuration = $importEnd - $exportStart

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Green
Write-Host "MIGRATION COMPLETED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Summary:" -ForegroundColor Cyan
Write-Host "  Table: abstracts" -ForegroundColor White
Write-Host "  Source: s2orc_d3 (port 5433)" -ForegroundColor White
Write-Host "  Target: s2orc_d0 (port 5430)" -ForegroundColor White
Write-Host "  Rows: $finalCount" -ForegroundColor White
Write-Host "  Size: $finalSize" -ForegroundColor White
Write-Host ""
Write-Host "Performance:" -ForegroundColor Cyan
Write-Host "  Export: $($exportDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  Import: $($importDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  Indexes: $($indexDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  Total: $($totalDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host ""
Write-Host "Log file: $LOG_FILE" -ForegroundColor Cyan
Write-Host ""

# Cleanup
$cleanup = Read-Host "Delete temporary files (${dataSizeGB} GB)? (y/n)"
if ($cleanup -eq "y") {
    if (Test-Path $DUMP_SCHEMA) { Remove-Item $DUMP_SCHEMA -Force }
    if (Test-Path $DUMP_DATA) { Remove-Item $DUMP_DATA -Force }
    if (Test-Path $DUMP_INDEXES) { Remove-Item $DUMP_INDEXES -Force }
    Write-Success-Log "Temporary files deleted"
}

Write-Host ""
Write-Host "[SUCCESS] abstracts table migration complete!" -ForegroundColor Green
Write-Log "Migration completed successfully"

