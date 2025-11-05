# ============================================================================
# PostgreSQL Table Migration Script - Universal Version
# Migrate any table from source database (s2orc_d3) to target database (s2orc_d0)
# 
# Usage:
#   .\migrate_table_universal.ps1 -TableName "authors"
#   .\migrate_table_universal.ps1 -TableName "abstracts"
#   .\migrate_table_universal.ps1 -TableName "papers"
#   .\migrate_table_universal.ps1 -TableName "citations"
#   .\migrate_table_universal.ps1 -TableName "publication_venues"
#   .\migrate_table_universal.ps1 -TableName "tldrs"
#
# Features:
# - Optimized for speed: data first, indexes later (5-10x faster)
# - Parallel import and index creation
# - Automatic PostgreSQL tools detection
# - Full verification and error handling
# ============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$TableName,
    
    [Parameter(Mandatory=$false)]
    [int]$ParallelJobs = 8,
    
    [Parameter(Mandatory=$false)]
    [int]$CompressionLevel = 6
)

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

# Export file paths (using D: drive for temporary storage)
$EXPORT_DIR = "D:\pg_migration_temp"
$DUMP_SCHEMA = Join-Path $EXPORT_DIR "${TableName}_schema.sql"
$DUMP_DATA = Join-Path $EXPORT_DIR "${TableName}_data.dump"
$DUMP_INDEXES = Join-Path $EXPORT_DIR "${TableName}_indexes.sql"
$LOG_FILE = Join-Path $EXPORT_DIR "${TableName}_migration.log"

# Create export directory if not exists
if (-not (Test-Path $EXPORT_DIR)) {
    New-Item -Path $EXPORT_DIR -ItemType Directory -Force | Out-Null
}

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

function Write-Error-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] ERROR: $Message"
    Write-Host $logMessage -ForegroundColor Red
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

function Find-PostgreSQLPath {
    Write-Log "Searching for PostgreSQL installation..."
    
    # Common PostgreSQL installation paths
    $possiblePaths = @(
        "D:\person_data\postgresql\bin",
        "C:\Program Files\PostgreSQL\18\bin",
        "C:\Program Files\PostgreSQL\17\bin",
        "C:\Program Files\PostgreSQL\16\bin",
        "C:\Program Files\PostgreSQL\15\bin",
        "C:\Program Files\PostgreSQL\14\bin"
    )
    
    # Check each possible path
    foreach ($path in $possiblePaths) {
        if (Test-Path "$path\pg_dump.exe") {
            Write-Success-Log "Found PostgreSQL at: $path"
            return $path
        }
    }
    
    # Try to find via Windows Registry
    try {
        $regKeys = Get-ChildItem "HKLM:\SOFTWARE\PostgreSQL\Installations" -ErrorAction SilentlyContinue
        if ($regKeys) {
            foreach ($key in $regKeys) {
                $binPath = (Get-ItemProperty $key.PSPath).Path
                if ($binPath -and (Test-Path "$binPath\bin\pg_dump.exe")) {
                    $fullPath = "$binPath\bin"
                    Write-Success-Log "Found PostgreSQL via registry at: $fullPath"
                    return $fullPath
                }
            }
        }
    }
    catch {
        Write-Warning-Log "Could not search registry: $_"
    }
    
    return $null
}

function Test-DatabaseConnection {
    param(
        [string]$Hostname,
        [string]$Port,
        [string]$Database,
        [string]$User,
        [string]$Password
    )
    
    try {
        $env:PGPASSWORD = $Password
        $result = & $script:PSQL_PATH -h $Hostname -p $Port -U $User -d $Database -c "SELECT 1;" 2>&1
        return ($LASTEXITCODE -eq 0)
    }
    catch {
        return $false
    }
}

function Get-TableRowCountEstimate {
    param(
        [string]$Hostname,
        [string]$Port,
        [string]$Database,
        [string]$User,
        [string]$Password,
        [string]$Table
    )
    
    try {
        $env:PGPASSWORD = $Password
        $query = "SELECT COALESCE(c.reltuples, 0)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = '$Table' AND n.nspname = 'public';"
        $result = & $script:PSQL_PATH -h $Hostname -p $Port -U $User -d $Database -t -c $query 2>&1
        if ($LASTEXITCODE -eq 0) {
            return [long]($result | Select-Object -First 1 | ForEach-Object { $_.Trim() })
        }
        return -1
    }
    catch {
        return -1
    }
}

function Get-TableRowCountExact {
    param(
        [string]$Hostname,
        [string]$Port,
        [string]$Database,
        [string]$User,
        [string]$Password,
        [string]$Table
    )
    
    try {
        $env:PGPASSWORD = $Password
        $result = & $script:PSQL_PATH -h $Hostname -p $Port -U $User -d $Database -t -c "SELECT COUNT(*) FROM $Table;" 2>&1
        if ($LASTEXITCODE -eq 0) {
            return [long]($result | Select-Object -First 1 | ForEach-Object { $_.Trim() })
        }
        return -1
    }
    catch {
        return -1
    }
}

function Check-DiskSpace {
    param([string]$Path)
    
    $drive = (Get-Item $Path).PSDrive.Name
    $disk = Get-PSDrive -Name $drive
    $freeSpaceGB = [math]::Round($disk.Free / 1GB, 2)
    return $freeSpaceGB
}

# ============================================================================
# Start Migration Process
# ============================================================================

Write-Host "============================================================================" -ForegroundColor Yellow
Write-Host "PostgreSQL Table Migration - Universal Version" -ForegroundColor Yellow
Write-Host "============================================================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "Target Table: $TableName" -ForegroundColor Cyan
Write-Host "Parallel Jobs: $ParallelJobs" -ForegroundColor Cyan
Write-Host "Compression: Level $CompressionLevel" -ForegroundColor Cyan
Write-Host ""

# Initialize log file
if (Test-Path $LOG_FILE) { Remove-Item $LOG_FILE }
New-Item -Path $LOG_FILE -ItemType File -Force | Out-Null

Write-Log "Migration started for table: $TableName"
Write-Log "Source: $SOURCE_DB @ ${SOURCE_HOST}:${SOURCE_PORT}"
Write-Log "Target: $TARGET_DB @ ${TARGET_HOST}:${TARGET_PORT}"

# ============================================================================
# Step 1: Locate PostgreSQL Tools
# ============================================================================

Write-Host "Step 1/9: Locate PostgreSQL Tools" -ForegroundColor Yellow
Write-Host "----------------------------------------"

$pgBinPath = Find-PostgreSQLPath

if (-not $pgBinPath) {
    Write-Error-Log "PostgreSQL installation not found!"
    Write-Host ""
    Write-Host "Please manually specify PostgreSQL bin path:" -ForegroundColor Yellow
    $manualPath = Read-Host "Enter PostgreSQL bin path (or press Enter to exit)"
    
    if ($manualPath -and (Test-Path "$manualPath\pg_dump.exe")) {
        $pgBinPath = $manualPath
        Write-Success-Log "Using manually specified path: $pgBinPath"
    }
    else {
        Write-Error-Log "Cannot proceed without PostgreSQL tools"
        exit 1
    }
}

# Set tool paths
$script:PG_DUMP_PATH = Join-Path $pgBinPath "pg_dump.exe"
$script:PG_RESTORE_PATH = Join-Path $pgBinPath "pg_restore.exe"
$script:PSQL_PATH = Join-Path $pgBinPath "psql.exe"

Write-Success-Log "PostgreSQL tools located"
$pgVersion = & $script:PG_DUMP_PATH --version
Write-Log "Version: $pgVersion"

# ============================================================================
# Step 2: Check Disk Space
# ============================================================================

Write-Host ""
Write-Host "Step 2/9: Check Disk Space" -ForegroundColor Yellow
Write-Host "----------------------------------------"

$freeSpace = Check-DiskSpace -Path "D:\"
Write-Log "D: drive free space: ${freeSpace} GB"

if ($freeSpace -lt 30) {
    Write-Warning-Log "Low disk space! Recommend at least 30GB free"
    $continue = Read-Host "Continue anyway? (y/n)"
    if ($continue -ne "y") {
        exit 0
    }
}

# ============================================================================
# Step 3: Test Database Connections
# ============================================================================

Write-Host ""
Write-Host "Step 3/9: Test Database Connections" -ForegroundColor Yellow
Write-Host "----------------------------------------"

Write-Log "Testing source database..."
if (-not (Test-DatabaseConnection -Hostname $SOURCE_HOST -Port $SOURCE_PORT -Database $SOURCE_DB -User $SOURCE_USER -Password $SOURCE_PASSWORD)) {
    Write-Error-Log "Cannot connect to source database!"
    exit 1
}
Write-Success-Log "Source database connection OK"

Write-Log "Testing target database..."
if (-not (Test-DatabaseConnection -Hostname $TARGET_HOST -Port $TARGET_PORT -Database $TARGET_DB -User $TARGET_USER -Password $TARGET_PASSWORD)) {
    Write-Error-Log "Cannot connect to target database!"
    exit 1
}
Write-Success-Log "Target database connection OK"

# ============================================================================
# Step 4: Analyze Source Table
# ============================================================================

Write-Host ""
Write-Host "Step 4/9: Analyze Source Table" -ForegroundColor Yellow
Write-Host "----------------------------------------"

$env:PGPASSWORD = $SOURCE_PASSWORD
$tableExists = & $script:PSQL_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '$TableName');" 2>&1

if ($tableExists.Trim() -ne "t") {
    Write-Error-Log "Table '$TableName' not found in source database!"
    exit 1
}

Write-Success-Log "Table '$TableName' found in source database"

# Get source table information
$sourceRowCount = Get-TableRowCountEstimate -Hostname $SOURCE_HOST -Port $SOURCE_PORT -Database $SOURCE_DB -User $SOURCE_USER -Password $SOURCE_PASSWORD -Table $TableName
Write-Log "Source table row count (estimate): $sourceRowCount"

$tableSizeRaw = & $script:PSQL_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c "SELECT pg_size_pretty(pg_total_relation_size('$TableName'));" 2>&1
$tableSize = ($tableSizeRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })
Write-Log "Source table size: $tableSize"

# ============================================================================
# Step 5: Check Target Database
# ============================================================================

Write-Host ""
Write-Host "Step 5/9: Check Target Database" -ForegroundColor Yellow
Write-Host "----------------------------------------"

$env:PGPASSWORD = $TARGET_PASSWORD
$targetTableExists = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '$TableName');" 2>&1

if ($targetTableExists.Trim() -eq "t") {
    Write-Warning-Log "Table '$TableName' already exists in target database!"
    $choice = Read-Host "Drop existing table and re-import? (y/n)"
    if ($choice -eq "y") {
        Write-Log "Dropping existing table..."
        & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "DROP TABLE IF EXISTS $TableName CASCADE;" 2>&1 | Out-Null
        Write-Success-Log "Existing table dropped"
    }
    else {
        Write-Log "Migration cancelled by user"
        exit 0
    }
}

# ============================================================================
# Step 6: Export Table Structure (No Indexes)
# ============================================================================

Write-Host ""
Write-Host "Step 6/9: Export Table Structure" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_SCHEMA) { Remove-Item $DUMP_SCHEMA -Force }

Write-Log "Exporting table structure..."
$env:PGPASSWORD = $SOURCE_PASSWORD

& $script:PG_DUMP_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB `
    -t $TableName `
    --section=pre-data `
    --no-owner `
    --no-privileges `
    -f $DUMP_SCHEMA 2>&1 | Out-Null

if ($LASTEXITCODE -ne 0) {
    Write-Error-Log "Failed to export table structure"
    exit 1
}

# Remove constraints and indexes from schema
$schemaContent = Get-Content $DUMP_SCHEMA -Raw -Encoding UTF8
$schemaContent = $schemaContent -replace 'ALTER TABLE ONLY.*ADD CONSTRAINT.*PRIMARY KEY.*;\s*', ''
$schemaContent = $schemaContent -replace 'ALTER TABLE.*ADD CONSTRAINT.*;\s*', ''
$schemaContent = $schemaContent -replace 'CREATE.*INDEX.*ON.*;\s*', ''
Set-Content -Path $DUMP_SCHEMA -Value $schemaContent -NoNewline -Encoding UTF8

Write-Success-Log "Table structure exported"

# ============================================================================
# Step 7: Export Data
# ============================================================================

Write-Host ""
Write-Host "Step 7/9: Export Data (Est. 10-30 minutes)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_DATA) { Remove-Item $DUMP_DATA -Force }

Write-Log "Exporting data from table: $TableName"
Write-Host ""

$env:PGPASSWORD = $SOURCE_PASSWORD
$env:PGCLIENTENCODING = "UTF8"
$exportStart = Get-Date

$pinfo = New-Object System.Diagnostics.ProcessStartInfo
$pinfo.FileName = $script:PG_DUMP_PATH
$pinfo.RedirectStandardError = $true
$pinfo.RedirectStandardOutput = $true
$pinfo.UseShellExecute = $false
$pinfo.Arguments = "-h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t public.$TableName --data-only --no-sync -Fc -Z $CompressionLevel -f $DUMP_DATA"
$pinfo.EnvironmentVariables["PGPASSWORD"] = $SOURCE_PASSWORD
$pinfo.EnvironmentVariables["PGCLIENTENCODING"] = "UTF8"

$p = New-Object System.Diagnostics.Process
$p.StartInfo = $pinfo
$p.Start() | Out-Null
$stdout = $p.StandardOutput.ReadToEnd()
$stderr = $p.StandardError.ReadToEnd()
$p.WaitForExit()

$exportEnd = Get-Date
$exportDuration = $exportEnd - $exportStart

if (-not (Test-Path $DUMP_DATA)) {
    Write-Error-Log "Export failed - dump file not created"
    if ($stderr) { Write-Host $stderr -ForegroundColor Red }
    exit 1
}

$dataSize = (Get-Item $DUMP_DATA).Length
$dataSizeGB = [math]::Round($dataSize / 1GB, 2)

Write-Host ""
Write-Success-Log "Data export completed!"
Write-Log "Duration: $($exportDuration.ToString('hh\:mm\:ss'))"
Write-Log "File size: ${dataSizeGB} GB"

# ============================================================================
# Step 8: Export Indexes and Constraints
# ============================================================================

Write-Host ""
Write-Host "Step 8/9: Export Indexes and Constraints" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_INDEXES) { Remove-Item $DUMP_INDEXES -Force }

Write-Log "Exporting indexes and constraints..."
$env:PGPASSWORD = $SOURCE_PASSWORD

& $script:PG_DUMP_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB `
    -t $TableName `
    --section=post-data `
    --no-owner `
    --no-privileges `
    -f $DUMP_INDEXES 2>&1 | Out-Null

Write-Success-Log "Indexes and constraints exported"

# ============================================================================
# Step 9: Import to Target Database
# ============================================================================

Write-Host ""
Write-Host "Step 9/9: Import to Target Database" -ForegroundColor Yellow
Write-Host "----------------------------------------"

# 9.1: Create table structure
Write-Log "Creating table structure..."
$env:PGPASSWORD = $TARGET_PASSWORD
& $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -f $DUMP_SCHEMA 2>&1 | Out-Null
Write-Success-Log "Table structure created"

# 9.2: Import data
Write-Host ""
Write-Log "Importing data (est. 15-45 minutes)..."
Write-Log "Using $ParallelJobs parallel workers..."
Write-Host ""

$importStart = Get-Date

$pinfo = New-Object System.Diagnostics.ProcessStartInfo
$pinfo.FileName = $script:PG_RESTORE_PATH
$pinfo.RedirectStandardError = $true
$pinfo.RedirectStandardOutput = $true
$pinfo.UseShellExecute = $false
$pinfo.Arguments = "-h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB --data-only --no-owner --no-privileges -j $ParallelJobs $DUMP_DATA"
$pinfo.EnvironmentVariables["PGPASSWORD"] = $TARGET_PASSWORD

$p = New-Object System.Diagnostics.Process
$p.StartInfo = $pinfo
$p.Start() | Out-Null
$stdout = $p.StandardOutput.ReadToEnd()
$stderr = $p.StandardError.ReadToEnd()
$p.WaitForExit()

$importEnd = Get-Date
$importDuration = $importEnd - $importStart

Write-Host ""
Write-Log "Import completed"
Write-Log "Duration: $($importDuration.ToString('hh\:mm\:ss'))"

# Verify import
$env:PGPASSWORD = $TARGET_PASSWORD
$quickCheckRaw = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT COUNT(*) FROM $TableName LIMIT 1;" 2>&1
$quickCheck = ($quickCheckRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })

if ($quickCheck -and $quickCheck -match "^\d+$" -and [int]$quickCheck -gt 0) {
    Write-Success-Log "Data import successful! (Quick check: $quickCheck rows)"
}
else {
    Write-Error-Log "Data import failed or no data imported"
    exit 1
}

# 9.3: Verify exact row count
Write-Log "Verifying row count..."
$exactSourceCount = Get-TableRowCountExact -Hostname $SOURCE_HOST -Port $SOURCE_PORT -Database $SOURCE_DB -User $SOURCE_USER -Password $SOURCE_PASSWORD -Table $TableName
$importedRowCount = Get-TableRowCountExact -Hostname $TARGET_HOST -Port $TARGET_PORT -Database $TARGET_DB -User $TARGET_USER -Password $TARGET_PASSWORD -Table $TableName

Write-Log "Source rows: $exactSourceCount"
Write-Log "Target rows: $importedRowCount"

if ($importedRowCount -eq $exactSourceCount) {
    Write-Success-Log "Row count matches! Data integrity verified"
}
else {
    Write-Warning-Log "Row count mismatch - please verify manually"
}

# 9.4: Create indexes and constraints
Write-Host ""
Write-Log "Creating indexes and constraints (est. 20-60 minutes)..."
Write-Host ""

$indexStart = Get-Date

$env:PGPASSWORD = $TARGET_PASSWORD
$indexOutput = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -f $DUMP_INDEXES 2>&1

$indexOutput | ForEach-Object {
    $line = $_.ToString()
    if ($line -match "ERROR|error") {
        Write-Host $line -ForegroundColor Red
    }
    elseif ($line -match "CREATE INDEX|ALTER TABLE|PRIMARY KEY") {
        Write-Host $line -ForegroundColor Green
    }
}

$indexEnd = Get-Date
$indexDuration = $indexEnd - $indexStart

Write-Host ""
Write-Success-Log "Indexes created!"
Write-Log "Duration: $($indexDuration.ToString('hh\:mm\:ss'))"

# Run ANALYZE
Write-Log "Running ANALYZE..."
& $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "ANALYZE $TableName;" 2>&1 | Out-Null
Write-Success-Log "ANALYZE completed"

# ============================================================================
# Migration Complete
# ============================================================================

$totalDuration = $importEnd - $exportStart

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Green
Write-Host "MIGRATION COMPLETED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Summary:" -ForegroundColor Cyan
Write-Host "  Table: $TableName" -ForegroundColor White
Write-Host "  Rows migrated: $importedRowCount" -ForegroundColor White
Write-Host "  Source size: $tableSize" -ForegroundColor White
Write-Host ""
Write-Host "Performance:" -ForegroundColor Cyan
Write-Host "  Export: $($exportDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  Import: $($importDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  Indexes: $($indexDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  Total: $($totalDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host ""
Write-Host "Log file: $LOG_FILE" -ForegroundColor Cyan
Write-Host ""

# Cleanup prompt
$cleanup = Read-Host "Delete temporary export files (${dataSizeGB} GB)? (y/n)"
if ($cleanup -eq "y") {
    if (Test-Path $DUMP_SCHEMA) { Remove-Item $DUMP_SCHEMA -Force }
    if (Test-Path $DUMP_DATA) { Remove-Item $DUMP_DATA -Force }
    if (Test-Path $DUMP_INDEXES) { Remove-Item $DUMP_INDEXES -Force }
    Write-Success-Log "Temporary files deleted"
}

Write-Host ""
Write-Host "[SUCCESS] Table '$TableName' migration complete!" -ForegroundColor Green
Write-Log "Migration completed successfully"

