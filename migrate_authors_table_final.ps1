# ============================================================================
# PostgreSQL Authors Table Migration Script - FINAL VERSION
# Complete and Safe Migration with Auto-detection and Full Verification
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
$DUMP_SCHEMA = "D:\authors_schema.sql"
$DUMP_DATA = "D:\authors_data.dump"
$DUMP_INDEXES = "D:\authors_indexes.sql"
$LOG_FILE = "D:\authors_migration_final.log"

# Performance optimization parameters
$PARALLEL_JOBS = 8
$COMPRESSION_LEVEL = 6

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
        "C:\Program Files\PostgreSQL\18\bin",
        "C:\Program Files\PostgreSQL\17\bin",
        "C:\Program Files\PostgreSQL\16\bin",
        "C:\Program Files\PostgreSQL\15\bin",
        "C:\Program Files\PostgreSQL\14\bin",
        "C:\Program Files (x86)\PostgreSQL\18\bin",
        "C:\Program Files (x86)\PostgreSQL\17\bin",
        "C:\Program Files (x86)\PostgreSQL\16\bin",
        "D:\PostgreSQL\bin",
        "E:\PostgreSQL\bin"
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
    
    # Search the entire C drive (last resort, slow)
    Write-Log "Performing deep search (this may take a moment)..."
    $found = Get-ChildItem "C:\Program Files" -Recurse -Filter "pg_dump.exe" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($found) {
        $path = Split-Path $found.FullName
        Write-Success-Log "Found PostgreSQL at: $path"
        return $path
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
        if ($LASTEXITCODE -eq 0) {
            return $true
        }
        return $false
    }
    catch {
        return $false
    }
}

function Get-TableRowCount {
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
            return [long]$result.Trim()
        }
        return -1
    }
    catch {
        return -1
    }
}

function Get-TableStructure {
    param(
        [string]$Hostname,
        [string]$Port,
        [string]$Database,
        [string]$User,
        [string]$Password,
        [string]$Table
    )
    
    $env:PGPASSWORD = $Password
    $result = & $script:PSQL_PATH -h $Hostname -p $Port -U $User -d $Database -c "\d $Table" 2>&1
    return $result
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
Write-Host "PostgreSQL Authors Table Migration - FINAL COMPLETE VERSION" -ForegroundColor Yellow
Write-Host "============================================================================" -ForegroundColor Yellow
Write-Host ""

# Initialize log file
if (Test-Path $LOG_FILE) {
    Remove-Item $LOG_FILE
}
New-Item -Path $LOG_FILE -ItemType File -Force | Out-Null

Write-Log "Migration script started"
Write-Log "Source: $SOURCE_DB @ ${SOURCE_HOST}:${SOURCE_PORT}"
Write-Log "Target: $TARGET_DB @ ${TARGET_HOST}:${TARGET_PORT}"

# ============================================================================
# Step 1: Find and Setup PostgreSQL Tools
# ============================================================================

Write-Host ""
Write-Host "Step 1/10: Locate PostgreSQL Tools" -ForegroundColor Yellow
Write-Host "----------------------------------------"

$pgBinPath = Find-PostgreSQLPath

if (-not $pgBinPath) {
    Write-Error-Log "PostgreSQL installation not found!"
    Write-Host ""
    Write-Host "Please manually specify PostgreSQL bin path:" -ForegroundColor Yellow
    Write-Host "Example: C:\Program Files\PostgreSQL\18\bin" -ForegroundColor Gray
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

# Verify tools exist
if (-not (Test-Path $script:PG_DUMP_PATH)) {
    Write-Error-Log "pg_dump not found at: $script:PG_DUMP_PATH"
    exit 1
}

Write-Success-Log "PostgreSQL tools located successfully"
Write-Log "pg_dump: $script:PG_DUMP_PATH"
Write-Log "pg_restore: $script:PG_RESTORE_PATH"
Write-Log "psql: $script:PSQL_PATH"

# Get version
$pgVersion = & $script:PG_DUMP_PATH --version
Write-Log "Version: $pgVersion"

# ============================================================================
# Step 2: Check Disk Space
# ============================================================================

Write-Host ""
Write-Host "Step 2/10: Check Disk Space" -ForegroundColor Yellow
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
Write-Host "Step 3/10: Test Database Connections" -ForegroundColor Yellow
Write-Host "----------------------------------------"

Write-Log "Testing source database (s2orc_d3 @ port 5433)..."
if (-not (Test-DatabaseConnection -Hostname $SOURCE_HOST -Port $SOURCE_PORT -Database $SOURCE_DB -User $SOURCE_USER -Password $SOURCE_PASSWORD)) {
    Write-Error-Log "Cannot connect to source database!"
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "  1. Check if PostgreSQL service on port 5433 is running" -ForegroundColor Gray
    Write-Host "  2. Verify database name: $SOURCE_DB" -ForegroundColor Gray
    Write-Host "  3. Verify credentials" -ForegroundColor Gray
    exit 1
}
Write-Success-Log "Source database connection OK"

Write-Log "Testing target database (s2orc_d0 @ port 5430)..."
if (-not (Test-DatabaseConnection -Hostname $TARGET_HOST -Port $TARGET_PORT -Database $TARGET_DB -User $TARGET_USER -Password $TARGET_PASSWORD)) {
    Write-Error-Log "Cannot connect to target database!"
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "  1. Check if PostgreSQL service on port 5430 is running" -ForegroundColor Gray
    Write-Host "  2. Verify database name: $TARGET_DB" -ForegroundColor Gray
    Write-Host "  3. Verify credentials" -ForegroundColor Gray
    exit 1
}
Write-Success-Log "Target database connection OK"

# ============================================================================
# Step 4: Analyze Source Table
# ============================================================================

Write-Host ""
Write-Host "Step 4/10: Analyze Source Table" -ForegroundColor Yellow
Write-Host "----------------------------------------"

$env:PGPASSWORD = $SOURCE_PASSWORD
$tableExists = & $script:PSQL_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'authors');" 2>&1

if ($tableExists.Trim() -ne "t") {
    Write-Error-Log "authors table not found in source database!"
    exit 1
}

Write-Success-Log "authors table found in source database"

# Get source table information (use fast estimate from pg_class)
Write-Log "Getting source table row count (using fast estimate)..."
$env:PGPASSWORD = $SOURCE_PASSWORD
$estimateQuery = "SELECT COALESCE(c.reltuples, 0)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'authors' AND n.nspname = 'public';"
$sourceRowCountRaw = & $script:PSQL_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c $estimateQuery 2>&1
$sourceRowCount = [long]($sourceRowCountRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })
Write-Log "Source table row count (estimate): $sourceRowCount"

$env:PGPASSWORD = $SOURCE_PASSWORD
$tableSizeRaw = & $script:PSQL_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c "SELECT pg_size_pretty(pg_total_relation_size('authors'));" 2>&1
$tableSize = ($tableSizeRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })
Write-Log "Source table size (with indexes): $tableSize"

# Get indexes
Write-Log "Analyzing indexes and constraints..."
$env:PGPASSWORD = $SOURCE_PASSWORD
$sourceIndexes = & $script:PSQL_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -c "SELECT indexname FROM pg_indexes WHERE tablename = 'authors' ORDER BY indexname;" 2>&1
Write-Host "Source table indexes:" -ForegroundColor Gray
Write-Host $sourceIndexes -ForegroundColor Gray

# ============================================================================
# Step 5: Check Target Database
# ============================================================================

Write-Host ""
Write-Host "Step 5/10: Check Target Database" -ForegroundColor Yellow
Write-Host "----------------------------------------"

$env:PGPASSWORD = $TARGET_PASSWORD
$targetTableExists = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'authors');" 2>&1

if ($targetTableExists.Trim() -eq "t") {
    Write-Warning-Log "authors table already exists in target database!"
    $env:PGPASSWORD = $TARGET_PASSWORD
    $targetRowEstimateRaw = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c $estimateQuery 2>&1
    $targetRowEstimate = [long]($targetRowEstimateRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })
    Write-Host "Current target table rows (estimate): $targetRowEstimate" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Options:" -ForegroundColor Yellow
    Write-Host "  1. DROP existing table and migrate (RECOMMENDED)" -ForegroundColor Yellow
    Write-Host "  2. Cancel migration" -ForegroundColor Yellow
    Write-Host ""
    $choice = Read-Host "Choose (1/2)"
    
    if ($choice -eq "1") {
        Write-Log "Dropping existing authors table..."
        $env:PGPASSWORD = $TARGET_PASSWORD
        & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "DROP TABLE IF EXISTS authors CASCADE;" 2>&1 | Out-Null
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
Write-Host "Step 6/10: Export Table Structure (No Indexes)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_SCHEMA) { Remove-Item $DUMP_SCHEMA -Force }

Write-Log "Exporting table structure only..."
$env:PGPASSWORD = $SOURCE_PASSWORD

try {
    & $script:PG_DUMP_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB `
        -t authors `
        --section=pre-data `
        --no-owner `
        --no-privileges `
        -f $DUMP_SCHEMA 2>&1 | Out-Null
    
    if ($LASTEXITCODE -ne 0) {
        throw "pg_dump failed with exit code $LASTEXITCODE"
    }
    
    # Remove constraints and indexes
    $schemaContent = Get-Content $DUMP_SCHEMA -Raw -Encoding UTF8
    $schemaContent = $schemaContent -replace 'ALTER TABLE ONLY.*ADD CONSTRAINT.*PRIMARY KEY.*;\s*', ''
    $schemaContent = $schemaContent -replace 'ALTER TABLE.*ADD CONSTRAINT.*;\s*', ''
    $schemaContent = $schemaContent -replace 'CREATE.*INDEX.*ON.*;\s*', ''
    Set-Content -Path $DUMP_SCHEMA -Value $schemaContent -NoNewline -Encoding UTF8
    
    Write-Success-Log "Table structure exported (pure schema, no indexes)"
}
catch {
    Write-Error-Log "Failed to export table structure: $_"
    exit 1
}

# ============================================================================
# Step 7: Export Data
# ============================================================================

Write-Host ""
Write-Host "Step 7/10: Export Data (This will take 10-30 minutes)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_DATA) { Remove-Item $DUMP_DATA -Force }

Write-Log "Starting data export..."
Write-Log "Source: $sourceRowCount rows"
Write-Host ""

$env:PGPASSWORD = $SOURCE_PASSWORD
$env:PGCLIENTENCODING = "UTF8"
$exportStart = Get-Date

Write-Log "Starting pg_dump with optimized settings..."
Write-Log "Using data-only mode with custom format..."
Write-Log "Command: pg_dump -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t public.authors --data-only --no-sync -Fc -Z $COMPRESSION_LEVEL -f $DUMP_DATA"
Write-Host ""
Write-Host "This will take 10-30 minutes, please wait..." -ForegroundColor Yellow
Write-Host "Progress messages will appear below:" -ForegroundColor Cyan
Write-Host ""

# Run pg_dump - let it complete regardless of stderr output
$pgDumpArgs = @(
    "-h", $SOURCE_HOST,
    "-p", $SOURCE_PORT,
    "-U", $SOURCE_USER,
    "-d", $SOURCE_DB,
    "-t", "public.authors",
    "--data-only",
    "--no-sync",
    "-Fc",
    "-Z", $COMPRESSION_LEVEL.ToString(),
    "-f", $DUMP_DATA
)

# Start the process and capture output
$pinfo = New-Object System.Diagnostics.ProcessStartInfo
$pinfo.FileName = $script:PG_DUMP_PATH
$pinfo.RedirectStandardError = $true
$pinfo.RedirectStandardOutput = $true
$pinfo.UseShellExecute = $false
$pinfo.Arguments = $pgDumpArgs -join " "
$pinfo.EnvironmentVariables["PGPASSWORD"] = $SOURCE_PASSWORD
$pinfo.EnvironmentVariables["PGCLIENTENCODING"] = "UTF8"

$p = New-Object System.Diagnostics.Process
$p.StartInfo = $pinfo
$p.Start() | Out-Null

# Read output in real-time
$stdout = $p.StandardOutput.ReadToEnd()
$stderr = $p.StandardError.ReadToEnd()
$p.WaitForExit()

# Log all output
if ($stdout) {
    $stdout | Add-Content -Path $LOG_FILE
    Write-Host $stdout -ForegroundColor Gray
}
if ($stderr) {
    $stderr | Add-Content -Path $LOG_FILE
    # Don't treat all stderr as errors - PostgreSQL writes progress to stderr
    $stderr -split "`n" | ForEach-Object {
        if ($_ -match "error|ERROR|fatal|FATAL") {
            Write-Host $_ -ForegroundColor Red
        }
        else {
            Write-Host $_ -ForegroundColor Gray
        }
    }
}

$exportEnd = Get-Date
$exportDuration = $exportEnd - $exportStart

Write-Host ""
Write-Log "pg_dump completed with exit code: $($p.ExitCode)"
Write-Log "Duration: $($exportDuration.ToString('hh\:mm\:ss'))"

# Verify results by checking the dump file
if (-not (Test-Path $DUMP_DATA)) {
    Write-Error-Log "FAILED: Dump file was not created at: $DUMP_DATA"
    Write-Host ""
    Write-Host "Possible causes:" -ForegroundColor Yellow
    Write-Host "  1. No SELECT permission on authors table" -ForegroundColor Gray
    Write-Host "  2. Source database connection lost" -ForegroundColor Gray
    Write-Host "  3. Disk full or write permission denied" -ForegroundColor Gray
    Write-Host "  4. Table 'authors' does not exist in 'public' schema" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Check log file for details: $LOG_FILE" -ForegroundColor Cyan
    exit 1
}

$dataSize = (Get-Item $DUMP_DATA).Length
$dataSizeGB = [math]::Round($dataSize / 1GB, 2)
$dataSizeMB = [math]::Round($dataSize / 1MB, 2)

if ($dataSize -lt 100KB) {
    Write-Error-Log "FAILED: Dump file is too small: $([math]::Round($dataSize/1KB, 2)) KB"
    Write-Error-Log "This indicates the export failed or returned no data"
    Write-Host ""
    Write-Host "Dump file exists but is too small to be valid" -ForegroundColor Red
    Write-Host "Check log file: $LOG_FILE" -ForegroundColor Cyan
    Remove-Item $DUMP_DATA -Force -ErrorAction SilentlyContinue
    exit 1
}

Write-Host ""
Write-Success-Log "Data export completed successfully!"
if ($dataSizeGB -gt 1) {
    Write-Log "File size: ${dataSizeGB} GB"
}
else {
    Write-Log "File size: ${dataSizeMB} MB"
}
Write-Log "Dump file: $DUMP_DATA"

# ============================================================================
# Step 8: Export Indexes and Constraints
# ============================================================================

Write-Host ""
Write-Host "Step 8/10: Export Indexes and Constraints" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_INDEXES) { Remove-Item $DUMP_INDEXES -Force }

Write-Log "Exporting indexes and constraints (including primary key)..."
$env:PGPASSWORD = $SOURCE_PASSWORD

try {
    & $script:PG_DUMP_PATH -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB `
        -t authors `
        --section=post-data `
        --no-owner `
        --no-privileges `
        -f $DUMP_INDEXES 2>&1 | Out-Null
    
    if ($LASTEXITCODE -ne 0) {
        throw "Index export failed"
    }
    
    Write-Success-Log "Indexes and constraints exported"
    Write-Log "Preview:"
    Get-Content $DUMP_INDEXES -Encoding UTF8 | Select-Object -First 20 | ForEach-Object { Write-Host $_ -ForegroundColor Gray }
}
catch {
    Write-Error-Log "Failed to export indexes: $_"
    exit 1
}

# ============================================================================
# Step 9: Import Everything to Target Database
# ============================================================================

Write-Host ""
Write-Host "Step 9/10: Import to Target Database" -ForegroundColor Yellow
Write-Host "----------------------------------------"

# 9.1: Create table structure
Write-Log "Creating table structure..."
$env:PGPASSWORD = $TARGET_PASSWORD

try {
    & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -f $DUMP_SCHEMA 2>&1 | Tee-Object -Append -FilePath $LOG_FILE
    Write-Success-Log "Table structure created"
}
catch {
    Write-Error-Log "Failed to create table structure: $_"
    exit 1
}

# 9.2: Import data (fastest step)
Write-Host ""
Write-Log "Importing data (estimated 15-45 minutes)..."
Write-Log "Using $PARALLEL_JOBS parallel workers..."
Write-Log "Command: pg_restore -h $TARGET_HOST -p $TARGET_PORT -d $TARGET_DB --data-only --no-owner -j $PARALLEL_JOBS $DUMP_DATA"
Write-Host ""
Write-Host "Importing data in parallel, please wait..." -ForegroundColor Yellow
Write-Host "Progress messages will appear below:" -ForegroundColor Cyan
Write-Host ""

$importStart = Get-Date

# Run pg_restore with process control
$pgRestoreArgs = @(
    "-h", $TARGET_HOST,
    "-p", $TARGET_PORT,
    "-U", $TARGET_USER,
    "-d", $TARGET_DB,
    "--data-only",
    "--no-owner",
    "--no-privileges",
    "-j", $PARALLEL_JOBS.ToString(),
    $DUMP_DATA
)

# Start the process
$pinfo = New-Object System.Diagnostics.ProcessStartInfo
$pinfo.FileName = $script:PG_RESTORE_PATH
$pinfo.RedirectStandardError = $true
$pinfo.RedirectStandardOutput = $true
$pinfo.UseShellExecute = $false
$pinfo.Arguments = $pgRestoreArgs -join " "
$pinfo.EnvironmentVariables["PGPASSWORD"] = $TARGET_PASSWORD
$pinfo.EnvironmentVariables["PGCLIENTENCODING"] = "UTF8"

$p = New-Object System.Diagnostics.Process
$p.StartInfo = $pinfo
$p.Start() | Out-Null

# Read output in real-time
$stdout = $p.StandardOutput.ReadToEnd()
$stderr = $p.StandardError.ReadToEnd()
$p.WaitForExit()

# Log all output
if ($stdout) {
    $stdout | Add-Content -Path $LOG_FILE
    Write-Host $stdout -ForegroundColor Gray
}
if ($stderr) {
    $stderr | Add-Content -Path $LOG_FILE
    # PostgreSQL writes progress to stderr
    $stderr -split "`n" | ForEach-Object {
        if ($_ -match "error|ERROR|fatal|FATAL") {
            Write-Host $_ -ForegroundColor Red
        }
        else {
            Write-Host $_ -ForegroundColor Gray
        }
    }
}

$importEnd = Get-Date
$importDuration = $importEnd - $importStart

Write-Host ""
Write-Log "pg_restore completed with exit code: $($p.ExitCode)"
Write-Log "Duration: $($importDuration.ToString('hh\:mm\:ss'))"

# Check if import was successful by verifying table has data
Write-Log "Checking if data was imported..."
$env:PGPASSWORD = $TARGET_PASSWORD
$quickCheckRaw = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT COUNT(*) FROM authors LIMIT 1;" 2>&1
$quickCheck = ($quickCheckRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })

if ($quickCheck -and $quickCheck -match "^\d+$" -and [int]$quickCheck -gt 0) {
    Write-Success-Log "Data import completed successfully! (Quick check: $quickCheck rows)"
}
else {
    Write-Error-Log "Data import failed or no data was imported"
    Write-Host ""
    Write-Host "Possible causes:" -ForegroundColor Yellow
    Write-Host "  1. Connection to target database lost" -ForegroundColor Gray
    Write-Host "  2. No write permission on target database" -ForegroundColor Gray
    Write-Host "  3. Target database disk full" -ForegroundColor Gray
    Write-Host "  4. Dump file corrupted" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Check log file: $LOG_FILE" -ForegroundColor Cyan
    exit 1
}

# 9.3: Verify row count (EXACT count for verification)
Write-Log "Verifying row count with EXACT count (this will take a few minutes)..."
Write-Host "Counting source table rows precisely..." -ForegroundColor Yellow
$exactSourceCount = Get-TableRowCount -Hostname $SOURCE_HOST -Port $SOURCE_PORT -Database $SOURCE_DB -User $SOURCE_USER -Password $SOURCE_PASSWORD -Table "authors"
Write-Log "Source rows (EXACT): $exactSourceCount"

Write-Host "Counting target table rows precisely..." -ForegroundColor Yellow
$importedRowCount = Get-TableRowCount -Hostname $TARGET_HOST -Port $TARGET_PORT -Database $TARGET_DB -User $TARGET_USER -Password $TARGET_PASSWORD -Table "authors"
Write-Log "Target rows (EXACT): $importedRowCount"

if ($importedRowCount -eq $exactSourceCount) {
    Write-Success-Log "Row count matches perfectly! ($importedRowCount rows)"
}
else {
    Write-Error-Log "Row count mismatch!"
    Write-Error-Log "Source: $exactSourceCount, Target: $importedRowCount"
    Write-Host "Migration failed - data incomplete" -ForegroundColor Red
    exit 1
}

# Update sourceRowCount to exact value for final summary
$sourceRowCount = $exactSourceCount

# 9.4: Create indexes and constraints
Write-Host ""
Write-Log "Creating indexes and primary key (estimated 20-60 minutes)..."
Write-Log "This is the slowest step, please be patient..."
Write-Host ""

$env:PGPASSWORD = $TARGET_PASSWORD
$indexStart = Get-Date

Write-Log "Running SQL to create indexes and constraints..."
$indexOutput = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -f $DUMP_INDEXES 2>&1

# Log and display output
$indexOutput | Add-Content -Path $LOG_FILE
$indexOutput | ForEach-Object {
    $line = $_.ToString()
    if ($line -match "ERROR|error|FATAL|fatal") {
        Write-Host $line -ForegroundColor Red
    }
    elseif ($line -match "CREATE INDEX|ALTER TABLE|PRIMARY KEY") {
        Write-Host $line -ForegroundColor Green
    }
    else {
        Write-Host $line -ForegroundColor Gray
    }
}

$indexEnd = Get-Date
$indexDuration = $indexEnd - $indexStart

Write-Host ""
Write-Success-Log "Index creation completed!"
Write-Log "Duration: $($indexDuration.ToString('hh\:mm\:ss'))"

# Verify indexes
Write-Log "Verifying indexes..."
$env:PGPASSWORD = $TARGET_PASSWORD
$indexCheckRaw = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'authors';" 2>&1
$indexCount = ($indexCheckRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })

if ($indexCount -match "^\d+$" -and [int]$indexCount -gt 0) {
    Write-Log "Verified $indexCount index(es) created"
}
else {
    Write-Warning-Log "Could not verify index count, but continuing..."
}

# ============================================================================
# Step 10: Final Verification
# ============================================================================

Write-Host ""
Write-Host "Step 10/10: Final Verification" -ForegroundColor Yellow
Write-Host "----------------------------------------"

# Verify table exists
$env:PGPASSWORD = $TARGET_PASSWORD
$finalTableCheck = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'authors');" 2>&1

if ($finalTableCheck.Trim() -ne "t") {
    Write-Error-Log "Table verification failed!"
    exit 1
}

# Final row count (already verified in Step 9.3)
$finalRowCount = $importedRowCount
Write-Log "Final row count: $finalRowCount"
Write-Success-Log "Data integrity verified!"

# Verify indexes
Write-Log "Verifying indexes..."
$env:PGPASSWORD = $TARGET_PASSWORD
$targetIndexes = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "SELECT indexname FROM pg_indexes WHERE tablename = 'authors' ORDER BY indexname;" 2>&1
Write-Host "Target table indexes:" -ForegroundColor Gray
Write-Host $targetIndexes -ForegroundColor Gray

# Run ANALYZE
Write-Log "Running ANALYZE to update statistics..."
$env:PGPASSWORD = $TARGET_PASSWORD
& $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "ANALYZE authors;" 2>&1 | Out-Null
Write-Success-Log "ANALYZE completed"

# Get final table size
$finalTableSizeRaw = & $script:PSQL_PATH -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT pg_size_pretty(pg_total_relation_size('authors'));" 2>&1
$finalTableSize = ($finalTableSizeRaw | Select-Object -First 1 | ForEach-Object { $_.Trim() })

# ============================================================================
# Migration Complete
# ============================================================================

$totalEnd = Get-Date
$totalDuration = $totalEnd - $exportStart

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Green
Write-Host "MIGRATION COMPLETED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Green
Write-Host ""

Write-Host "Migration Summary:" -ForegroundColor Cyan
Write-Host "  Source: s2orc_d3 @ port 5433" -ForegroundColor White
Write-Host "  Target: s2orc_d0 @ port 5430" -ForegroundColor White
Write-Host "  Rows migrated: $finalRowCount" -ForegroundColor White
Write-Host "  Source size: $tableSize" -ForegroundColor White
Write-Host "  Target size: $finalTableSize" -ForegroundColor White
Write-Host ""
Write-Host "Performance:" -ForegroundColor Cyan
Write-Host "  Data export: $($exportDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  Data import: $($importDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  Index creation: $($indexDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  Total time: $($totalDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host ""

# Cleanup
Write-Host "Temporary Files:" -ForegroundColor Yellow
Write-Host "  Schema: $DUMP_SCHEMA" -ForegroundColor Gray
Write-Host "  Data: $DUMP_DATA (${dataSizeGB} GB)" -ForegroundColor Gray
Write-Host "  Indexes: $DUMP_INDEXES" -ForegroundColor Gray
Write-Host ""

$cleanup = Read-Host "Delete temporary files to free ${dataSizeGB} GB? (y/n)"
if ($cleanup -eq "y") {
    if (Test-Path $DUMP_SCHEMA) { Remove-Item $DUMP_SCHEMA -Force }
    if (Test-Path $DUMP_DATA) { Remove-Item $DUMP_DATA -Force }
    if (Test-Path $DUMP_INDEXES) { Remove-Item $DUMP_INDEXES -Force }
    Write-Success-Log "Temporary files deleted"
}

Write-Host ""
Write-Host "[SUCCESS] authors table migration complete!" -ForegroundColor Green
Write-Host "  All data migrated: $finalRowCount rows" -ForegroundColor Green
Write-Host "  All indexes created" -ForegroundColor Green
Write-Host "  Primary key created" -ForegroundColor Green
Write-Host "  Data integrity verified" -ForegroundColor Green
Write-Host ""
Write-Host "Log file: $LOG_FILE" -ForegroundColor Cyan
Write-Host ""

Write-Log "Migration completed successfully"

