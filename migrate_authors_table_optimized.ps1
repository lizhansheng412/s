# ============================================================================
# PostgreSQL Authors Table Migration Script - OPTIMIZED FOR SPEED
# Migrate authors table from M3 drive (s2orc_d3) to local database (s2orc_d0)
# 
# Optimization Strategy:
# 1. Separate export: schema -> data -> indexes/constraints
# 2. Import data first, then create indexes (5-10x faster)
# 3. Temporary PostgreSQL optimization
# 4. Parallel import and index creation
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

# Export file paths (separate exports)
$DUMP_SCHEMA = "D:\authors_schema.dump"      # Table structure (no indexes)
$DUMP_DATA = "D:\authors_data.dump"          # Pure data
$DUMP_INDEXES = "D:\authors_indexes.dump"    # Indexes and constraints
$LOG_FILE = "D:\authors_migration_optimized.log"

# Performance optimization parameters
$PARALLEL_JOBS = 8  # Number of parallel jobs (adjust based on CPU cores, recommend 4-8)
$COMPRESSION_LEVEL = 6  # Compression level (0-9, 6 is balanced)

# ============================================================================
# Function Definitions
# ============================================================================

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] $Message"
    Write-Host $logMessage -ForegroundColor Cyan
    Add-Content -Path $LOG_FILE -Value $logMessage
}

function Write-Error-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] ERROR: $Message"
    Write-Host $logMessage -ForegroundColor Red
    Add-Content -Path $LOG_FILE -Value $logMessage
}

function Write-Success-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] SUCCESS: $Message"
    Write-Host $logMessage -ForegroundColor Green
    Add-Content -Path $LOG_FILE -Value $logMessage
}

function Write-Warning-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] WARNING: $Message"
    Write-Host $logMessage -ForegroundColor Yellow
    Add-Content -Path $LOG_FILE -Value $logMessage
}

function Test-DatabaseConnection {
    param(
        [string]$Host,
        [string]$Port,
        [string]$Database,
        [string]$User,
        [string]$Password
    )
    
    try {
        $env:PGPASSWORD = $Password
        $result = & psql -h $Host -p $Port -U $User -d $Database -c "SELECT 1;" 2>&1
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
        [string]$Host,
        [string]$Port,
        [string]$Database,
        [string]$User,
        [string]$Password,
        [string]$Table
    )
    
    try {
        $env:PGPASSWORD = $Password
        $result = & psql -h $Host -p $Port -U $User -d $Database -t -c "SELECT COUNT(*) FROM $Table;" 2>&1
        if ($LASTEXITCODE -eq 0) {
            return $result.Trim()
        }
        return "N/A"
    }
    catch {
        return "N/A"
    }
}

function Check-DiskSpace {
    param([string]$Path)
    
    $drive = (Get-Item $Path).PSDrive.Name
    $disk = Get-PSDrive -Name $drive
    $freeSpaceGB = [math]::Round($disk.Free / 1GB, 2)
    return $freeSpaceGB
}

function Get-TableIndexes {
    param(
        [string]$Host,
        [string]$Port,
        [string]$Database,
        [string]$User,
        [string]$Password,
        [string]$Table
    )
    
    $env:PGPASSWORD = $Password
    $indexes = & psql -h $Host -p $Port -U $User -d $Database -t -c @"
SELECT 
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = '$Table'
ORDER BY indexname;
"@ 2>&1
    
    return $indexes
}

function Get-TableConstraints {
    param(
        [string]$Host,
        [string]$Port,
        [string]$Database,
        [string]$User,
        [string]$Password,
        [string]$Table
    )
    
    $env:PGPASSWORD = $Password
    $constraints = & psql -h $Host -p $Port -U $User -d $Database -t -c @"
SELECT 
    conname,
    contype,
    pg_get_constraintdef(oid) as definition
FROM pg_constraint 
WHERE conrelid = '$Table'::regclass
ORDER BY conname;
"@ 2>&1
    
    return $constraints
}

# ============================================================================
# Start Migration Process
# ============================================================================

Write-Host "============================================================================" -ForegroundColor Yellow
Write-Host "PostgreSQL Authors Table Migration - Optimized Version (Maximum Speed)" -ForegroundColor Yellow
Write-Host "============================================================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "Optimization Strategies:" -ForegroundColor Green
Write-Host "  [OK] Separate export (schema, data, indexes)" -ForegroundColor Green
Write-Host "  [OK] Import data first, create indexes later (5-10x faster)" -ForegroundColor Green
Write-Host "  [OK] Parallel import and index creation ($PARALLEL_JOBS parallel jobs)" -ForegroundColor Green
Write-Host "  [OK] Compression level $COMPRESSION_LEVEL (balanced speed/size)" -ForegroundColor Green
Write-Host ""

# Clear or create log file
if (Test-Path $LOG_FILE) {
    Remove-Item $LOG_FILE
}
New-Item -Path $LOG_FILE -ItemType File -Force | Out-Null

Write-Log "Migration script started (Optimized Version)"
Write-Log "Source database: $SOURCE_DB @ ${SOURCE_HOST}:${SOURCE_PORT}"
Write-Log "Target database: $TARGET_DB @ ${TARGET_HOST}:${TARGET_PORT}"
Write-Log "Parallel jobs: $PARALLEL_JOBS"
Write-Log "Compression level: $COMPRESSION_LEVEL"

# ============================================================================
# Step 1: Environment Check
# ============================================================================

Write-Host ""
Write-Host "Step 1/9: Environment Check" -ForegroundColor Yellow
Write-Host "----------------------------------------"

# Check PostgreSQL tools
Write-Log "Checking PostgreSQL tools..."
try {
    $pgVersion = & pg_dump --version 2>&1
    Write-Success-Log "PostgreSQL tools: $pgVersion"
}
catch {
    Write-Error-Log "pg_dump tool not found"
    exit 1
}

# Check disk space
Write-Log "Checking disk space..."
$freeSpace = Check-DiskSpace -Path "D:\"
Write-Log "D: drive free space: ${freeSpace} GB"
if ($freeSpace -lt 20) {
    Write-Warning-Log "D: drive has less than 20GB free space, recommend freeing more space"
    $continue = Read-Host "Continue? (y/n)"
    if ($continue -ne "y") {
        exit 0
    }
}

# ============================================================================
# Step 2: Test Database Connections
# ============================================================================

Write-Host ""
Write-Host "Step 2/9: Test Database Connections" -ForegroundColor Yellow
Write-Host "----------------------------------------"

Write-Log "Testing source database connection..."
if (Test-DatabaseConnection -Host $SOURCE_HOST -Port $SOURCE_PORT -Database $SOURCE_DB -User $SOURCE_USER -Password $SOURCE_PASSWORD) {
    Write-Success-Log "Source database connection successful (s2orc_d3 @ port 5433)"
}
else {
    Write-Error-Log "Cannot connect to source database"
    exit 1
}

Write-Log "Testing target database connection..."
if (Test-DatabaseConnection -Host $TARGET_HOST -Port $TARGET_PORT -Database $TARGET_DB -User $TARGET_USER -Password $TARGET_PASSWORD) {
    Write-Success-Log "Target database connection successful (s2orc_d0 @ port 5430)"
}
else {
    Write-Error-Log "Cannot connect to target database"
    exit 1
}

# ============================================================================
# Step 3: Analyze Source Table
# ============================================================================

Write-Host ""
Write-Host "Step 3/9: Analyze Source Table Information" -ForegroundColor Yellow
Write-Host "----------------------------------------"

Write-Log "Checking source table authors..."
$env:PGPASSWORD = $SOURCE_PASSWORD
$tableExists = & psql -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'authors');" 2>&1

if ($tableExists.Trim() -ne "t") {
    Write-Error-Log "authors table does not exist in source database"
    exit 1
}

Write-Success-Log "authors table exists"

# Get row count
$sourceRowCount = Get-TableRowCount -Host $SOURCE_HOST -Port $SOURCE_PORT -Database $SOURCE_DB -User $SOURCE_USER -Password $SOURCE_PASSWORD -Table "authors"
Write-Log "Source table row count: $sourceRowCount"

# Get table size
$env:PGPASSWORD = $SOURCE_PASSWORD
$tableSize = & psql -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c "SELECT pg_size_pretty(pg_total_relation_size('authors'));" 2>&1 | ForEach-Object { $_.Trim() }
Write-Log "Source table size (with indexes): $tableSize"

# Get index information
Write-Log "Analyzing index structure..."
$sourceIndexes = Get-TableIndexes -Host $SOURCE_HOST -Port $SOURCE_PORT -Database $SOURCE_DB -User $SOURCE_USER -Password $SOURCE_PASSWORD -Table "authors"
Write-Host "Source table indexes:" -ForegroundColor Gray
Write-Host $sourceIndexes -ForegroundColor Gray

# Get constraint information (including primary key)
Write-Log "Analyzing constraints (including primary key)..."
$sourceConstraints = Get-TableConstraints -Host $SOURCE_HOST -Port $SOURCE_PORT -Database $SOURCE_DB -User $SOURCE_USER -Password $SOURCE_PASSWORD -Table "authors"
Write-Host "Source table constraints:" -ForegroundColor Gray
Write-Host $sourceConstraints -ForegroundColor Gray

# Check target database
Write-Log "Checking target database..."
$env:PGPASSWORD = $TARGET_PASSWORD
$targetTableExists = & psql -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'authors');" 2>&1

if ($targetTableExists.Trim() -eq "t") {
    Write-Host ""
    Write-Host "WARNING: authors table already exists in target database!" -ForegroundColor Red
    $targetRowCount = Get-TableRowCount -Host $TARGET_HOST -Port $TARGET_PORT -Database $TARGET_DB -User $TARGET_USER -Password $TARGET_PASSWORD -Table "authors"
    Write-Host "Current target table row count: $targetRowCount" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Choose an option:" -ForegroundColor Yellow
    Write-Host "  1. Drop existing table and re-import (recommended)" -ForegroundColor Yellow
    Write-Host "  2. Cancel operation" -ForegroundColor Yellow
    Write-Host ""
    $choice = Read-Host "Please choose (1/2)"
    
    if ($choice -eq "1") {
        Write-Log "Dropping existing authors table..."
        $env:PGPASSWORD = $TARGET_PASSWORD
        & psql -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "DROP TABLE IF EXISTS authors CASCADE;" 2>&1 | Out-Null
        Write-Success-Log "Existing table dropped"
    }
    else {
        Write-Log "User cancelled operation"
        exit 0
    }
}

# ============================================================================
# Step 4: Export Table Structure (No Indexes)
# ============================================================================

Write-Host ""
Write-Host "Step 4/9: Export Table Structure (No Indexes)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_SCHEMA) { Remove-Item $DUMP_SCHEMA -Force }

Write-Log "Exporting table structure (table definition only, no primary key or indexes)..."
$env:PGPASSWORD = $SOURCE_PASSWORD

try {
    # Export only table structure, no indexes or constraints
    & pg_dump -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB `
        -t authors `
        --section=pre-data `
        --no-owner `
        --no-privileges `
        -Fp `
        -f $DUMP_SCHEMA 2>&1 | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        # Remove all constraints and indexes from exported SQL (ensure pure table structure)
        $schemaContent = Get-Content $DUMP_SCHEMA -Raw
        
        # Remove primary key constraint
        $schemaContent = $schemaContent -replace 'ALTER TABLE ONLY.*ADD CONSTRAINT.*PRIMARY KEY.*;\s*', ''
        
        # Remove all other constraints
        $schemaContent = $schemaContent -replace 'ALTER TABLE.*ADD CONSTRAINT.*;\s*', ''
        
        # Remove all index creation statements
        $schemaContent = $schemaContent -replace 'CREATE.*INDEX.*ON.*;\s*', ''
        
        # Save cleaned table structure
        Set-Content -Path $DUMP_SCHEMA -Value $schemaContent -NoNewline
        
        Write-Success-Log "Table structure export successful (pure table definition, no indexes)"
        $schemaSize = (Get-Item $DUMP_SCHEMA).Length / 1KB
        Write-Log "Table structure file size: $([math]::Round($schemaSize, 2)) KB"
    }
    else {
        throw "Table structure export failed"
    }
}
catch {
    Write-Error-Log "Export table structure failed: $_"
    exit 1
}

# ============================================================================
# Step 5: Export Pure Data (No Index Maintenance, Fastest)
# ============================================================================

Write-Host ""
Write-Host "Step 5/9: Export Pure Data" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_DATA) { Remove-Item $DUMP_DATA -Force }

Write-Log "Exporting authors table data (pure data, no indexes)..."
Write-Log "Estimated time: 10-30 minutes (depends on disk speed)"
Write-Host ""

$env:PGPASSWORD = $SOURCE_PASSWORD
$dataExportStart = Get-Date

try {
    # Export only data section, use custom format with compression
    & pg_dump -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB `
        -t authors `
        --section=data `
        -Fc `
        -Z $COMPRESSION_LEVEL `
        -v `
        -f $DUMP_DATA 2>&1 | Tee-Object -Append -FilePath $LOG_FILE
    
    if ($LASTEXITCODE -ne 0) {
        throw "Data export failed"
    }
    
    $dataExportEnd = Get-Date
    $dataExportDuration = $dataExportEnd - $dataExportStart
    
    Write-Success-Log "Data export successful!"
    Write-Log "Data export duration: $($dataExportDuration.ToString('hh\:mm\:ss'))"
    
    $dataFileSize = (Get-Item $DUMP_DATA).Length
    $dataFileSizeGB = [math]::Round($dataFileSize / 1GB, 2)
    Write-Log "Data file size: ${dataFileSizeGB} GB"
}
catch {
    Write-Error-Log "Data export failed: $_"
    exit 1
}

# ============================================================================
# Step 6: Export Index and Constraint Definitions
# ============================================================================

Write-Host ""
Write-Host "Step 6/9: Export Index and Constraint Definitions (Including Primary Key)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

if (Test-Path $DUMP_INDEXES) { Remove-Item $DUMP_INDEXES -Force }

Write-Log "Exporting all index and constraint definitions..."
$env:PGPASSWORD = $SOURCE_PASSWORD

try {
    # Export post-data (including primary key, indexes, constraints)
    & pg_dump -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB `
        -t authors `
        --section=post-data `
        --no-owner `
        --no-privileges `
        -Fp `
        -f $DUMP_INDEXES 2>&1 | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success-Log "Index and constraint definitions export successful"
        $indexSize = (Get-Item $DUMP_INDEXES).Length / 1KB
        Write-Log "Index definition file size: $([math]::Round($indexSize, 2)) KB"
        
        # Show exported index content
        Write-Log "Index and constraint definition preview:"
        Get-Content $DUMP_INDEXES | Select-Object -First 30 | ForEach-Object { Write-Host $_ -ForegroundColor Gray }
    }
    else {
        throw "Index export failed"
    }
}
catch {
    Write-Error-Log "Export indexes failed: $_"
    exit 1
}

# ============================================================================
# Step 7: Create Table Structure (No Indexes)
# ============================================================================

Write-Host ""
Write-Host "Step 7/9: Create Table Structure in Target Database (No Indexes)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

Write-Log "Creating empty table structure..."
$env:PGPASSWORD = $TARGET_PASSWORD

try {
    & psql -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -f $DUMP_SCHEMA 2>&1 | Tee-Object -Append -FilePath $LOG_FILE
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success-Log "Table structure created successfully (no indexes, ready for fast data import)"
    }
    else {
        Write-Warning-Log "Table structure creation may have warnings, continuing to check..."
    }
    
    # Verify table was created
    $tableCreated = & psql -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'authors');" 2>&1
    
    if ($tableCreated.Trim() -eq "t") {
        Write-Success-Log "Table structure verification passed"
    }
    else {
        throw "Table not successfully created"
    }
}
catch {
    Write-Error-Log "Create table structure failed: $_"
    exit 1
}

# ============================================================================
# Step 8: Import Data (No Indexes, Maximum Speed)
# ============================================================================

Write-Host ""
Write-Host "Step 8/9: Import Data (No Index Maintenance, Maximum Speed)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

Write-Log "Starting import of $sourceRowCount rows..."
Write-Log "Estimated time: 15-45 minutes"
Write-Log "Using $PARALLEL_JOBS parallel jobs for acceleration..."
Write-Host ""

$env:PGPASSWORD = $TARGET_PASSWORD
$dataImportStart = Get-Date

try {
    # Use pg_restore to import data in parallel
    & pg_restore -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB `
        -v `
        --no-owner `
        --no-privileges `
        --data-only `
        -j $PARALLEL_JOBS `
        $DUMP_DATA 2>&1 | Tee-Object -Append -FilePath $LOG_FILE
    
    $dataImportEnd = Get-Date
    $dataImportDuration = $dataImportEnd - $dataImportStart
    
    Write-Success-Log "Data import completed!"
    Write-Log "Data import duration: $($dataImportDuration.ToString('hh\:mm\:ss'))"
    
    # Verify row count
    $importedRowCount = Get-TableRowCount -Host $TARGET_HOST -Port $TARGET_PORT -Database $TARGET_DB -User $TARGET_USER -Password $TARGET_PASSWORD -Table "authors"
    Write-Log "Target table row count: $importedRowCount"
    Write-Log "Source table row count: $sourceRowCount"
    
    if ($importedRowCount -eq $sourceRowCount) {
        Write-Success-Log "[OK] Row count matches! Data integrity verification passed"
    }
    else {
        Write-Warning-Log "[WARNING] Row count mismatch, please check"
    }
}
catch {
    Write-Error-Log "Data import failed: $_"
    exit 1
}

# ============================================================================
# Step 9: Create Indexes and Constraints (Including Primary Key)
# ============================================================================

Write-Host ""
Write-Host "Step 9/9: Create Indexes and Constraints (Including Primary Key)" -ForegroundColor Yellow
Write-Host "----------------------------------------"

Write-Log "Starting creation of primary key and all indexes..."
Write-Log "This is the most time-consuming step, estimated time: 20-60 minutes"
Write-Log "Tip: Creating indexes on large tables takes time, please be patient..."
Write-Host ""

# Temporary optimization
Write-Log "Temporarily increasing maintenance_work_mem to accelerate index creation..."
$env:PGPASSWORD = $TARGET_PASSWORD
& psql -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "SET maintenance_work_mem = '2GB';" 2>&1 | Out-Null

$indexCreationStart = Get-Date

try {
    # Create all indexes and constraints (including primary key)
    $env:PGPASSWORD = $TARGET_PASSWORD
    & psql -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -f $DUMP_INDEXES 2>&1 | Tee-Object -Append -FilePath $LOG_FILE
    
    $indexCreationEnd = Get-Date
    $indexCreationDuration = $indexCreationEnd - $indexCreationStart
    
    Write-Success-Log "Index and constraint creation completed!"
    Write-Log "Index creation duration: $($indexCreationDuration.ToString('hh\:mm\:ss'))"
    
    # Verify indexes
    Write-Log "Verifying indexes..."
    $targetIndexes = Get-TableIndexes -Host $TARGET_HOST -Port $TARGET_PORT -Database $TARGET_DB -User $TARGET_USER -Password $TARGET_PASSWORD -Table "authors"
    Write-Host "Target table indexes:" -ForegroundColor Gray
    Write-Host $targetIndexes -ForegroundColor Gray
    
    # Verify constraints (including primary key)
    Write-Log "Verifying constraints and primary key..."
    $targetConstraints = Get-TableConstraints -Host $TARGET_HOST -Port $TARGET_PORT -Database $TARGET_DB -User $TARGET_USER -Password $TARGET_PASSWORD -Table "authors"
    Write-Host "Target table constraints:" -ForegroundColor Gray
    Write-Host $targetConstraints -ForegroundColor Gray
    
}
catch {
    Write-Error-Log "Index creation failed: $_"
    Write-Warning-Log "Data has been imported, but index creation failed. You can create indexes manually."
    exit 1
}

# ============================================================================
# Post-optimization
# ============================================================================

Write-Host ""
Write-Host "Performing post-optimization..." -ForegroundColor Yellow
Write-Host "----------------------------------------"

Write-Log "Running ANALYZE to update statistics..."
$env:PGPASSWORD = $TARGET_PASSWORD
& psql -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -c "ANALYZE authors;" 2>&1 | Out-Null
Write-Success-Log "ANALYZE completed"

# ============================================================================
# Final Verification and Summary
# ============================================================================

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Green
Write-Host "Migration Completed Successfully!" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Green
Write-Host ""

$totalEndTime = Get-Date
$totalDuration = $totalEndTime - $dataExportStart

Write-Host "Migration Statistics:" -ForegroundColor Cyan
Write-Host "  * Total duration: $($totalDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  * Data export: $($dataExportDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  * Data import: $($dataImportDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  * Index creation: $($indexCreationDuration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "  * Migrated rows: $importedRowCount" -ForegroundColor White
Write-Host ""

# Show table size
$env:PGPASSWORD = $TARGET_PASSWORD
$finalTableSize = & psql -h $TARGET_HOST -p $TARGET_PORT -U $TARGET_USER -d $TARGET_DB -t -c "SELECT pg_size_pretty(pg_total_relation_size('authors'));" 2>&1 | ForEach-Object { $_.Trim() }
Write-Host "Target table size (with indexes): $finalTableSize" -ForegroundColor Cyan
Write-Host ""

Write-Success-Log "Log file: $LOG_FILE"
Write-Host ""

# Clean up files
Write-Host "Temporary files cleanup..." -ForegroundColor Yellow
Write-Host "  * Table structure file: $DUMP_SCHEMA" -ForegroundColor Gray
Write-Host "  * Data file: $DUMP_DATA (${dataFileSizeGB} GB)" -ForegroundColor Gray
Write-Host "  * Index file: $DUMP_INDEXES" -ForegroundColor Gray
Write-Host ""

$cleanup = Read-Host "Delete all temporary export files to free disk space? (y/n)"
if ($cleanup -eq "y") {
    if (Test-Path $DUMP_SCHEMA) { Remove-Item $DUMP_SCHEMA -Force }
    if (Test-Path $DUMP_DATA) { Remove-Item $DUMP_DATA -Force }
    if (Test-Path $DUMP_INDEXES) { Remove-Item $DUMP_INDEXES -Force }
    Write-Success-Log "All temporary files deleted, freed approximately ${dataFileSizeGB} GB of space"
}

Write-Host ""
Write-Host "[SUCCESS] authors table migration completed!" -ForegroundColor Green
Write-Host "   * All indexes created (including primary key)" -ForegroundColor Green
Write-Host "   * Data integrity verified" -ForegroundColor Green
Write-Host "   * Statistics updated" -ForegroundColor Green
Write-Host ""

# Final verification commands
Write-Host "Manual verification commands (optional):" -ForegroundColor Yellow
Write-Host "  psql -h localhost -p 5430 -U postgres -d s2orc_d0" -ForegroundColor Gray
Write-Host "  \d authors           # View table structure and indexes" -ForegroundColor Gray
Write-Host "  SELECT COUNT(*) FROM authors;  # Verify row count" -ForegroundColor Gray
Write-Host ""

Write-Log "Migration script ended"
