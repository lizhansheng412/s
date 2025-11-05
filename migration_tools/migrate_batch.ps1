# ============================================================================
# Batch Migration Script - Migrate Multiple Tables
# Migrate all remaining tables from s2orc_d3 to s2orc_d0
# ============================================================================

param(
    [Parameter(Mandatory=$false)]
    [string[]]$Tables = @("papers", "citations", "publication_venues", "tldrs"),
    
    [Parameter(Mandatory=$false)]
    [int]$ParallelJobs = 8,
    
    [Parameter(Mandatory=$false)]
    [int]$CompressionLevel = 6
)

Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "Batch Table Migration Tool" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Tables to migrate:" -ForegroundColor Yellow
foreach ($table in $Tables) {
    Write-Host "  - $table" -ForegroundColor White
}
Write-Host ""

$totalStartTime = Get-Date
$results = @()

foreach ($table in $Tables) {
    Write-Host ""
    Write-Host ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" -ForegroundColor Magenta
    Write-Host "Starting migration for table: $table" -ForegroundColor Magenta
    Write-Host ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" -ForegroundColor Magenta
    Write-Host ""
    
    $tableStartTime = Get-Date
    
    try {
        # Run the universal migration script for this table
        & .\migrate_table_universal.ps1 -TableName $table -ParallelJobs $ParallelJobs -CompressionLevel $CompressionLevel
        
        $tableEndTime = Get-Date
        $tableDuration = $tableEndTime - $tableStartTime
        
        $results += [PSCustomObject]@{
            Table = $table
            Status = "SUCCESS"
            Duration = $tableDuration.ToString('hh\:mm\:ss')
        }
        
        Write-Host ""
        Write-Host "Table '$table' migration completed successfully!" -ForegroundColor Green
        Write-Host ""
    }
    catch {
        $tableEndTime = Get-Date
        $tableDuration = $tableEndTime - $tableStartTime
        
        $results += [PSCustomObject]@{
            Table = $table
            Status = "FAILED"
            Duration = $tableDuration.ToString('hh\:mm\:ss')
        }
        
        Write-Host ""
        Write-Host "Table '$table' migration FAILED: $_" -ForegroundColor Red
        Write-Host ""
        
        $choice = Read-Host "Continue with next table? (y/n)"
        if ($choice -ne "y") {
            break
        }
    }
}

$totalEndTime = Get-Date
$totalDuration = $totalEndTime - $totalStartTime

# Print summary
Write-Host ""
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "BATCH MIGRATION SUMMARY" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

$results | Format-Table -AutoSize

Write-Host ""
Write-Host "Total time: $($totalDuration.ToString('hh\:mm\:ss'))" -ForegroundColor Cyan
Write-Host ""

$successCount = ($results | Where-Object { $_.Status -eq "SUCCESS" }).Count
$failedCount = ($results | Where-Object { $_.Status -eq "FAILED" }).Count

Write-Host "Results: $successCount succeeded, $failedCount failed" -ForegroundColor White
Write-Host ""

if ($failedCount -eq 0) {
    Write-Host "[SUCCESS] All tables migrated successfully!" -ForegroundColor Green
}
else {
    Write-Host "[WARNING] Some tables failed to migrate" -ForegroundColor Yellow
}

