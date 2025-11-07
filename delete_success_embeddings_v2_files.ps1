# Script to delete successfully processed embeddings-specter_v2 gz files
$embeddingsV2Path = "E:\machine_win01\2025-09-30\embeddings-specter_v2"
$successFiles = Get-Content "logs\batch_update\embeddings_specter_v2\success.txt" | Where-Object { $_ -ne "" }

Write-Host "Starting deletion of successfully processed embeddings-specter_v2 files..."
Write-Host "Target directory: $embeddingsV2Path"
Write-Host "Number of files to delete: $($successFiles.Count)"
Write-Host ""

$deletedCount = 0
$notFoundCount = 0

foreach ($file in $successFiles) {
    $fullPath = Join-Path $embeddingsV2Path $file

    if (Test-Path $fullPath) {
        try {
            Remove-Item $fullPath -Force
            Write-Host "✓ Deleted: $file" -ForegroundColor Green
            $deletedCount++
        }
        catch {
            Write-Host "✗ Failed to delete: $file - $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    else {
        Write-Host "⚠ Not found: $file" -ForegroundColor Yellow
        $notFoundCount++
    }
}

Write-Host ""
Write-Host "Summary:"
Write-Host "  Deleted: $deletedCount files"
Write-Host "  Not found: $notFoundCount files"
Write-Host "  Total processed: $($successFiles.Count) files"
