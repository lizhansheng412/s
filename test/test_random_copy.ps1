#requires -Version 7.0

# 测试复制指定30个JSONL文件的性能（使用与 batch_copy.ps1 相同的复制逻辑）
param(
    [Parameter(Mandatory=$false)]
    [string]$SourceDir = "E:\final_delivery",
    
    [Parameter(Mandatory=$false)]
    [string]$DestDir = "D:\jsonl_copy"
)

# 要复制的30个文件列表
$files = @(
    "2da7da3f.jsonl",
    "2fd9645e.jsonl",
    "3cba8d31.jsonl",
    "3f50c70b.jsonl",
    "24de14de.jsonl",
    "25c88c54.jsonl",
    "62bd4673.jsonl",
    "74c1e952.jsonl",
    "80c89228.jsonl",
    "520ef2ac.jsonl",
    "685b98c3.jsonl",
    "999b2135.jsonl",
    "3833d5aa.jsonl",
    "32639d9d.jsonl",
    "92463a68.jsonl",
    "551064a0.jsonl",
    "638988e4.jsonl",
    "1228947f.jsonl",
    "b6565067.jsonl",
    "bda050dd.jsonl",
    "c08bc7e8.jsonl",
    "c574a204.jsonl",
    "cdcf2f97.jsonl",
    "ce1695cc.jsonl",
    "d3afbf1c.jsonl",
    "d6d23742.jsonl",
    "e0bc736a.jsonl",
    "e170cd64.jsonl",
    "f76b5a18.jsonl",
    "f11568e9.jsonl"
)

# 确保目标目录存在
if (-not (Test-Path $DestDir)) {
    New-Item -ItemType Directory -Path $DestDir -Force | Out-Null
}

# 记录开始时间（用于性能统计）
$startTime = Get-Date
$totalSize = 0

# 顺序复制（与 batch_copy.ps1 逻辑完全一致）
$successCount = 0

foreach ($fileName in $files) {
    $src = Join-Path $SourceDir $fileName
    $dst = Join-Path $DestDir $fileName
    
    if (Test-Path $src) {
        # 在复制前获取文件大小（用于性能统计）
        $fileInfo = Get-Item $src
        
        Copy-Item -Path $src -Destination $dst -Force -ErrorAction SilentlyContinue
        if ($?) {
            $successCount++
            $totalSize += $fileInfo.Length
        }
    }
}

# 计算性能统计
$endTime = Get-Date
$duration = ($endTime - $startTime).TotalSeconds
$totalSizeMB = [math]::Round($totalSize / 1MB, 2)
$avgSpeedMBps = if ($duration -gt 0) { [math]::Round($totalSizeMB / $duration, 2) } else { 0 }

# 输出统计信息
Write-Host "耗时: $([math]::Round($duration, 2))秒 | 大小: ${totalSizeMB}MB | 速度: ${avgSpeedMBps}MB/s" -ForegroundColor Green

# 输出固定格式（与 batch_copy.ps1 一致）
Write-Output "SUCCESS:$successCount"
exit 0
