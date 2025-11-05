#requires -Version 7.0

# 批量复制 JSONL 文件（顺序模式，适合 USB 机械硬盘）
param(
    [Parameter(Mandatory=$true)]
    [string]$FileListPath,
    
    [Parameter(Mandatory=$true)]
    [string]$SourceDir,
    
    [Parameter(Mandatory=$true)]
    [string]$DestDir
)

# 确保目标目录存在
if (-not (Test-Path $DestDir)) {
    New-Item -ItemType Directory -Path $DestDir -Force | Out-Null
}

# 读取文件列表
$files = Get-Content $FileListPath -ErrorAction SilentlyContinue

if (-not $files -or $files.Count -eq 0) {
    Write-Output "SUCCESS:0"
    exit 0
}

# 顺序复制（与手动命令逻辑一致，适合 USB 机械硬盘）
$successCount = 0

foreach ($fileName in $files) {
    $src = Join-Path $SourceDir $fileName
    $dst = Join-Path $DestDir $fileName
    
    if (Test-Path $src) {
        Copy-Item -Path $src -Destination $dst -Force -ErrorAction SilentlyContinue
        if ($?) {
            $successCount++
        }
    }
}

# 输出固定格式（便于 Python 解析）
Write-Output "SUCCESS:$successCount"
exit 0
