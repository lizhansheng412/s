# 局域网文件传输脚本
# 从 machine2 (192.168.0.104) 复制指定文件到本地

# 配置
$fileListPath = "test_merge"
$remoteHost = "192.168.0.104"
$remoteFolder = "E:\final_delivery"
$localFolder = "E:\test"
$credential = $null  # 如需认证,使用: Get-Credential

# 创建本地目标文件夹
if (-not (Test-Path $localFolder)) {
    New-Item -ItemType Directory -Path $localFolder -Force | Out-Null
    Write-Host "✓ 已创建本地文件夹: $localFolder" -ForegroundColor Green
}

# 读取文件列表
$files = Get-Content $fileListPath | Where-Object { $_.Trim() -ne "" }
Write-Host "`n找到 $($files.Count) 个文件待复制`n" -ForegroundColor Cyan

# 统计
$success = 0
$failed = 0
$skipped = 0

foreach ($file in $files) {
    $fileName = $file.Trim()
    if (-not $fileName) { continue }
    
    # 构造远程路径 (UNC路径)
    $remotePath = "\\$remoteHost\$($remoteFolder.Replace(':', '$'))\$fileName"
    $localPath = Join-Path $localFolder $fileName
    
    Write-Host "[处理] $fileName" -NoNewline
    
    try {
        # 检查远程文件是否存在
        if (Test-Path $remotePath) {
            # 检查本地是否已存在
            if (Test-Path $localPath) {
                Write-Host " - 已存在,跳过" -ForegroundColor Yellow
                $skipped++
            } else {
                # 复制文件
                Copy-Item -Path $remotePath -Destination $localPath -Force
                Write-Host " - 成功" -ForegroundColor Green
                $success++
            }
        } else {
            Write-Host " - 远程文件不存在" -ForegroundColor Red
            $failed++
        }
    } catch {
        Write-Host " - 失败: $($_.Exception.Message)" -ForegroundColor Red
        $failed++
    }
}

# 输出统计
Write-Host "`n" + "="*60 -ForegroundColor Cyan
Write-Host "传输完成统计:" -ForegroundColor Cyan
Write-Host "  成功: $success" -ForegroundColor Green
Write-Host "  跳过: $skipped" -ForegroundColor Yellow
Write-Host "  失败: $failed" -ForegroundColor Red
Write-Host "="*60 -ForegroundColor Cyan

