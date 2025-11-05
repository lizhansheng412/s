# PostgreSQL 表迁移工具

## 📁 文件说明

### 1. `cleanup.ps1` - 清理临时文件
清理 PostgreSQL 表迁移过程中产生的临时文件（dump、schema、indexes等）

**使用方法：**
```powershell
# 预览要删除的文件（不实际删除）
.\migration_tools\cleanup.ps1 -DryRun

# 直接删除所有临时文件
.\migration_tools\cleanup.ps1 -DeleteAll

# 交互式删除（推荐）
.\migration_tools\cleanup.ps1
```

**参数：**
- `-DryRun` - 预览模式，只显示不删除
- `-DeleteAll` - 直接删除，不询问确认
- `-DeleteLogs` - 同时删除日志文件

### 2. `migrate_table.ps1` - 单表迁移
迁移单个表从源数据库到目标数据库

**使用方法：**
```powershell
# 迁移 papers 表
.\migration_tools\migrate_table.ps1 -TableName "papers"

# 自定义并行任务数
.\migration_tools\migrate_table.ps1 -TableName "authors" -ParallelJobs 16

# 自定义压缩级别（0-9）
.\migration_tools\migrate_table.ps1 -TableName "citations" -CompressionLevel 9
```

**参数：**
- `-TableName` - 表名（必需）
- `-ParallelJobs` - 并行任务数（默认: 8）
- `-CompressionLevel` - 压缩级别 0-9（默认: 6）

### 3. `migrate_batch.ps1` - 批量迁移
批量迁移多个表

**使用方法：**
```powershell
# 使用默认表列表
.\migration_tools\migrate_batch.ps1

# 自定义表列表
.\migration_tools\migrate_batch.ps1 -Tables @("papers", "authors", "citations")

# 自定义并行任务数
.\migration_tools\migrate_batch.ps1 -ParallelJobs 16
```

**参数：**
- `-Tables` - 表名数组（默认: papers, citations, publication_venues, tldrs）
- `-ParallelJobs` - 并行任务数（默认: 8）
- `-CompressionLevel` - 压缩级别（默认: 6）

## 🔄 完整迁移流程

### 1. 迁移单个表
```powershell
# 1. 迁移表
.\migration_tools\migrate_table.ps1 -TableName "papers"

# 2. 迁移完成后清理临时文件
.\migration_tools\cleanup.ps1 -DeleteAll
```

### 2. 批量迁移多表
```powershell
# 1. 批量迁移
.\migration_tools\migrate_batch.ps1

# 2. 全部完成后清理
.\migration_tools\cleanup.ps1 -DeleteAll
```

## ⚙️ 配置说明

### 源数据库配置（修改 migrate_table.ps1）
```powershell
$SOURCE_HOST = "localhost"
$SOURCE_PORT = "5433"
$SOURCE_DB = "s2orc_d3"
$SOURCE_USER = "postgres"
$SOURCE_PASSWORD = "grained"
```

### 目标数据库配置
```powershell
$TARGET_HOST = "localhost"
$TARGET_PORT = "5430"
$TARGET_DB = "s2orc_d0"
$TARGET_USER = "postgres"
$TARGET_PASSWORD = "grained"
```

### 临时文件目录
```powershell
$EXPORT_DIR = "D:\pg_migration_temp"
```

## 📊 性能优化

- **并行任务数**: 根据CPU核心数调整（推荐 8-16）
- **压缩级别**: 
  - 0: 无压缩（最快）
  - 6: 默认（平衡）
  - 9: 最大压缩（最慢，但文件最小）

## 🗑️ 临时文件清理

迁移过程会在 `D:\pg_migration_temp\` 生成临时文件：
- `*_schema.sql` - 表结构
- `*_data.dump` - 数据文件（最大，通常几十GB）
- `*_indexes.sql` - 索引和约束
- `*_migration.log` - 迁移日志

**建议**: 每次迁移完成后运行 `cleanup.ps1 -DeleteAll` 清理临时文件。

## ⚠️ 注意事项

1. 确保目标磁盘有足够空间（临时文件可能很大）
2. 迁移大表时需要较长时间（可能数小时）
3. 迁移前建议备份目标数据库
4. 如果目标表已存在，脚本会询问是否删除

