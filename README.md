# S2ORC 数据库导入工具

高性能并行处理工具，用于将S2ORC数据集的GZ文件导入PostgreSQL数据库。

## ✨ 核心特性

- ⚡ **极速导入**: 50,000+条/秒（移除索引后）
- 🔄 **断点续传**: 中断后自动恢复
- 🎯 **智能主键**: 自动识别不同表的主键字段
- 🔐 **100% E盘存储**: 所有数据存储在 `E:\postgreSQL`

---

## 🚀 标准导入流程（推荐）

### 单机导入（以 Machine3 为例）

```powershell
# 1. 初始化数据库（创建表）
python scripts/init_database.py --init --machine machine3

# 2. 移除所有索引（极速模式）
python scripts/optimize_table_indexes.py --machine machine3 --remove-indexes

# 3. 极速导入（50,000+条/秒）
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"

# 4. 恢复索引（一次性建索引+自动去重）
python scripts/optimize_table_indexes.py --machine machine3 --restore-indexes
```

### 分布式导入（4台机器并行）

| 机器 | 负责的表 |
|------|---------|
| **Machine1** | embeddings_specter_v1, s2orc |
| **Machine2** | embeddings_specter_v2, s2orc_v2 |
| **Machine3** | abstracts, authors, papers, publication_venues, tldrs, citations |
| **Machine0** | paper_ids |

**每台机器执行相同流程**：

```powershell
# 替换 machineX 为 machine1/machine2/machine3/machine0
python scripts/init_database.py --init --machine machineX
python scripts/optimize_table_indexes.py --machine machineX --remove-indexes
python scripts/batch_process_machine.py --machine machineX --base-dir "E:\2025-09-30"
python scripts/optimize_table_indexes.py --machine machineX --restore-indexes
```

---

## 🔄 断点续传

导入中断后，直接重新运行导入命令即可（自动跳过已完成文件）：

```powershell
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"
```

---

## ⚙️ 高级操作

### 单表操作

```powershell
# 移除单表索引
python scripts/optimize_table_indexes.py --table papers --remove-indexes

# 导入单个文件夹
python scripts/stream_gz_to_db_optimized.py --dir "E:\2025-09-30\papers" --table papers

# 恢复单表索引
python scripts/optimize_table_indexes.py --table papers --restore-indexes
```

### 批量指定表

```powershell
python scripts/optimize_table_indexes.py --tables papers abstracts authors --remove-indexes
python scripts/optimize_table_indexes.py --tables papers abstracts authors --restore-indexes
```

### 调整并发数

```powershell
# 减少内存占用（默认8进程）
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30" --extractors 4
```

---

## 🔧 故障排除

**Q: 如何重新开始？**
```powershell
# 清理指定机器的表（推荐）
python scripts/clean_start.py --machine machine3
python scripts/init_database.py --init --machine machine3

# 完全清理（删除整个数据库和表空间）
python scripts/clean_start.py
python scripts/init_database.py --init --machine machine3
```

**Q: 如何验证存储位置？**
```powershell
python scripts/verify_storage.py
```

**Q: 如何查看进度？**
```powershell
# 查看进度文件
type logs\progress\papers_progress.txt

# 查看已导入数据量
psql -U postgres -d s2orc_d1 -c "SELECT COUNT(*) FROM papers;"
```

**Q: USB 外接硬盘导入速度慢（<5000条/秒）？**

USB 硬盘写入慢会导致插入速度只有 873条/秒，使用以下完整流程优化至 **30000-60000条/秒**（提速 35-70 倍）：

```powershell
# 1. 优化 PostgreSQL 配置
python scripts/test/optimize_postgresql_for_usb.py `
    --config "C:\PostgreSQL\data\postgresql.conf" `
    --mode import

# 2. 重启 PostgreSQL
net stop postgresql-x64-13
net start postgresql-x64-13

# 3. 运行导入（速度提升至 30000-60000条/秒）
python scripts/test/batch_process_machine_mapping_test.py `
    --machine machine1 `
    --base-dir "E:\machine_win01\2025-09-30"

# 4. 导入完成后恢复安全配置
python scripts/test/optimize_postgresql_for_usb.py `
    --config "C:\PostgreSQL\data\postgresql.conf" `
    --mode safe

# 5. 重启 PostgreSQL
net stop postgresql-x64-13
net start postgresql-x64-13
```

⚠️ **重要**：
- `fsync=off` 会在断电时丢失数据，**仅用于批量导入**
- 导入完成后**必须执行步骤4和5**恢复安全配置
- 配置文件路径通常为：`C:\Program Files\PostgreSQL\{version}\data\postgresql.conf`

---

## 📝 配置说明

### 机器数据库和端口映射

| 机器 | 数据库 | 端口 | 表 |
|------|--------|------|-----|
| machine0 | s2orc_d0 | 5430 | paper_ids |
| machine1 | s2orc_d1 | 5431 | embeddings_specter_v1, s2orc |
| machine2 | s2orc_d2 | 5432 | embeddings_specter_v2, s2orc_v2 |
| machine3 | s2orc_d3 | 5433 | abstracts, authors, papers, etc. |

### 支持的表和主键

| 表名 | 主键字段 | 额外索引 |
|------|---------|---------|
| papers, abstracts, tldrs, s2orc, s2orc_v2, embeddings_* | corpusid | 无 |
| authors | authorid | 无 |
| publication_venues | publicationvenueid | 无 |
| paper_ids | corpusid | 无 |
| citations | id | citingcorpusid |

### 修改存储位置

编辑 `database/config/db_config_v2.py`：

```python
TABLESPACE_CONFIG = {
    'enabled': True,
    'name': 'd1_tablespace',
    'location': 'E:\\postgreSQL',  # ← 修改为其他盘符
}
```

---

## 🧪 轻量级映射表导入（测试/验证专用）

适用于需要快速验证或只需要 corpusid 映射的场景。

### 特点

- ⚡ **极速**：只提取 corpusid（无 JSONB 数据），速度快 10 倍以上
- 💾 **轻量**：单表存储所有数据集的 corpusid
- 🔒 **去重**：自动去除重复的 corpusid
- 🎯 **无索引导入**：先插入后建主键，最大化速度

### 使用流程（全部在 Machine1 上执行）

**前提：** 硬盘1（数据库）和硬盘2（数据源）同时连接到 Machine1

```powershell
# 步骤1：在硬盘1上创建数据库表（极速导入模式，无主键）
python scripts/test/init_mapping_table.py

# 步骤2：处理硬盘1数据 → 写入硬盘1数据库（embeddings_specter_v1 + s2orc）
python scripts/test/batch_process_machine_mapping_test.py `
    --machine machine1 `
    --base-dir "E:\2025-09-30"

# 步骤3：处理硬盘2数据 → 写入硬盘1数据库（embeddings_specter_v2 + s2orc_v2）
python scripts/test/batch_process_machine_mapping_test.py `
    --machine machine2 `
    --base-dir "F:\2025-09-30"

# 步骤4：导入完成后添加主键（快速去重+一次性建索引）
python scripts/test/init_mapping_table.py --add-pk
```

**说明：**
- 数据库位置：硬盘1（E盘）PostgreSQL 端口 5431
- 硬盘1数据：E:\2025-09-30\（embeddings_specter_v1, s2orc）
- 硬盘2数据：F:\2025-09-30\（embeddings_specter_v2, s2orc_v2）
- 步骤2和3可以按任意顺序执行，甚至可以并行执行（如果性能允许）

### 单文件夹导入

```powershell
# 导入单个数据集
python scripts/test/stream_gz_to_mapping_table.py `
    --dir "E:\data\s2orc" `
    --dataset s2orc `
    --extractors 4
```

### 支持的数据集

- `embeddings_specter_v1`
- `embeddings_specter_v2`
- `s2orc`
- `s2orc_v2`

### 输出表结构

```sql
-- 表名：corpus_bigdataset
CREATE TABLE corpus_bigdataset (
    corpusid BIGINT PRIMARY KEY  -- 4个大数据集的唯一 corpusid 并集
);
```

### 注意事项

1. **所有操作在 Machine1 执行**：数据库存储在硬盘1（PostgreSQL 端口 5431）
2. **硬盘配置**：
   - 硬盘1（E盘）：数据库 + embeddings_v1 + s2orc
   - 硬盘2（F盘）：embeddings_v2 + s2orc_v2
   - 两个硬盘同时连接到 Machine1
3. **--machine 参数**：指定使用哪个硬盘的文件夹配置，不是物理机器
   - `machine1` → 硬盘1的文件夹（embeddings_specter_v1, s2orc）
   - `machine2` → 硬盘2的文件夹（embeddings_specter_v2, s2orc_v2）
4. **可以并行处理**：如果 I/O 和 CPU 允许，步骤2和3可以同时执行
5. **断点续传**：使用独立的进度目录 `logs/progress_mapping/`
6. **内存优化**：Extractor 端自动去重，减少内存占用
7. **与正式导入隔离**：不影响现有的数据库表和进度