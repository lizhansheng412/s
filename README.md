# S2ORC 数据库导入工具

高性能并行处理工具，用于将S2ORC数据集的GZ文件导入PostgreSQL数据库。

## ✨ 核心特性

- ⚡ **超高速度**: 51,000+ 条/秒
- 🔄 **断点续传**: 中断后自动恢复
- 🚀 **多进程并行**: 生产者-消费者模式
- 💾 **UNLOGGED表**: 极速插入，无WAL开销
- 🎯 **分布式支持**: 3台机器并行处理
- 🔐 **100% E盘存储保证**: 所有数据存储在指定位置，不占用C盘或D盘
- 🎯 **智能主键配置**: 不同表自动使用正确的主键字段

---

## 📋 目录

- [快速开始](#快速开始)
- [完整操作流程](#完整操作流程)
- [存储位置保证](#存储位置保证)
- [分布式处理](#分布式处理)
- [主键字段配置](#主键字段配置)
- [性能优化](#性能优化)
- [故障排除](#故障排除)
- [配置文件说明](#配置文件说明)

---

## 🚀 快速开始

### 前提条件

1. PostgreSQL已安装并运行
2. `E:\postgreSQL` 目录存在且有写入权限
3. `E:\2025-09-30` 目录包含S2ORC数据文件
4. Python 3.8+

### 安装依赖

```bash
pip install -r requirements.txt
```

---

## 📖 完整操作流程

### 方案A：首次使用（推荐）

适用于第一次使用或需要重新开始的情况。

#### 步骤1: 完全清理（删除旧数据）

```bash
python scripts/clean_start.py
```

**说明：**
- 删除数据库 `s2orc_d1`
- 删除所有自定义表空间
- 确保干净的起点
- 需要输入 `yes` 确认

**输出示例：**
```
✅ 数据库已删除: s2orc_d1
✅ 已删除表空间: d1_tablespace
✅ 清理完成！
```

#### 步骤2: 验证配置

```bash
python scripts/verify_storage.py
```

**说明：**
- 检查 `E:\postgreSQL` 目录权限
- 验证PostgreSQL连接
- 确认配置正确

**输出示例：**
```
✅ 目录存在
✅ 有写入权限
✅ 验证通过
保证: 数据库 → E:/postgreSQL
```

#### 步骤3: 初始化数据库

```bash
# 根据机器配置初始化
python scripts/init_database.py --init --machine machine3

# 或者初始化所有表
python scripts/init_database.py --init
```

**说明：**
- 创建表空间 `d1_tablespace` → `E:\postgreSQL`
- 创建数据库 `s2orc_d1` → 使用E盘表空间
- 创建表（papers, authors, citations等） → 使用E盘表空间

**输出示例：**
```
✓ 表空间创建成功
✓ 数据库 s2orc_d1 创建成功
数据库存储位置: E:\postgreSQL
✓ 表 papers 创建成功 (TEXT类型)
✓ 表 authors 创建成功 (TEXT类型)
```

#### 步骤4: 批量导入数据

```bash
# 分布式模式（推荐）
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"

# 单个文件夹模式
python scripts/stream_gz_to_db_optimized.py --dir "E:\2025-09-30\papers" --table papers
```

**说明：**
- 自动使用各表的正确主键字段
- 所有数据写入 `E:\postgreSQL`
- 支持断点续传

**输出示例：**
```
主键字段: authorid (authors表)
主键字段: citedcorpusid (citations表)
主键字段: corpusid (其他表)
📊 [1/7] 100% | 1,234,567条 | 3500条/秒
```

#### 步骤5: 最终验证

```bash
python scripts/verify_storage.py
```

**输出示例：**
```
✅ 表空间配置正确: E:\postgreSQL
✅ 数据库使用正确表空间: d1_tablespace
✅ 找到 7 个表
```

### 方案B：继续之前的导入

如果已经初始化过，只是想继续导入数据：

```bash
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"
```

---

## 🔐 存储位置保证

### 100% E盘存储

**保证存储在 `E:\postgreSQL`：**
- ✅ 数据库系统文件 → `E:\postgreSQL\PG_xx_xxxxx\s2orc_d1\`
- ✅ papers表数据 → `E:\postgreSQL\PG_xx_xxxxx\s2orc_d1\papers_xxxxx`
- ✅ authors表数据 → `E:\postgreSQL\PG_xx_xxxxx\s2orc_d1\authors_xxxxx`
- ✅ citations表数据 → `E:\postgreSQL\PG_xx_xxxxx\s2orc_d1\citations_xxxxx`
- ✅ 所有索引 → `E:\postgreSQL\...`
- ✅ WAL日志 → `E:\postgreSQL\...`

**绝不会存储在：**
- ❌ C盘
- ❌ D盘
- ❌ PostgreSQL默认目录 (`D:\person_data\postgresql\data\`)

### 验证数据位置

```bash
# 方法1: 运行验证脚本
python scripts/verify_storage.py

# 方法2: 检查目录
dir E:\postgreSQL
# 应该能看到 PG_18_xxxxxx 等PostgreSQL目录

# 方法3: SQL查询
psql -U postgres -d s2orc_d1 -c "
SELECT d.datname, t.spcname, pg_tablespace_location(t.oid)
FROM pg_database d
LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid
WHERE d.datname = 's2orc_d1';
"
```

---

## 🌐 分布式处理

### 机器分配

三台机器并行处理，大幅提升速度：

| 机器 | 负责的表 | 预估时间 |
|------|---------|---------|
| **Machine 1** | embeddings_specter_v1, s2orc | ~10小时 |
| **Machine 2** | embeddings_specter_v2, s2orc_v2 | ~10小时 |
| **Machine 3** | papers, abstracts, authors, citations, paper_ids, publication_venues, tldrs | ~10小时 |

### 使用方法

```bash
# 在电脑1上运行
python scripts/batch_process_machine.py --machine machine1 --base-dir "E:\2025-09-30"

# 在电脑2上运行
python scripts/batch_process_machine.py --machine machine2 --base-dir "E:\2025-09-30"

# 在电脑3上运行
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"
```

### 性能对比

- **单机处理**: 27-29 小时
- **三机并行**: 9-12 小时 ⚡ **节省 60%**

---

## 🎯 主键字段配置

不同的表使用不同的主键字段，系统会**自动识别**：

| 表名 | 主键字段 | 说明 |
|------|---------|------|
| `authors` | `authorid` | 作者ID |
| `citations` | `citedcorpusid` | 被引用文献ID |
| `papers` | `corpusid` | 论文ID |
| `abstracts` | `corpusid` | 摘要关联论文ID |
| `s2orc` | `corpusid` | S2ORC数据ID |
| 其他表 | `corpusid` | 默认使用corpusid |

**无需手动配置**，导入时会自动使用正确的主键字段。

---

## ⚡ 性能优化

### PostgreSQL配置优化

编辑 `postgresql.conf`：

```ini
# 内存配置
shared_buffers = 4GB
work_mem = 256MB
maintenance_work_mem = 2GB
effective_cache_size = 16GB

# WAL配置
max_wal_size = 10GB
min_wal_size = 2GB
wal_buffers = 32MB
checkpoint_timeout = 30min
checkpoint_completion_target = 0.9

# 并行配置
max_worker_processes = 16
max_parallel_workers_per_gather = 4
max_parallel_workers = 12
```

### 解压进程数优化

根据CPU核心数调整：

```bash
# 4核CPU
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\data" --extractors 4

# 8核CPU
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\data" --extractors 7

# 12核以上
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\data" --extractors 10
```

### 批量大小配置

已针对不同表优化，无需调整：

- **大数据表** (s2orc系列): 2,000条/批
- **中等数据表** (embeddings系列): 10,000条/批
- **小数据表** (papers, authors等): 100,000条/批

---

## 🔧 故障排除

### 常见问题

#### Q1: 如何确认数据真的在E盘？

**A1:**
1. 运行验证脚本: `python scripts/verify_storage.py`
2. 检查目录: `dir E:\postgreSQL`
3. 应该能看到 `PG_18_xxxxxx` 等PostgreSQL目录和大量数据文件

#### Q2: 如果已经有旧数据怎么办？

**A2:**
```bash
# 运行清理脚本
python scripts/clean_start.py
# 输入 yes 确认删除

# 重新初始化
python scripts/init_database.py --init --machine machine3
```

#### Q3: 导入过程中断了怎么办？

**A3:**
直接重新运行相同命令，会自动跳过已完成的文件：
```bash
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"
```

#### Q4: 如何查看进度？

**A4:**
```bash
# 查看进度文件
cat logs/gz_progress.txt

# 查看已导入数据量
psql -U postgres -d s2orc_d1 -c "SELECT COUNT(*) FROM papers;"
```

#### Q5: 磁盘空间不足怎么办？

**A5:**
1. 检查 `E:\postgreSQL` 大小
2. 清理其他文件腾出空间
3. 或修改配置使用其他盘符（需修改 `database/config/db_config_v2.py`）

#### Q6: 内存不足怎么办？

**A6:**
减少并发进程数：
```bash
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\data" --extractors 2
```

### 错误处理流程

如果导入失败：

```bash
# 1. 查看错误信息
# 2. 运行清理
python scripts/clean_start.py

# 3. 重新开始
python scripts/init_database.py --init --machine machine3
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"
```

---

## 📁 配置文件说明

### database/config/db_config_v2.py

核心配置文件：

```python
# 数据库连接配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 's2orc_d1',
    'user': 'postgres',
    'password': 'your_password',
}

# 表空间配置（100%存储位置控制）
TABLESPACE_CONFIG = {
    'enabled': True,
    'name': 'd1_tablespace',      # 表空间名称
    'location': 'E:\\postgreSQL',  # ← 存储位置（E盘）
}

# 主键字段映射
TABLE_PRIMARY_KEY_MAP = {
    'authors': 'authorid',           # authors表使用authorid
    'citations': 'citedcorpusid',    # citations表使用citedcorpusid
    # 其他表默认使用corpusid
}
```

### machine_config.py

机器分配配置：

```python
MACHINE_CONFIGS = {
    'machine1': {
        'folders': ['embeddings-specter_v1', 's2orc'],
        'tables': ['embeddings_specter_v1', 's2orc'],
    },
    'machine2': {
        'folders': ['embeddings-specter_v2', 's2orc_v2'],
        'tables': ['embeddings_specter_v2', 's2orc_v2'],
    },
    'machine3': {
        'folders': ['abstracts', 'authors', 'citations', 'paper_ids', 'papers', 'publication_venues', 'tldrs'],
        'tables': ['abstracts', 'authors', 'citations', 'paper_ids', 'papers', 'publication_venues', 'tldrs'],
    }
}
```

---

## 📂 项目结构

```
gz_filed_update/
├── database/
│   └── config/
│       └── db_config_v2.py       # 数据库和表空间配置
├── scripts/
│   ├── clean_start.py            # 完全清理脚本
│   ├── verify_storage.py         # 验证存储位置
│   ├── init_database.py          # 初始化数据库
│   ├── stream_gz_to_db_optimized.py  # 单文件夹处理
│   ├── batch_process_machine.py  # 批量处理
│   └── finalize_database.py      # 最终化
├── logs/
│   └── gz_progress.txt           # 进度记录
├── machine_config.py             # 机器分配配置
├── requirements.txt              # Python依赖
└── README.md                     # 本文档
```

---

## 🎯 核心脚本说明

| 脚本 | 功能 | 使用时机 |
|------|------|---------|
| `clean_start.py` | 删除所有旧数据库和表空间 | 首次使用或重新开始 |
| `verify_storage.py` | 验证存储位置配置 | 任何时候都可以验证 |
| `init_database.py` | 初始化数据库和表 | 清理后或首次使用 |
| `batch_process_machine.py` | 批量处理多个文件夹 | 分布式自动化导入 |
| `stream_gz_to_db_optimized.py` | 处理单个文件夹 | 单独处理某个表 |
| `finalize_database.py` | 最终化数据库 | 所有数据导入完成后 |

---

## 📊 监控进度

### 查看进度文件

```bash
# Windows
type logs\gz_progress.txt

# 查看最后10个完成的文件
tail -10 logs/gz_progress.txt
```

### 查询数据库

```bash
# 查看各表数据量
psql -U postgres -d s2orc_d1 -c "
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
    n_live_tup AS row_count
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"

# 查看表空间使用情况
psql -U postgres -c "
SELECT 
    spcname,
    pg_size_pretty(pg_tablespace_size(spcname)) AS size,
    pg_tablespace_location(oid) AS location
FROM pg_tablespace
WHERE spcname = 'd1_tablespace';
"
```

---

## ✅ 完成标志

当看到以下输出时，表示导入成功：

```
✅ 所有文件已处理完成！
✅ 插入完成: XXX,XXX,XXX条
✅ 验证通过
✅ 数据库 → E:/postgreSQL
✅ 所有表 → E:/postgreSQL
```

此时可以检查 `E:\postgreSQL` 目录，应该有大量数据文件。

---

## 🔒 系统要求

- **Python**: 3.8+
- **PostgreSQL**: 12+
- **内存**: 8GB+ 推荐
- **CPU**: 多核推荐（用于并行解压）
- **磁盘**: E盘需有足够空间（建议预留500GB+）

---

## 📝 许可证

MIT License

---

## 🤝 贡献

欢迎提交Issue和Pull Request！

---

## 📮 联系方式

如有问题，请提交Issue。
