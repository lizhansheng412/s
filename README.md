# S2ORC 数据库导入工具

高性能并行处理工具，用于将S2ORC数据集的GZ文件导入PostgreSQL数据库。

## ✨ 核心特性

- ⚡ **超高速度**: 2000-4500条/秒（优化后提升2.5-4倍）
- 🔄 **断点续传**: 中断后自动恢复
- 💾 **TURBO模式**: 可选极速插入（临时禁用WAL）
- 🔐 **100% E盘存储**: 所有数据存储在 `E:\postgreSQL`
- 🎯 **智能主键**: 不同表自动使用正确的主键字段
- 🛡️ **内存安全**: 仅使用15-17GB（32GB系统安全）

---

## 🚀 快速开始

### 📋 必备命令（按顺序执行）

```powershell
# 1. 安装依赖
pip install -r requirements.txt

# 2. 验证配置
python scripts/verify_storage.py

# 3. 初始化数据库
python scripts/init_database.py --init --machine machine3

# 4. 开始导入（推荐）
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"

# 4. 开始导入（TURBO模式，极速但有风险）
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30" --turbo
```

### 🔄 中断后继续

```powershell
# 直接运行相同命令即可（自动跳过已完成文件）
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"
```

### 🗑️ 重新开始

```powershell
# 1. 清理旧数据
python scripts/clean_start.py

# 2. 重新初始化
python scripts/init_database.py --init --machine machine3

# 3. 重新导入
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"
```

---

## 📊 性能优化

### 优化效果对比

| 配置 | 速度 | 完成时间 | 单次commit | 内存使用 |
|------|------|---------|-----------|---------|
| **原始配置** | 827条/秒 | 26小时+ | 480MB | ~6GB |
| **优化配置** | 2000-3000条/秒 | 8-12小时 | 1.6GB | ~8-11GB |
| **TURBO模式** | 3000-4500条/秒 | 6-9小时 | 1.6GB | ~8-11GB |

**提升效果**: 速度提升 **2.5-4倍**，时间节省 **60-70%**

### 各表优化配置（自动应用，彻底避免缓冲区问题）

| 表名 | batch_size | commit_batches | extractors | 每次commit | 重复处理 |
|------|-----------|---------------|-----------|-----------|---------|
| embeddings_specter_v1/v2 | 15,000 | 3 | 6 | 720MB | VALUES |
| s2orc/s2orc_v2 | 2,000 | 3 | 6 | 600MB | VALUES |
| citations | 8,000 | 2 | 6 | 160MB | VALUES |
| papers/abstracts/等 | 25,000 | 3 | 7 | 375MB | VALUES |

**关键优化**：
- ✅ 所有表都使用 **VALUES 方式**处理重复键，不使用临时表
- ✅ 减小 batch_size 和 commit_batches，频繁释放内存
- ✅ 彻底避免"没有可用的本地缓冲区"错误

### TURBO模式说明

**启用方式**：添加 `--turbo` 参数

```powershell
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30" --turbo
```

**说明**：
- ✅ 速度提升30-50%（3000-4500条/秒）
- ⚠️ 临时禁用WAL日志（表设为UNLOGGED）
- ⚠️ 数据库崩溃可能丢失正在导入的数据
- ✅ 完成后自动恢复为LOGGED模式
- 💡 建议：仅在初次批量导入时使用

---

## 🌐 分布式处理（3台机器并行）

### 机器分配

| 机器 | 负责的表 | 预估时间 |
|------|---------|---------|
| **Machine 1** | embeddings_specter_v1, s2orc | 8-12小时 |
| **Machine 2** | embeddings_specter_v2, s2orc_v2 | 8-12小时 |
| **Machine 3** | papers, abstracts, authors, citations, paper_ids, publication_venues, tldrs | 8-12小时 |

### 使用方法

```powershell
# 在电脑1上运行
python scripts/batch_process_machine.py --machine machine1 --base-dir "E:\2025-09-30"

# 在电脑2上运行
python scripts/batch_process_machine.py --machine machine2 --base-dir "E:\2025-09-30"

# 在电脑3上运行
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"
```

---

## 🎯 主键字段配置

系统自动识别，无需手动配置：

| 表名 | 主键字段 |
|------|---------|
| `authors` | `authorid` |
| `citations` | `citedcorpusid` |
| 其他表 | `corpusid` |

---

## 🔐 存储位置保证

**100% 存储在 `E:\postgreSQL`**

- ✅ 数据库文件 → `E:\postgreSQL\PG_xx_xxxxx\s2orc_d1\`
- ✅ 所有表数据 → `E:\postgreSQL\...`
- ✅ 所有索引 → `E:\postgreSQL\...`
- ❌ 绝不会存储在 C盘或D盘

**验证命令**：
```powershell
python scripts/verify_storage.py
```

---

## 🔧 常用命令集合

### 监控进度

```powershell
# 查看进度文件
type logs\gz_progress.txt

# 查看数据量
psql -U postgres -d s2orc_d1 -c "SELECT COUNT(*) FROM papers;"

# 查看表空间使用
psql -U postgres -c "SELECT spcname, pg_size_pretty(pg_tablespace_size(spcname)) AS size, pg_tablespace_location(oid) AS location FROM pg_tablespace WHERE spcname = 'd1_tablespace';"
```

### 处理单个文件夹

```powershell
# 处理特定文件夹
python scripts/stream_gz_to_db_optimized.py --dir "E:\2025-09-30\papers" --table papers

# 使用TURBO模式
python scripts/stream_gz_to_db_optimized.py --dir "E:\2025-09-30\papers" --table papers --turbo

# 自定义进程数
python scripts/stream_gz_to_db_optimized.py --dir "E:\2025-09-30\papers" --table papers --extractors 12
```

---

## 🔧 故障排除

### 常见问题

**Q: 导入中断了怎么办？**
```powershell
# 直接重新运行相同命令（自动续传）
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"
```

**Q: 如何重新开始？**
```powershell
python scripts/clean_start.py
python scripts/init_database.py --init --machine machine3
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30"
```

**Q: 内存不足怎么办？**
```powershell
# 减少并发进程数
python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\2025-09-30" --extractors 4
```

**Q: 如何验证数据在E盘？**
```powershell
python scripts/verify_storage.py
dir E:\postgreSQL
```

---

## ⚙️ 高级配置（可选）

### PostgreSQL配置优化

如需进一步提升性能，编辑 `postgresql.conf`：

```ini
# 内存配置（32GB系统推荐）
shared_buffers = 8GB
effective_cache_size = 24GB
maintenance_work_mem = 4GB
work_mem = 2GB

# WAL配置（关键）
max_wal_size = 16GB
min_wal_size = 4GB
checkpoint_timeout = 30min

# IO优化（SSD）
random_page_cost = 1.1
effective_io_concurrency = 200
```

修改后重启PostgreSQL：
```powershell
Restart-Service postgresql-x64-14
```

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

## 📂 项目结构

```
gz_filed_update/
├── scripts/
│   ├── batch_process_machine.py      # 批量处理（推荐）
│   ├── stream_gz_to_db_optimized.py  # 单文件夹处理
│   ├── init_database.py              # 初始化数据库
│   ├── verify_storage.py             # 验证存储位置
│   └── clean_start.py                # 清理旧数据
├── database/config/
│   └── db_config_v2.py               # 配置文件
├── machine_config.py                 # 机器分配
└── logs/
    └── gz_progress.txt               # 进度记录
```

---

## 🔒 系统要求

- **Python**: 3.8+
- **PostgreSQL**: 12+
- **内存**: 16GB+ 推荐（优化配置下使用15-17GB）
- **CPU**: 多核推荐（用于并行解压）
- **磁盘**: E盘需有足够空间（建议500GB+）

---

## 📝 许可证

MIT License
