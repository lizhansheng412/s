# S2ORC 数据库导入工具

高性能并行处理工具，用于将S2ORC数据集的GZ文件导入PostgreSQL数据库。

## 核心特性

- ⚡ **超高速度**: 51,000+ 条/秒
- 🔄 **断点续传**: 中断后自动恢复
- 🚀 **多进程并行**: 生产者-消费者模式
- 💾 **UNLOGGED表**: 极速插入，无WAL开销
- 🎯 **分布式支持**: 3台机器并行处理

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置数据库

编辑 `database/config/db_config_v2.py`：

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 's2orc',
    'user': 'your_username',
    'password': 'your_password'
}
```

### 3. 初始化数据库

```bash
# 单机模式：创建所有表
python scripts/init_database.py --init

# 分布式模式：只创建该机器需要的表
python scripts/init_database.py --init --machine machine1
```

### 4. 开始处理

**单机模式（处理单个文件夹）：**
```bash
python scripts/stream_gz_to_db_optimized.py --dir "E:\S2ORC_Data\papers" --table papers
```

**分布式模式（自动处理该机器的所有文件夹）：**
```bash
python scripts/batch_process_machine.py --machine machine1 --base-dir "E:\S2ORC_Data"
```

## 分布式处理方案

详见 [DISTRIBUTED_PROCESSING_GUIDE.md](DISTRIBUTED_PROCESSING_GUIDE.md)

### 机器分配

- **电脑1**: `embeddings_specter_v1`, `s2orc`
- **电脑2**: `embeddings_specter_v2`, `s2orc_v2`
- **电脑3**: `abstracts`, `authors`, `citations`, `paper_ids`, `papers`, `publication_venues`, `tldrs`

### 并行加速

- **单机时间**: 27-29 小时
- **3机并行**: 9-12 小时 ⚡ **节省 60%**

## 核心脚本

| 脚本 | 功能 | 使用场景 |
|------|------|---------|
| `init_database.py` | 初始化数据库和表 | 首次运行前执行 |
| `stream_gz_to_db_optimized.py` | 处理单个文件夹 | 单机或手动控制 |
| `batch_process_machine.py` | 批量处理多个文件夹 | 分布式自动化 |
| `finalize_database.py` | 最终化数据库 | 所有数据导入完成后 |

## 性能优化

### 建议配置

**PostgreSQL优化（postgresql.conf）：**
```ini
shared_buffers = 4GB
work_mem = 256MB
maintenance_work_mem = 2GB
max_wal_size = 10GB
checkpoint_timeout = 30min
```

**解压进程数：**
- 4核CPU: 3-4个进程
- 8核CPU: 6-8个进程
- 12核+: 8-10个进程

```bash
# 自定义进程数
python scripts/batch_process_machine.py --machine machine1 --base-dir "E:\data" --extractors 10
```

## 监控进度

```bash
# 查看进度文件
cat logs/gz_progress.txt

# 查看数据库数据量
psql -U user -d s2orc -c "SELECT COUNT(*) FROM papers;"
```

## 故障恢复

脚本自动保存进度，中断后直接重新运行即可：

```bash
# 自动跳过已完成的文件
python scripts/batch_process_machine.py --machine machine1 --base-dir "E:\data"
```

## 系统要求

- **Python**: 3.8+
- **PostgreSQL**: 12+
- **内存**: 8GB+ 推荐
- **CPU**: 多核推荐（用于并行解压）

## 项目结构

```
gz_filed_update/
├── machine_config.py           # 机器配置
├── config.py                   # 通用配置
├── database/                   # 数据库配置和Schema
│   └── config/
│       └── db_config_v2.py
├── scripts/                    # 核心脚本
│   ├── init_database.py        # 初始化数据库
│   ├── stream_gz_to_db_optimized.py  # 单文件夹处理
│   ├── batch_process_machine.py      # 批量处理
│   └── finalize_database.py          # 最终化
├── logs/                       # 日志和进度
│   └── gz_progress.txt
└── DISTRIBUTED_PROCESSING_GUIDE.md   # 分布式处理详细指南
```

## 常见问题

**Q: 如何暂停处理？**  
A: 按 `Ctrl+C`，进度自动保存

**Q: 处理失败如何恢复？**  
A: 直接重新运行，自动跳过已完成文件

**Q: 如何查看已处理多少数据？**  
A: 查看 `logs/gz_progress.txt` 或直接查询数据库

**Q: 内存不足怎么办？**  
A: 减少 `--extractors` 参数值

## 许可证

MIT License
