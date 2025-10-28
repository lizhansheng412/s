# 大数据集 corpusid 提取脚本

提取 4 个大数据集的 corpusid 到 `corpus_bigdataset` 表（并集）

## 📋 处理流程

### 1️⃣ 初始化表
```bash
python scripts/test/init_mapping_table.py
```
创建 `corpus_bigdataset` 表（无主键模式，极速导入）

### 2️⃣ 批量导入数据

**标准模式（SSD/内置硬盘）：**
```bash
python scripts/test/batch_process_machine_mapping_test.py \
  --machine machine1 \
  --base-dir /path/to/data
```

**USB 硬盘模式（推荐）：**
```bash
python scripts/test/batch_process_machine_mapping_test.py \
  --machine machine1 \
  --base-dir /path/to/data \
  --usb-mode
```

**支持的参数：**
- `--machine`: 机器配置ID（machine1 或 machine2）
- `--base-dir`: 数据根目录
- `--extractors`: 解压进程数（默认: 4，USB模式自动降为1）
- `--usb-mode`: **USB 硬盘模式**（单进程顺序读取，避免磁头寻道）
- `--no-resume`: 禁用断点续传

### 3️⃣ 去重并添加主键
```bash
python scripts/test/init_mapping_table.py --add-pk
```
使用 `SELECT DISTINCT` 快速去重，然后添加主键

## 📁 日志文件

- **进度记录**: `logs/mapping_progress/{dataset}_progress.txt`
- **失败记录**: `logs/mapping_failed/{dataset}_failed.txt`

独立的日志目录，不与主脚本冲突。

## 🎯 设计目标

- **极速导入**: 无约束批量插入 → 一次性建索引
- **智能并发**: 根据存储类型自动调整（SSD: 4进程 / USB HDD: 1进程）
- **超大事务**: 300 万条/次提交（减少磁盘 I/O）
- **断点续传**: 支持中断后继续处理

## ⚡ 性能建议

### USB 机械硬盘（HDD）
```bash
# 推荐：--usb-mode
# 原因：减少并发，避免磁头频繁寻道
python ... --usb-mode
```

**性能对比：**
- ❌ 4进程并发：磁头在4个文件间来回跳跃，~20-40 MB/s
- ✅ 1进程模式：减少磁头寻道开销，~80-120 MB/s
- **提升：2-3倍**

### SSD 或内置硬盘
```bash
# 推荐：默认 4 进程
# 原因：SSD 无磁头，随机访问性能好
python ... --extractors 4
```

### 性能瓶颈识别
- **USB HDD**: 看到 CPU 空闲但处理慢 → 开启 `--usb-mode`
- **SSD**: CPU 占用低 → 可增加 `--extractors` 到 6-8
- **网络盘**: 带宽限制 → 降低并发数

## 📊 支持的数据集

- `embeddings_specter_v1`
- `embeddings_specter_v2`
- `s2orc`
- `s2orc_v2`

所有数据集的 corpusid 将合并到同一张表，最后去重得到并集。

