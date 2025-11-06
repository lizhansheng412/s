# Batch Update 使用说明

所有命令必须通过 `--machine` 参数指定目标机器。

## 可用机器
- `machine0`: s2orc_d0:5430
- `machine2`: s2orc_d2:5432

---

## 1. 首次使用：创建表

```bash
# machine0
python batch_update/init_temp_table.py --machine machine0
python batch_update/init_temp_table.py --machine machine0 --init-log-table

# machine2
python batch_update/init_temp_table.py --machine machine2
python batch_update/init_temp_table.py --machine machine2 --init-log-table
```

---

## 2. 导入数据（必须指定数据集类型）

### 2.1 自动流水线模式（推荐）

一键执行：导入 → 建索引 → 更新JSONL

```bash
# machine0
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine0 --auto-pipeline

# machine2（自动流水线 + 删除gz文件）
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine2 --auto-pipeline --delete-gz
```

### 2.2 手动分步模式

```bash
# machine0
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine0

# machine2（导入并删除gz文件）
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine2 --delete-gz
```

---

## 3. 导入完成后创建索引

```bash
# machine0
python batch_update/init_temp_table.py --machine machine0 --create-indexes

# machine2
python batch_update/init_temp_table.py --machine machine2 --create-indexes
```

---

## 4. 更新 JSONL 文件（必须指定数据集类型）

```bash
# machine0
python batch_update/jsonl_batch_updater.py --machine machine0 --dataset s2orc

# machine2
python batch_update/jsonl_batch_updater.py --machine machine2 --dataset s2orc
```

---

## 5. 清空临时表但保留导入记录

```bash
# machine0
python batch_update/init_temp_table.py --machine machine0 --truncate

# machine2
python batch_update/init_temp_table.py --machine machine2 --truncate
```

---

## 6. 重新处理所有文件（清空导入记录）

```bash
# machine0
python batch_update/init_temp_table.py --machine machine0 --clear-log

# machine2
python batch_update/init_temp_table.py --machine machine2 --clear-log
```

---

## 支持的数据集类型

- `s2orc`: S2ORC 数据集（更新 content 字段）
- `s2orc_v2`: S2ORC v2 数据集（更新 content 字段）
- `embeddings_specter_v1`: SPECTER v1 嵌入向量（更新 embedding 字段）
- `embeddings_specter_v2`: SPECTER v2 嵌入向量（更新 embedding 字段）
- `citations`: 引用数据（逻辑待实现）

---

## 常用工作流

### 快速开始（自动流水线模式，推荐）

```bash
# 首次使用：创建表（只需执行一次）
python batch_update/init_temp_table.py --machine machine0
python batch_update/init_temp_table.py --machine machine0 --init-log-table

# 一键完成：导入 → 建索引 → 更新JSONL
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine0 --auto-pipeline

# 清空临时表（准备下次导入）
python batch_update/init_temp_table.py --machine machine0 --truncate
```

### 完整流程示例（手动分步模式）

```bash
# 1. 创建表（只需执行一次）
python batch_update/init_temp_table.py --machine machine0
python batch_update/init_temp_table.py --machine machine0 --init-log-table

# 2. 导入数据
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine0

# 3. 创建索引
python batch_update/init_temp_table.py --machine machine0 --create-indexes

# 4. 更新JSONL
python batch_update/jsonl_batch_updater.py --machine machine0 --dataset s2orc

# 5. 清空临时表（保留记录）
python batch_update/init_temp_table.py --machine machine0 --truncate
```

### 双机并行处理

```bash
# Terminal 1 - machine0
python batch_update/import_gz_to_temp.py D:\gz_temp\batch1 --dataset s2orc --machine machine0 --auto-pipeline

# Terminal 2 - machine2
python batch_update/import_gz_to_temp.py D:\gz_temp\batch2 --dataset s2orc --machine machine2 --auto-pipeline
```

### 不同数据集类型示例

```bash
# 导入并更新 SPECTER v1 嵌入向量 (machine0)
python batch_update/import_gz_to_temp.py D:\gz_temp\embeddings --dataset embeddings_specter_v1 --machine machine0 --auto-pipeline

# 导入并更新 SPECTER v2 嵌入向量 (machine0)
python batch_update/import_gz_to_temp.py D:\gz_temp\embeddings --dataset embeddings_specter_v2 --machine machine0 --auto-pipeline
```

---

## 注意事项

1. **必需参数**: 
   - 所有命令必须指定 `--machine` 参数（machine0 或 machine2）
   - 导入命令必须指定 `--dataset` 参数
   - 更新JSONL必须指定 `--machine` 和 `--dataset` 参数
2. **删除文件**: 使用 `--delete-gz` 需谨慎，确保数据已成功导入
3. **索引创建**: 大量数据导入后再创建索引，提升导入速度
4. **自动流水线**: 使用 `--auto-pipeline` 可一键完成导入→建索引→更新JSONL的全流程，推荐使用
5. **数据安全**: 
   - s2orc/s2orc_v2: 只更新 `content: {}` 为空的记录
   - embeddings: 只更新 `embedding: {}` 为空的记录
