# Batch Update 使用说明

## 可用机器
- `machine0`: s2orc_d0:5430
- `machine2`: s2orc_d2:5432

## 数据集类型
- `s2orc` / `s2orc_v2` → content字段
- `embeddings_specter_v1` → specter_v1字段
- `embeddings_specter_v2` → specter_v2字段

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

## 2. 导入数据

```bash
# 自动流水线（导入→建索引→更新JSONL）
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine0 --auto-pipeline

# 手动导入
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine0

# 删除gz文件
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine0 --delete-gz
```

---

## 3. 创建索引

```bash
python batch_update/init_temp_table.py --machine machine0 --create-indexes
```

---

## 4. 更新JSONL文件

```bash
# 单机器模式（自动处理所有字段）
python batch_update/jsonl_batch_updater.py --machine machine0

# 多机器协同模式（局域网合并数据）
python batch_update/jsonl_batch_updater.py --machines machine0,machine2
```

---

## 5. 清空临时表

```bash
# 清空表数据（保留导入记录）
python batch_update/init_temp_table.py --machine machine0 --truncate

# 清空导入记录（重新处理所有文件）
python batch_update/init_temp_table.py --machine machine0 --clear-log
```

---

## 工作流

### 快速流程（自动流水线）

```bash
# 1. 创建表（首次）
python batch_update/init_temp_table.py --machine machine0
python batch_update/init_temp_table.py --machine machine0 --init-log-table

# 2. 一键完成
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine0 --auto-pipeline

# 3. 清空
python batch_update/init_temp_table.py --machine machine0 --truncate
```

### 手动流程

```bash
# 1. 创建表（首次）
python batch_update/init_temp_table.py --machine machine0
python batch_update/init_temp_table.py --machine machine0 --init-log-table

# 2. 导入多个数据集
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine0
python batch_update/import_gz_to_temp.py D:\gz_temp\embeddings_v1 --dataset embeddings_specter_v1 --machine machine0
python batch_update/import_gz_to_temp.py D:\gz_temp\embeddings_v2 --dataset embeddings_specter_v2 --machine machine0

# 3. 创建索引
python batch_update/init_temp_table.py --machine machine0 --create-indexes

# 4. 更新JSONL
python batch_update/jsonl_batch_updater.py --machine machine0

# 5. 清空
python batch_update/init_temp_table.py --machine machine0 --truncate
```

### 多机器协同

```bash
# machine0
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine0
python batch_update/import_gz_to_temp.py D:\gz_temp\embeddings_v1 --dataset embeddings_specter_v1 --machine machine0
python batch_update/init_temp_table.py --machine machine0 --create-indexes

# machine2
python batch_update/import_gz_to_temp.py D:\gz_temp\embeddings_v2 --dataset embeddings_specter_v2 --machine machine2
python batch_update/init_temp_table.py --machine machine2 --create-indexes

# 协同更新（在machine0执行）
python batch_update/jsonl_batch_updater.py --machines machine0,machine2

# 清空
python batch_update/init_temp_table.py --machine machine0 --truncate
python batch_update/init_temp_table.py --machine machine2 --truncate
```
