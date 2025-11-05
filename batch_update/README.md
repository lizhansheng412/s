# Batch Update 使用说明

所有命令默认操作 `machine0`，可通过 `--machine` 参数指定目标机器。

## 可用机器
- `machine0`: s2orc_d0:5430 (默认)
- `machine1`: s2orc_d1:5431
- `machine2`: s2orc_d2:5432
- `machine3`: s2orc_d3:5433

---

## 1. 首次使用：创建表

```bash
# 在 machine0 (默认)
python batch_update/init_temp_table.py
python batch_update/init_temp_table.py --init-log-table

# 在 machine1
python batch_update/init_temp_table.py --machine machine1
python batch_update/init_temp_table.py --machine machine1 --init-log-table

# 在 machine2
python batch_update/init_temp_table.py --machine machine2
python batch_update/init_temp_table.py --machine machine2 --init-log-table
```

---

## 2. 导入数据（必须指定数据集类型）

```bash
# 导入到 machine0 (默认)
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc

# 导入到 machine1
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine1

# 导入到 machine2 并删除gz文件
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine2 --delete-gz
```

---

## 3. 导入完成后创建索引

```bash
# machine0
python batch_update/init_temp_table.py --create-indexes

# machine1
python batch_update/init_temp_table.py --machine machine1 --create-indexes

# machine2
python batch_update/init_temp_table.py --machine machine2 --create-indexes
```

---

## 4. 更新 JSONL 文件

```bash
# 更新 machine0 的数据
python batch_update/jsonl_batch_updater.py

# 更新 machine1 的数据
python batch_update/jsonl_batch_updater.py --machine machine1

# 更新 machine2 的数据
python batch_update/jsonl_batch_updater.py --machine machine2
```

---

## 5. 清空临时表但保留导入记录

```bash
# machine0
python batch_update/init_temp_table.py --truncate

# machine1
python batch_update/init_temp_table.py --machine machine1 --truncate
```

---

## 6. 重新处理所有文件（清空导入记录）

```bash
# machine0
python batch_update/init_temp_table.py --clear-log

# machine1
python batch_update/init_temp_table.py --machine machine1 --clear-log
```

---

## 支持的数据集类型

- `s2orc`
- `embeddings_specter_v1`
- `embeddings_specter_v2`
- `citations`

---

## 常用工作流

### 完整流程示例（machine1）

```bash
# 1. 创建表
python batch_update/init_temp_table.py --machine machine1
python batch_update/init_temp_table.py --machine machine1 --init-log-table

# 2. 导入数据
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --machine machine1

# 3. 创建索引
python batch_update/init_temp_table.py --machine machine1 --create-indexes

# 4. 更新JSONL
python batch_update/jsonl_batch_updater.py --machine machine1

# 5. 清空临时表（保留记录）
python batch_update/init_temp_table.py --machine machine1 --truncate
```

### 多机器并行处理

```bash
# Terminal 1 - machine0
python batch_update/import_gz_to_temp.py D:\gz_temp\batch1 --dataset s2orc --machine machine0

# Terminal 2 - machine1
python batch_update/import_gz_to_temp.py D:\gz_temp\batch2 --dataset s2orc --machine machine1

# Terminal 3 - machine2
python batch_update/import_gz_to_temp.py D:\gz_temp\batch3 --dataset s2orc --machine machine2
```

---

## 注意事项

1. **默认机器**: 所有命令默认操作 `machine0`，除非指定 `--machine`
2. **数据集类型**: 导入命令必须指定 `--dataset` 参数
3. **删除文件**: 使用 `--delete-gz` 需谨慎，确保数据已成功导入
4. **索引创建**: 大量数据导入后再创建索引，提升导入速度
