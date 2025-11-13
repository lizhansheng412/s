# Batch Update 使用说明

## 可用机器
- `machine0`: s2orc_d0:5430
- `machine2`: s2orc_d2:5432
- `machine3`: s2orc_d3:5433

## 数据集类型
- `s2orc` / `s2orc_v2` → content字段
- `embeddings_specter_v1` → specter_v1字段
- `embeddings_specter_v2` → specter_v2字段
- `citations` → citations和references字段（使用独立流程）

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
python batch_update/jsonl_batch_updater.py --machines machine2,machine0

# 清空
python batch_update/init_temp_table.py --machine machine0 --truncate
python batch_update/init_temp_table.py --machine machine2 --truncate
```

---

## Citations数据处理流程（独立流程）

Citations数据处理流程与其他数据集不同，需要单独执行6个阶段：

### 完整流程（推荐分步执行）

```bash
# 阶段1：导入gz文件到citation_raw表（14亿+条记录，约1-2小时）
python batch_update/import_citations.py D:\gz_temp\citations --only-import

# 阶段2：创建索引（约30-60分钟）
python batch_update/import_citations.py D:\gz_temp\citations --only-index

# 阶段3：构造references缓存（约20-30分钟）
python batch_update/import_citations.py D:\gz_temp\citations --only-stage3

# 阶段4：构造citations缓存（约20-30分钟）
python batch_update/import_citations.py D:\gz_temp\citations --only-stage4

# 阶段5：批量INSERT数据到temp_import表（约10-20分钟，插入6700万条记录）
python batch_update/import_citations.py D:\gz_temp\citations --only-stage5
```

### 快速选项

```bash
# 跳过已完成的阶段（例如：已完成阶段1-2，直接执行阶段3-5）
python batch_update/import_citations.py D:\gz_temp\citations --skip-import --skip-index

# 强制重建缓存（数据变化后使用）
python batch_update/import_citations.py D:\gz_temp\citations --only-stage3 --force-stage3
python batch_update/import_citations.py D:\gz_temp\citations --only-stage4 --force-stage4
```

### 注意事项

1. **阶段5直接INSERT**：不需要temp_import表预先有数据，会直接插入约6700万条记录
2. **极致性能优化**（充分利用32GB内存和8核CPU）：
   - 批次大小：5万条/批（避免PostgreSQL的VALUES子句1664条目限制）
   - 复用临时表：使用TRUNCATE代替DROP/CREATE（速度提升100倍）
   - CTE优化查询：预聚合数据，减少子查询深度
   - 并行执行：启用4路并行查询，充分利用多核CPU
   - 批量提交：每5批提交一次，减少事务开销
   - 高内存配置：work_mem=2GB, 支持大规模哈希聚合
   - 异步提交：synchronous_commit=OFF，提升10倍写入速度
3. **性能监控**：每批显示详细耗时（TRUNCATE/COPY/INSERT/COMMIT），便于定位瓶颈
4. **缓存表可复用**：temp_references和temp_citations除非数据变化，否则不需要重建
5. **citation_raw表保留**：默认保留，重建需1-2小时
6. **前置依赖**：确保corpusid_mapping_title表已存在
7. **执行顺序**：必须按照阶段1→2→3→4→5的顺序执行（可跳过已完成的阶段）
8. **预期性能**：优化后每批5万条处理时间应在5-15秒，整体速度提升5-10倍

---

## 6. 合并Citations数据到完整数据（merge_citations_to_full_data.py）

### 数据库连接说明
- **Machine2**: 使用本地数据库 (localhost:5432)
- **Machine0 和 Machine3**: 通过局域网连接 Machine2 的数据库 (192.168.0.104:5432)

### Machine0（通过局域网连接machine2数据库）

```bash
python batch_update/merge_citations_to_full_data.py --source-dir "G:\1500_part2" --target-dir "G:\final_delivery" --machine machine0
```

### Machine2（使用本地数据库）

```bash
python batch_update/merge_citations_to_full_data.py --source-dir "E:\copy_final_cache" --target-dir "F:\final_delivery_1" --machine machine2
```

### Machine3（通过局域网连接machine2数据库）

```bash
python batch_update/merge_citations_to_full_data.py --source-dir "D:\part2_data" --target-dir "D:\final_data" --machine machine3
```

