# Final Delivery - 数据提取与导出

## 📁 核心脚本

| 脚本 | 功能 | 用途 |
|------|------|------|
| `init_table.py` | 初始化/去重表 | 创建表、去重、建主键 |
| `extract_corpusid.py` | 提取 corpusid | 从 gz 文件提取数据 |
| `rebuild_sorted_table_v2.py` | 重建排序表 | 按 corpusid 排序优化查询 |
| `export_final_delivery.py` | 导出数据 | 导出到 JSONL 文件 |

## 🚀 完整工作流程

### 步骤1：创建表并提取数据
```bash
# 1. 创建表
python scripts/all_corpusid_of_5dataset/init_table.py

# 2. 提取数据（单个文件夹）
python scripts/all_corpusid_of_5dataset/extract_corpusid.py \
  --dir "E:\machine_win01\2025-09-30\s2orc"

# 3. 去重并建主键
python scripts/all_corpusid_of_5dataset/init_table.py --finalize
```

### 步骤2：重建排序表（性能优化）
```bash
# 按 corpusid 排序重建表（提升查询性能 5-10倍）
python scripts/all_corpusid_of_5dataset/rebuild_sorted_table_v2.py --yes
```

### 步骤3：导出数据
```bash
# 导出到 E:\final_delivery
python scripts/all_corpusid_of_5dataset/export_final_delivery.py
```

## 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--extractors` | 提取进程数（USB硬盘建议1） | 1 |
| `--inserters` | 插入进程数（SSD建议4-6） | 4 |
| `--no-resume` | 禁用断点续传 | 默认启用 |
| `--reset` | 重置进度 | - |

## ⚙️ 性能配置

### 提取数据 (extract_corpusid.py)
- **批次大小**: 100万条/批
- **提取进程**: 1个（USB硬盘，避免随机访问）
- **插入进程**: 1个（默认）可用 `--inserters` 调整

### 导出数据 (export_final_delivery.py)
- **批次大小**: 5万条/批
- **Worker进程**: 4个（并行查询）
- **输出目录**: E:\final_delivery
- **文件命名**: 8位UUID.jsonl

## 日志文件

- **进度日志**: `logs/final_delivery_progress/<文件夹名>_progress.txt`
- **失败日志**: `logs/final_delivery_failed/<文件夹名>_failed.txt`

## 处理流程

```
创建表(无约束)
    ↓
单文件夹处理 ←→ 批量处理多个文件夹
    ↓
去重 + 建主键
```

## 📖 完整示例

### 提取 corpusid（批量处理）
```bash
python scripts/all_corpusid_of_5dataset/extract_corpusid.py \
  --dirs "E:\data\s2orc" "E:\data\citations" "E:\data\papers"
```

### 自定义进程数
```bash
python scripts/all_corpusid_of_5dataset/extract_corpusid.py \
  --dir "E:\data\s2orc" \
  --inserters 4
```

## 示例输出

```
⏰ 开始时间: 2025-10-30 14:23:15
📊 总文件数: 1250

📊 进度:125/1250 (10.0%) | ✅成功:123 ❌失败:2 | ⏱️已用:00:15:30 预计剩余:02:18:45

======================================================================
✅ [s2orc] 处理完成
======================================================================
⏰ 结束时间: 2025-10-30 16:50:00
📊 处理统计:
   - 成功文件: 1,248
   - 失败文件: 2
   - 插入记录: 125,340,567 条
⏱️  性能统计:
   - 总耗时: 02:26:45
   - 插入速度: 14,234 条/秒
   - 平均每文件: 7.1 秒
======================================================================
```

## 📊 表结构

```sql
-- 初始状态（无约束）
CREATE TABLE final_delivery (
    corpusid BIGINT NOT NULL
);

-- finalize 后（带主键）
CREATE TABLE final_delivery (
    corpusid BIGINT PRIMARY KEY
);

-- rebuild_sorted_table_v2 后（优化版）
CREATE TABLE final_delivery (
    id BIGSERIAL PRIMARY KEY,        -- 自增ID（1,2,3...）
    corpusid BIGINT NOT NULL,        -- 按 corpusid 排序
    filename TEXT                    -- 文件名（可选）
);
CREATE INDEX idx_final_delivery_corpusid ON final_delivery(corpusid);
CREATE INDEX idx_final_delivery_filename ON final_delivery(filename);
```

## 💡 关键优势

### 重建排序表的性能提升

**重建前（无序）：**
- id=1 → corpusid=146370575（随机）
- id=2 → corpusid=111463468（随机）
- 查询 id 1-50000 → 50000个随机 corpusid
- **结果：随机 I/O，性能差**

**重建后（有序）：**
- id=1 → corpusid=2（最小）
- id=2 → corpusid=5（递增）
- id=50000 → corpusid≈100000（连续）
- 查询 id 1-50000 → corpusid 连续在 2-100000
- **结果：顺序 I/O，性能提升 5-10倍**
