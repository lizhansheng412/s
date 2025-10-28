# S2ORC数据合并导出工具

## 快速开始

### 运行命令
```bash
cd D:\projects\S2ORC\gz_filed_update
python scripts\merge\export_to_jsonl_parallel.py
```

### 配置参数

在 `export_to_jsonl_parallel.py` 中可调整：

```python
BATCH_SIZE = 15000              # 每批15000条 (~1.65GB/文件)
NUM_WORKERS = 8                 # 8个Worker进程
TARGET_TOTAL = 100_000_000      # 目标1亿条
OUTPUT_DIR = r"F:\delivery_data\first_batch"
```

## 控制台输出

运行时会实时显示：

```
[████████████░░░░░░░░░░░░░░░░░░░░] 31.5% | 已处理: 31,500,000 | 剩余: 68,500,000 | 速度: 920条/秒 | 已用时: 9.5h | 预计剩余: 20.7h
```

- **进度条**：直观显示完成百分比
- **已处理**：成功写入的corpusid总量
- **剩余**：待处理的corpusid数量
- **速度**：当前处理速度（条/秒）
- **已用时**：已经运行的时间
- **预计剩余**：预估还需要的时间

## 性能指标

| 指标 | 预期值 |
|------|--------|
| **总耗时** | 30-45小时（1.25-1.9天）|
| **文件数量** | 6,667个 |
| **单文件大小** | 1.5-1.8GB |
| **总数据量** | ~10.2TB |
| **CPU使用率** | 90-100% |
| **内存占用** | 28-30GB |

## 断点续传

程序支持自动断点续传：
- 中断后直接重新运行相同命令即可
- 自动从上次成功的批次继续

## 日志文件

详细日志保存在：`merge_export_parallel.log`

包含所有Worker和Writer进程的详细运行信息，用于问题排查。

## 输出格式

**文件名**：`batch_{start_id}_{end_id}.jsonl`

**内容**：每行一个JSON对象

```json
{
  "corpusid": 123456789,
  "title": "Paper Title",
  "authors": [...],
  "abstract": "...",
  "tldr": {"model": "...", "text": "..."},
  "detailOfAuthors": [...],
  "publicationVenue": {...},
  "paperId": null,
  "externalIds": null,
  "fieldsOfStudy": null,
  "citations": null,
  "references": null,
  "citationStyles": null,
  "corpusId": null,
  "content": null,
  "embedding": null,
  "detailOfReference": null
}
```

## 常见问题

### 内存不足
编辑脚本减小队列大小：
```python
TASK_QUEUE_SIZE = 16    # 默认32
RESULT_QUEUE_SIZE = 8   # 默认16
```

### USB写入太慢
- 检查USB接口（使用USB 3.0+）
- 检查硬盘健康状态

### 调整批次大小
```python
BATCH_SIZE = 20000  # 增大到2.2GB/批（需要更多内存）
BATCH_SIZE = 10000  # 减小到1.1GB/批（节省内存）
```

### 调整Worker数量
```python
NUM_WORKERS = 12    # 增加Worker（需要更多CPU和内存）
NUM_WORKERS = 6     # 减少Worker（降低资源消耗）
```

## 优雅停止

按 `Ctrl + C` 可随时停止程序，当前批次会完成后再退出。

---

**预计完成时间**：1.5天内 🚀

