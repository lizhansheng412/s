# corpusid 到 gz 文件名映射

## 快速开始

### 1. 初始化表
```bash
python scripts/new_test/init_corpusid_mapping.py
```

### 2. 批量导入
```bash
# m1 硬盘
python scripts/new_test/batch_process_mapping.py --disk m1 --base-dir /path/to/m1 --extractors 1 --inserters 4

# m2 硬盘
python scripts/new_test/batch_process_mapping.py --disk m2 --base-dir /path/to/m2 --extractors 1 --inserters 4

# m3 硬盘
python scripts/new_test/batch_process_mapping.py --disk m3 --base-dir /path/to/m3 --extractors 1 --inserters 4
```

### 3. 去重建索引
```bash
python scripts/new_test/init_corpusid_mapping.py --finalize
```

## 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--extractors` | 提取进程数（USB硬盘建议1） | 1 |
| `--inserters` | 插入进程数（SSD建议3-4） | 4 |
| `--no-resume` | 禁用断点续传 | 默认启用 |
| `--reset` | 重置进度（清空已完成记录） | - |

## 进度显示

运行时实时显示：
```
⏰ 开始时间: 2025-10-28 16:45:30
📊 总文件数: 1250

📊 进度:125/1250 (10.0%) | ✅成功:123 ❌失败:2 | ⏱️已用:00:15:30 预计剩余:02:18:45
```

完成后显示：
```
======================================================================
✅ [s2orc] 处理完成
======================================================================
⏰ 结束时间: 2025-10-28 19:12:15
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

## 硬盘配置

- **m1**: embeddings-specter_v1, s2orc
- **m2**: embeddings-specter_v2, s2orc_v2
- **m3**: citations

## 日志位置

- 进度: `logs/corpusid_mapping_progress/`
- 失败: `logs/corpusid_mapping_failed/`

