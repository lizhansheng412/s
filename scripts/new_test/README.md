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
| `--no-resume` | 禁用断点续传 | - |

## 硬盘配置

- **m1**: embeddings-specter_v1, s2orc
- **m2**: embeddings-specter_v2, s2orc_v2
- **m3**: citations

## 日志位置

- 进度: `logs/corpusid_mapping_progress/`
- 失败: `logs/corpusid_mapping_failed/`

