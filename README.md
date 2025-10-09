# S2ORC 数据集批量增强处理工具

一个用于处理 S2ORC 数据集的 Python 工具，可以从压缩的 GZ 文件中提取论文数据，调用 Semantic Scholar API 获取详细信息，并生成增强后的 JSONL 文件。

## 功能特性

- ✅ 自动扫描和解压 GZ 文件（保留原文件）
- ✅ 批量提取论文 corpusId 并自动格式化
- ✅ 调用 Semantic Scholar Graph API 批量查询论文详细信息
- ✅ 智能合并原始数据和 API 返回的详细信息
- ✅ 实时显示处理进度（进度条 + 详细日志）
- ✅ 完善的错误处理和自动重试机制
- ✅ 支持 API Key（避免速率限制）

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 申请 API Key（重要！）⭐

**为什么需要 API Key？**
- 无 API Key 会遇到 429 速率限制错误
- 有 API Key 可获得更高的速率限制（100+ 请求/5分钟）
- 完全免费，即时获取

**申请步骤**：

1. 访问申请页面：https://www.semanticscholar.org/product/api#api-key-form

2. 填写表单：
   - Name: 您的姓名
   - Email: 您的邮箱（用于接收 API Key）
   - Organization: 您的机构
   - Use Case: 例如 "Academic research on scientific literature analysis using S2ORC dataset"

3. 立即获取 API Key（会显示在页面上并发送到邮箱）

### 3. 配置

编辑 `config.py` 文件：

```python
# 必须配置：GZ 文件所在目录
INPUT_DIR = r"E:\machine_win01\2025-09-22"

# 强烈推荐：填写您申请到的 API Key
API_KEY = "your-api-key-here"  # 替换为实际的 API Key
```

### 4. 测试连接（可选）

```bash
python test_api.py
```

如果看到 `✓ API 工作正常`，说明配置成功。

### 5. 运行处理

```bash
python process_s2orc.py
```

## 输出说明

**文件位置**：与原 GZ 文件相同目录

**文件名格式**：`enhanced_<原文件名>.jsonl`

**文件格式**：标准 JSONL（每行一个 JSON 对象）

**示例**：
```
输入: 20250923_032917_00215_4nkum_00dc9a2c.gz
输出: enhanced_20250923_032917_00215_4nkum_00dc9a2c.jsonl
```

## 示例输出

```
============================================================
处理文件 1/5: 20250923_032917_00215_4nkum_00dc9a2c.gz
============================================================
步骤 1/5: 解压 GZ 文件...
成功读取，共 1500 篇论文

步骤 2/5: 提取 corpusId...
提取到 1500 个有效的 corpusId

步骤 3/5: 调用 Semantic Scholar API...
API 查询进度: 100%|████████████| 1500/1500 [00:45<00:00, 33.20篇/s]

步骤 4/5: 合并论文数据...
合并数据: 100%|██████████████| 1500/1500 [00:02<00:00, 587篇/s]

步骤 5/5: 保存增强数据...
成功保存到: enhanced_20250923_032917_00215_4nkum_00dc9a2c.jsonl
文件大小: 15.43 MB
```

## 配置说明

### config.py 配置项

```python
# ==================== 基本配置 ====================
INPUT_DIR = r"E:\machine_win01\2025-09-22"  # GZ 文件目录

# ==================== API Key 配置 ====================
API_KEY = None  # 填写申请到的 API Key

# ==================== 输出配置 ====================
OUTPUT_PREFIX = "enhanced_"  # 输出文件名前缀
OUTPUT_EXTENSION = ".jsonl"  # 输出文件扩展名

# ==================== 日志配置 ====================
LOG_FILE = "s2orc_processing.log"  # 日志文件路径
LOG_LEVEL = "INFO"  # 日志级别
```

### API 配置（代码中固定，无需修改）

- **API 端点**: `https://api.semanticscholar.org/graph/v1/paper/batch`
- **批次大小**: 500 篇/次（API 限制）
- **批次延迟**: 3 秒（避免速率限制）
- **超时时间**: 60 秒
- **自动重试**: 最多 3 次，递增延迟

### API 请求字段

脚本会自动请求以下论文详细信息：

```
url, title, abstract, venue, publicationVenue, year,
referenceCount, citationCount, influentialCitationCount,
isOpenAccess, openAccessPdf, fieldsOfStudy, s2FieldsOfStudy,
publicationTypes, publicationDate, journal, citationStyles,
authors, citations, references, embedding, tldr
```

## 错误处理

### 429 Too Many Requests（速率限制）

**原因**：未配置 API Key 或请求过于频繁

**解决方案**：
1. 申请并配置 API Key（推荐）
2. 等待 30-60 分钟后重试
3. 脚本会自动重试 3 次，每次等待递增

### 网络错误

**解决方案**：
- 检查网络连接
- 确保可以访问 semanticscholar.org
- 查看日志文件 `s2orc_processing.log` 获取详细错误信息

### corpusId 缺失

**解决方案**：
- 脚本会自动跳过缺失 corpusId 的论文
- 在日志中记录警告信息

## 技术细节

### corpusId 格式化

API 要求使用带前缀的格式：

```python
# 错误格式（会被 API 拒绝）
"1234567"

# 正确格式（脚本自动处理）
"CorpusId:1234567"
```

### 数据合并策略

- 保留原始数据中的所有字段
- 只添加原始数据中缺失的新字段
- 如果原始字段为 `null`，用 API 数据填充

### 性能指标

- **处理速度**: 约 30-35 篇论文/秒
- **内存使用**: 流式处理，内存占用低
- **预计时间**: 1500 篇论文约需 1 分钟

## 项目文件

```
gz_filed_update/
├── process_s2orc.py       # 主处理脚本
├── config.py              # 配置文件
├── test_api.py            # API 连接测试工具
├── requirements.txt       # Python 依赖
├── README.md              # 本文档
└── s2orc_processing.log   # 运行日志（自动生成）
```

## 常见问题

### Q1: 为什么推荐使用 API Key？

**A**: 无 API Key 的速率限制极低（几次/分钟），批量处理会频繁遭遇 429 错误。有 API Key 可获得 100+ 次/5分钟的限制，足够处理大规模数据。

### Q2: API Key 收费吗？

**A**: 完全免费！用于学术研究目的。

### Q3: 处理会修改原始 GZ 文件吗？

**A**: 不会。原始 GZ 文件保持不变，增强数据保存为新的 JSONL 文件。

### Q4: 可以中断后继续吗？

**A**: 当前版本不支持断点续传。如果中断，需要重新处理。建议一次性处理完成。

### Q5: 如何查看详细日志？

**A**: 所有日志保存在 `s2orc_processing.log` 文件中，包含详细的处理信息和错误记录。

### Q6: API Key 安全吗？

**A**: 请妥善保管 API Key，不要：
- 分享给他人
- 提交到公开代码仓库
- 用于商业用途

## API 使用规范

根据 Semantic Scholar 的使用条款：

✅ **允许**：
- 学术研究
- 非商业用途
- 合理的请求频率

❌ **禁止**：
- 商业用途（需单独授权）
- 过度频繁请求
- 分享或转售 API Key

## 技术支持

**遇到问题？**

1. 查看日志文件：`s2orc_processing.log`
2. 运行测试脚本：`python test_api.py`
3. 检查配置文件：`config.py`
4. 确认 API Key 已正确配置

**API 文档**：
- Semantic Scholar API: https://api.semanticscholar.org/api-docs/graph
- S2ORC 数据集: https://github.com/allenai/s2orc

## 系统要求

- **Python**: 3.7+
- **网络**: 需要访问 semanticscholar.org
- **依赖**: requests, tqdm

## 许可证

本工具仅用于学术研究目的。使用 Semantic Scholar API 需遵守其服务条款。

---

**快速开始**：
1. `pip install -r requirements.txt`
2. 申请 API Key: https://www.semanticscholar.org/product/api#api-key-form
3. 配置 `config.py`
4. 运行 `python process_s2orc.py`

祝您数据处理顺利！🚀
