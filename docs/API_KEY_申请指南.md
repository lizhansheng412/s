# Semantic Scholar API Key 申请指南

## 🚨 为什么需要 API Key？

您遇到的 **429 Too Many Requests** 错误是因为 Semantic Scholar 对**没有 API Key 的公共访问**有严格的速率限制。

### 速率限制对比

| 访问方式 | 速率限制 | 适用场景 |
|---------|---------|---------|
| 无 API Key | **极低**（几次请求/分钟） | 测试、偶尔查询 |
| 有 API Key | **较高**（100+ 请求/5分钟） | 批量处理、研究项目 |

## ✅ 解决方案

### 方案 1：申请免费 API Key（推荐）⭐

#### 步骤 1：访问申请页面

🔗 **申请地址**: https://www.semanticscholar.org/product/api#api-key-form

#### 步骤 2：填写申请表单

需要填写以下信息：
- **姓名** (Name)
- **电子邮件** (Email) - 使用真实邮箱接收 API Key
- **组织/机构** (Organization) - 填写您的大学或研究机构
- **使用目的** (Use Case) - 简要说明用途

示例填写：
```
Name: Zhang San
Email: zhangsan@university.edu
Organization: XX University
Use Case: Academic research on scientific literature analysis using S2ORC dataset
```

#### 步骤 3：接收 API Key

- 提交后会立即在页面显示 API Key
- 同时会发送到您的邮箱
- **请妥善保存，不要分享给他人**

#### 步骤 4：配置 API Key

编辑 `config.py` 文件：

```python
# 将您收到的 API Key 填写在这里
API_KEY = "your-actual-api-key-here"
```

例如：
```python
API_KEY = "AbCdEf123456789XyZ"
```

#### 步骤 5：验证配置

运行测试脚本：
```bash
python test_api.py
```

如果配置正确，会看到：
```
✓ 使用 API Key: AbCdEf1234...（已部分隐藏）
✓ API 连接成功！
✓ API 工作正常，可以开始处理数据
```

### 方案 2：等待后重试（临时方案）

如果您**暂时无法申请 API Key**，可以：

1. **等待时间**: 10-60 分钟
2. **清除浏览器缓存** 或 **更换网络环境**
3. **减少批次大小**: 已自动调整为更保守的设置

⚠️ **注意**: 此方案不适合批量处理大量数据，强烈建议申请 API Key。

## 🔧 代码已优化

我已经为您的代码添加了以下功能：

### ✅ 1. API Key 支持
- 自动检测并使用配置的 API Key
- 在请求头中添加 `x-api-key` 字段

### ✅ 2. 智能重试机制
- 遇到 429 错误自动重试（最多 3 次）
- 每次重试等待时间递增（10秒、20秒、30秒）
- 详细的重试日志

### ✅ 3. 增加批次延迟
- 从 1 秒增加到 **3 秒**
- 降低触发速率限制的概率

### ✅ 4. 更好的错误提示
- 清晰的错误信息和解决建议
- 自动提示申请 API Key 的链接

## 📊 配置文件说明

### config.py 完整配置

```python
"""
配置文件 - S2ORC 数据处理工具
"""

# ==================== 基本配置 ====================

# GZ 文件所在目录
INPUT_DIR = r"E:\machine_win01\2025-09-22"

# ==================== API Key 配置 ====================

# Semantic Scholar API Key (强烈推荐)
# 申请地址: https://www.semanticscholar.org/product/api#api-key-form
API_KEY = None  # 收到 API Key 后填写在这里

# ==================== 输出配置 ====================

OUTPUT_PREFIX = "enhanced_"
OUTPUT_EXTENSION = ".jsonl"

# ==================== 日志配置 ====================

LOG_FILE = "s2orc_processing.log"
LOG_LEVEL = "INFO"
```

## 🎯 使用流程

### 完整流程

1. ✅ **申请 API Key** （5分钟）
2. ✅ **配置 API Key** 到 `config.py`
3. ✅ **测试连接**: `python test_api.py`
4. ✅ **运行处理**: `python process_s2orc.py`

### 无 API Key 的临时流程（不推荐）

1. ⚠️ **等待 30-60 分钟**
2. ⚠️ **运行测试**: `python test_api.py`
3. ⚠️ **如果成功，立即运行**: `python process_s2orc.py`
4. ⚠️ **可能随时再次触发限制**

## ❓ 常见问题

### Q1: API Key 是免费的吗？
**A**: 是的，完全免费！Semantic Scholar 为研究用途提供免费 API Key。

### Q2: 申请需要多长时间？
**A**: 即时申请，立即获得。填写表单后会立即显示 API Key。

### Q3: API Key 会过期吗？
**A**: 一般不会过期，但要遵守使用条款，不要滥用。

### Q4: 有了 API Key 就没有限制了吗？
**A**: 仍然有限制，但比公共访问高得多。一般足够处理大规模数据集。

### Q5: 可以分享 API Key 吗？
**A**: 不建议。每个人应该申请自己的 API Key。

## 📝 API Key 使用规范

1. ✅ **学术研究使用**
2. ✅ **遵守速率限制**
3. ✅ **不要过度频繁请求**
4. ❌ **不要分享给他人**
5. ❌ **不要用于商业用途**（需要商业授权）
6. ❌ **不要公开在代码仓库中**

## 🔒 安全建议

如果使用 Git 管理代码，建议：

1. **方法 1**: 将 `config.py` 添加到 `.gitignore`
2. **方法 2**: 使用环境变量存储 API Key
3. **方法 3**: 创建 `config.local.py`（不提交到仓库）

## 📞 需要帮助？

如果遇到问题：

1. 查看日志文件：`s2orc_processing.log`
2. 运行测试脚本：`python test_api.py`
3. 检查 API Key 是否正确填写
4. 确认网络连接正常

---

**现在就申请 API Key，开始高效处理数据！** 🚀

申请地址: https://www.semanticscholar.org/product/api#api-key-form

