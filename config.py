"""
配置文件 - S2ORC 数据处理工具
"""

# ==================== 基本配置 ====================

# GZ 文件所在目录（必须修改为实际路径）
INPUT_DIR = r"E:\machine_win01\2025-09-22"

# ==================== API Key 配置 ====================

# Semantic Scholar API Key (可选，但强烈推荐)
# 申请地址: https://www.semanticscholar.org/product/api#api-key-form
API_KEY = None

# ==================== 输出配置 ====================

# 输出文件名前缀
OUTPUT_PREFIX = "enhanced_"

# 输出文件扩展名
OUTPUT_EXTENSION = ".jsonl"

# ==================== 日志配置 ====================

# 日志文件路径
LOG_FILE = "s2orc_processing.log"

# 日志级别（DEBUG, INFO, WARNING, ERROR, CRITICAL）
LOG_LEVEL = "INFO"