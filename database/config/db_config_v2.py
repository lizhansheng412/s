#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库配置文件 V2 - 分表方案
适配硬件：32GB内存 + 16核CPU
"""

import os
from pathlib import Path

# =============================================================================
# 数据库连接配置
# =============================================================================

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 's2orc_corpus_v2'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'grained'),
}

# =============================================================================
# 表名配置（分表方案）
# =============================================================================

# 主表：记录所有corpusid
MAIN_TABLE = 'corpus_registry'

# 字段表列表（11个数据集）
FIELD_TABLES = [
    'papers',                    # 论文基础数据
    'abstracts',                 # 摘要
    'tldrs',                     # TLDR摘要
    's2orc',                     # S2ORC完整数据
    's2orc_v2',                  # S2ORC v2数据
    'citations',                 # 引用数据
    'authors',                   # 作者数据
    'embeddings_specter_v1',     # 嵌入向量v1
    'embeddings_specter_v2',     # 嵌入向量v2
    'paper_ids',                 # 论文ID映射
    'publication_venues',        # 发表场所
]

# 所有表（包括主表）
ALL_TABLES = [MAIN_TABLE] + FIELD_TABLES

# =============================================================================
# 分区配置
# =============================================================================

PARTITION_CONFIG = {
    'partition_count': 64,           # 分区数量（建议2的幂次）
    'partition_modulus': 64,         # 哈希模数
}

# =============================================================================
# 批量操作配置
# =============================================================================

BULK_INSERT_CONFIG = {
    'batch_size': 10000,             # 每批次记录数
    'commit_interval': 50000,        # 提交间隔
    'use_copy': True,                # 优先使用COPY命令
    'disable_triggers': True,        # 批量插入时禁用触发器
}

BULK_UPSERT_CONFIG = {
    'batch_size': 5000,              # UPSERT批次较小
    'commit_interval': 25000,        # 提交间隔
    'use_temp_table': True,          # 使用临时表优化
}

# =============================================================================
# 连接池配置
# =============================================================================

POOL_CONFIG = {
    'min_size': 2,
    'max_size': 16,                  # 16核CPU，最多16个连接
}

# =============================================================================
# 日志配置
# =============================================================================

LOG_CONFIG = {
    'log_level': 'INFO',
    'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'log_file': 'logs/database_v2.log',
}

# =============================================================================
# SQL脚本路径
# =============================================================================

DATABASE_ROOT = Path(__file__).parent.parent
SCHEMA_DIR = DATABASE_ROOT / 'schema_v2'

SQL_SCRIPTS = {
    'create_tables': SCHEMA_DIR / '01_create_tables_v2.sql',
    'create_indexes': SCHEMA_DIR / '02_create_indexes_v2.sql',
    'create_triggers': SCHEMA_DIR / '03_create_triggers_v2.sql',
    'optimize_config': SCHEMA_DIR / '04_optimize_config_v2.sql',
}

# =============================================================================
# 辅助函数
# =============================================================================

def get_corpusid_partition(corpusid: int) -> int:
    """计算corpusid对应的分区ID"""
    return corpusid % PARTITION_CONFIG['partition_count']


def get_partition_name(table_name: str, partition_id: int) -> str:
    """获取分区表名"""
    return f"{table_name}_p{partition_id}"


def get_all_partition_names(table_name: str) -> list:
    """获取某个表的所有分区名"""
    return [
        get_partition_name(table_name, i) 
        for i in range(PARTITION_CONFIG['partition_count'])
    ]


def get_connection_string() -> str:
    """构建PostgreSQL连接字符串"""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


def get_psql_command() -> str:
    """构建psql命令"""
    return (
        f"psql -h {DB_CONFIG['host']} -p {DB_CONFIG['port']} "
        f"-U {DB_CONFIG['user']} -d {DB_CONFIG['database']}"
    )


# =============================================================================
# PostgreSQL配置建议（32GB内存 + 16核CPU）
# =============================================================================

POSTGRES_CONFIG_RECOMMENDATIONS = {
    # 内存配置
    'shared_buffers': '8GB',                    # 系统内存的25%
    'effective_cache_size': '24GB',             # 系统内存的75%
    'maintenance_work_mem': '2GB',              # 索引创建、VACUUM
    'work_mem': '128MB',                        # 单个查询操作内存
    
    # WAL配置
    'wal_buffers': '32MB',
    'max_wal_size': '8GB',
    'min_wal_size': '2GB',
    'checkpoint_timeout': '30min',
    'checkpoint_completion_target': '0.9',
    
    # 并行配置（16核CPU）
    'max_parallel_workers_per_gather': '4',     # 单查询最多4个worker
    'max_parallel_workers': '12',               # 总并行worker数
    'max_worker_processes': '16',               # 后台进程数
    
    # 查询优化
    'random_page_cost': '1.1',                  # SSD优化
    'effective_io_concurrency': '200',          # SSD并发
    'jit': 'on',                                # JIT编译
    
    # 分区优化
    'enable_partition_pruning': 'on',           # 分区剪枝（关键！）
    'constraint_exclusion': 'partition',
    
    # 连接数
    'max_connections': '200',
    
    # Autovacuum
    'autovacuum': 'on',
    'autovacuum_max_workers': '4',
    'autovacuum_naptime': '30s',
}

