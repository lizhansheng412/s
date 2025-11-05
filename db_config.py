#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Configuration V2
"""

# =============================================================================
# Database Connection Config
# =============================================================================

# 共享配置（所有机器通用）
DB_SHARED_CONFIG = {
    'host': 'localhost',
    'user': 'postgres',
    'password': 'grained',
    'client_encoding': 'utf8',
}

# 机器配置映射（数据库和端口）
MACHINE_DB_MAP = {
    'machine0': {'database': 's2orc_d0', 'port': 5430},
    'machine1': {'database': 's2orc_d1', 'port': 5431},
    'machine2': {'database': 's2orc_d2', 'port': 5432},
    'machine3': {'database': 's2orc_d3', 'port': 5433},
}

def get_db_config(machine_id: str) -> dict:
    """
    获取数据库配置
    
    Args:
        machine_id: 机器ID ('machine0', 'machine1', 'machine2', 'machine3')
    
    Returns:
        数据库配置字典
    """
    if machine_id not in MACHINE_DB_MAP:
        raise ValueError(f"Invalid machine_id: {machine_id}. Valid: {list(MACHINE_DB_MAP.keys())}")
    
    config = DB_SHARED_CONFIG.copy()
    config.update(MACHINE_DB_MAP[machine_id])
    return config

# 默认配置
DB_CONFIG = get_db_config('machine0')

# =============================================================================
# Tablespace Config
# =============================================================================

TABLESPACE_CONFIG = {
    'enabled': False,  # 禁用自定义表空间，使用默认位置
    'name': 'd3_tablespace',  # 根据机器配置修改
    'location': 'E:\\postgreSQL',  # 禁用时此项被忽略
}

# =============================================================================
# Table Names
# =============================================================================

MAIN_TABLE = 'corpus_registry'

FIELD_TABLES = [
    'papers',
    'abstracts',
    'tldrs',
    's2orc',
    's2orc_v2',
    'citations',
    'authors',
    'embeddings_specter_v1',
    'embeddings_specter_v2',
    'paper_ids',
    'publication_venues',
]

ALL_TABLES = [MAIN_TABLE] + FIELD_TABLES

