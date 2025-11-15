#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified Machine and Database Configuration
整合机器配置和数据库配置文件
"""

# =============================================================================
# Database Connection Config
# =============================================================================

# 共享配置（默认配置）
DB_SHARED_CONFIG = {
    'user': 'postgres',
    'client_encoding': 'utf8',
}

# 机器配置映射（数据库、端口、主机、密码）
MACHINE_DB_MAP = {
    'machine0': {
        'host': 'localhost', 
        'database': 'machine0', 
        'port': 54300, 
        'password': '333444',
        'data_dir': 'E:\\postgresql_data',
        'service_name': 'machine0'
    },
    'machine2': {
        'host': 'localhost', 
        'database': 'machine2', 
        'port': 5432, 
        'password': 'grained',
        'data_dir': 'D:\\data\\data', 
        'service_name': 'postgresql-x64-18'
    },
    'machine3': {
        'host': 'localhost', 
        'database': 'machine3', 
        'port': 54330,
        'data_dir': 'E:\\postgresql_data',
        'service_name': 'machine3'
    },
}

def get_db_config(machine_id: str) -> dict:
    """
    获取数据库配置
    
    Args:
        machine_id: 机器ID ('machine0', 'machine2', 'machine3')
    
    Returns:
        数据库配置字典
    """
    if machine_id not in MACHINE_DB_MAP:
        raise ValueError(f"Invalid machine_id: {machine_id}. Valid: {list(MACHINE_DB_MAP.keys())}")
    
    config = DB_SHARED_CONFIG.copy()
    config.update(MACHINE_DB_MAP[machine_id])
    return config
