# -*- coding: utf-8 -*-
"""数据库配置模块 V2"""

from .db_config_v2 import (
    DB_CONFIG,
    DB_SHARED_CONFIG,
    MACHINE_DB_MAP,
    get_db_config,
    TABLESPACE_CONFIG,
    MAIN_TABLE,
    FIELD_TABLES,
    ALL_TABLES,
)

__all__ = [
    'DB_CONFIG',
    'DB_SHARED_CONFIG',
    'MACHINE_DB_MAP',
    'get_db_config',
    'TABLESPACE_CONFIG',
    'MAIN_TABLE',
    'FIELD_TABLES',
    'ALL_TABLES',
]
