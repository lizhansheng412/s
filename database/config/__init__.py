# -*- coding: utf-8 -*-
"""数据库配置模块 V2"""

from .db_config_v2 import (
    DB_CONFIG,
    MAIN_TABLE,
    FIELD_TABLES,
    ALL_TABLES,
    PARTITION_CONFIG,
    BULK_INSERT_CONFIG,
    BULK_UPSERT_CONFIG,
    POOL_CONFIG,
    LOG_CONFIG,
    SQL_SCRIPTS,
    POSTGRES_CONFIG_RECOMMENDATIONS,
    get_corpusid_partition,
    get_partition_name,
    get_all_partition_names,
    get_connection_string,
    get_psql_command,
)

__all__ = [
    'DB_CONFIG',
    'MAIN_TABLE',
    'FIELD_TABLES',
    'ALL_TABLES',
    'PARTITION_CONFIG',
    'BULK_INSERT_CONFIG',
    'BULK_UPSERT_CONFIG',
    'POOL_CONFIG',
    'LOG_CONFIG',
    'SQL_SCRIPTS',
    'POSTGRES_CONFIG_RECOMMENDATIONS',
    'get_corpusid_partition',
    'get_partition_name',
    'get_all_partition_names',
    'get_connection_string',
    'get_psql_command',
]
