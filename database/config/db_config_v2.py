#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Configuration V2
"""

import os
from pathlib import Path

# =============================================================================
# Database Connection Config
# =============================================================================

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 's2orc_d1'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'grained'),
}

# =============================================================================
# Tablespace Config
# =============================================================================

TABLESPACE_CONFIG = {
    'enabled': True,
    'name': 'D1_Tablespace',
    'location': 'F:\\PostgreSQL',
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

# =============================================================================
# Partition Config
# =============================================================================

PARTITION_CONFIG = {
    'partition_count': 64,
    'partition_modulus': 64,
}

# =============================================================================
# Bulk Operation Config
# =============================================================================

BULK_INSERT_CONFIG = {
    'batch_size': 10000,
    'commit_interval': 50000,
    'use_copy': True,
    'disable_triggers': True,
}

BULK_UPSERT_CONFIG = {
    'batch_size': 5000,
    'commit_interval': 25000,
    'use_temp_table': True,
}

# =============================================================================
# Connection Pool Config
# =============================================================================

POOL_CONFIG = {
    'min_size': 2,
    'max_size': 16,
}

# =============================================================================
# Logging Config
# =============================================================================

LOG_CONFIG = {
    'log_level': 'INFO',
    'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'log_file': 'logs/database_v2.log',
}

# =============================================================================
# SQL Scripts Path
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
# Helper Functions
# =============================================================================

def get_corpusid_partition(corpusid: int) -> int:
    """Get partition ID for corpusid"""
    return corpusid % PARTITION_CONFIG['partition_count']


def get_partition_name(table_name: str, partition_id: int) -> str:
    """Get partition table name"""
    return f"{table_name}_p{partition_id}"


def get_all_partition_names(table_name: str) -> list:
    """Get all partition names for a table"""
    return [
        get_partition_name(table_name, i) 
        for i in range(PARTITION_CONFIG['partition_count'])
    ]


def get_connection_string() -> str:
    """Build PostgreSQL connection string"""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


def get_psql_command() -> str:
    """Build psql command"""
    return (
        f"psql -h {DB_CONFIG['host']} -p {DB_CONFIG['port']} "
        f"-U {DB_CONFIG['user']} -d {DB_CONFIG['database']}"
    )


# =============================================================================
# PostgreSQL Config Recommendations
# =============================================================================

POSTGRES_CONFIG_RECOMMENDATIONS = {
    'shared_buffers': '8GB',
    'effective_cache_size': '24GB',
    'maintenance_work_mem': '2GB',
    'work_mem': '128MB',
    'wal_buffers': '32MB',
    'max_wal_size': '8GB',
    'min_wal_size': '2GB',
    'checkpoint_timeout': '30min',
    'checkpoint_completion_target': '0.9',
    'max_parallel_workers_per_gather': '4',
    'max_parallel_workers': '12',
    'max_worker_processes': '16',
    'random_page_cost': '1.1',
    'effective_io_concurrency': '200',
    'jit': 'on',
    'enable_partition_pruning': 'on',
    'constraint_exclusion': 'partition',
    'max_connections': '200',
    'autovacuum': 'on',
    'autovacuum_max_workers': '4',
    'autovacuum_naptime': '30s',
}
