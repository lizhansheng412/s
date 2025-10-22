#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database initialization script
"""

import sys
import os
import logging
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.config.db_config_v2 import DB_CONFIG, FIELD_TABLES, TABLESPACE_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Force UTF-8 and C locale to avoid encoding issues
os.environ['PGCLIENTENCODING'] = 'UTF8'
os.environ['LC_ALL'] = 'C'
os.environ['LANG'] = 'en_US.UTF-8'


def get_connection_params(database='postgres'):
    """Get connection parameters with forced settings"""
    params = {
        'host': '127.0.0.1',  # Use 127.0.0.1 instead of localhost
        'port': DB_CONFIG['port'],
        'database': database,
        'user': DB_CONFIG['user'],
        'password': DB_CONFIG['password'],
        'options': '-c lc_messages=C -c client_encoding=UTF8'
    }
    return params


def create_database():
    """Create database if not exists"""
    db_name = DB_CONFIG['database']
    
    try:
        conn = psycopg2.connect(**get_connection_params('postgres'))
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        
        if exists:
            logger.info(f"Database {db_name} already exists")
        else:
            logger.info(f"Creating database: {db_name}")
            cursor.execute(f"CREATE DATABASE {db_name} WITH ENCODING 'UTF8'")
            logger.info(f"Database {db_name} created successfully")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        sys.exit(1)


def create_tables(tables: list = None):
    """Create tables (UNLOGGED, no indexes, TEXT type for data)"""
    try:
        conn = psycopg2.connect(**get_connection_params(DB_CONFIG['database']))
        conn.autocommit = True
        cursor = conn.cursor()
        
        # 创建表空间（如果启用）
        tablespace_clause = ""
        if TABLESPACE_CONFIG['enabled'] and TABLESPACE_CONFIG['location']:
            tablespace_name = TABLESPACE_CONFIG['name']
            location = TABLESPACE_CONFIG['location']
            
            logger.info(f"\n{'='*80}")
            logger.info(f"设置表空间")
            logger.info(f"{'='*80}")
            logger.info(f"表空间名称: {tablespace_name}")
            logger.info(f"存储位置: {location}")
            
            try:
                # 检查表空间是否存在
                cursor.execute("""
                    SELECT spcname FROM pg_tablespace WHERE spcname = %s;
                """, (tablespace_name,))
                
                if cursor.fetchone():
                    logger.info(f"✓ 表空间 '{tablespace_name}' 已存在")
                else:
                    # 创建表空间
                    cursor.execute(f"""
                        CREATE TABLESPACE {tablespace_name} 
                        LOCATION '{location}';
                    """)
                    logger.info(f"✓ 表空间创建成功")
                
                tablespace_clause = f" TABLESPACE {tablespace_name}"
            except Exception as e:
                logger.warning(f"⚠️  表空间创建失败: {e}")
                tablespace_clause = ""
        
        tables_to_create = tables if tables else FIELD_TABLES
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Creating tables (UNLOGGED + TEXT type for maximum speed)")
        logger.info(f"Target tables: {', '.join(tables_to_create)}")
        if tablespace_clause:
            logger.info(f"存储位置: {TABLESPACE_CONFIG['location']}")
        logger.info(f"{'='*80}\n")
        
        for table_name in tables_to_create:
            if table_name not in FIELD_TABLES:
                logger.warning(f"Skip invalid table: {table_name}")
                continue
            
            logger.info(f"Creating table: {table_name}")
            
            # 所有表统一使用TEXT类型（不验证不解析，最快）
            cursor.execute(f"""
                CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                    corpusid BIGINT PRIMARY KEY,
                    data TEXT,
                    insert_time TIMESTAMP DEFAULT NOW(),
                    update_time TIMESTAMP DEFAULT NOW()
                ){tablespace_clause}
            """)
            
            cursor.execute(f"ALTER TABLE {table_name} SET (autovacuum_enabled = false)")
            
            logger.info(f"  ✓ Table {table_name} created (TEXT type)")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Tables created: {len(tables_to_create)}/{len(FIELD_TABLES)}")
        logger.info(f"{'='*80}\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Initialize database and tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/init_database.py --init
  python scripts/init_database.py --init --tables embeddings_specter_v1 s2orc
  python scripts/init_database.py --init --machine machine1
        """
    )
    parser.add_argument('--init', action='store_true', help='Create database and tables')
    parser.add_argument('--tables', nargs='+', help='Table names to create')
    parser.add_argument('--machine', type=str, choices=['machine1', 'machine2', 'machine3'],
                       help='Machine ID')
    
    args = parser.parse_args()
    
    if args.init:
        create_database()
        
        tables = None
        if args.machine:
            from machine_config import get_machine_config
            config = get_machine_config(args.machine)
            tables = config['tables']
            logger.info(f"Loaded machine config: {args.machine}")
            logger.info(f"  {config['description']}")
        elif args.tables:
            tables = args.tables
        
        create_tables(tables)
        logger.info("\nDatabase initialization completed!")
        
        if args.machine:
            logger.info(f"Next: python scripts/batch_process_machine.py --machine {args.machine} --base-dir \"F:\\machine_win01\\2025-09-30\"\n")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
