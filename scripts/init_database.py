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
from database.config import db_config_v2
from database.config.db_config_v2 import FIELD_TABLES, TABLESPACE_CONFIG, get_db_config

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
        'port': db_config_v2.DB_CONFIG['port'],
        'database': database,
        'user': db_config_v2.DB_CONFIG['user'],
        'password': db_config_v2.DB_CONFIG['password'],
        'options': '-c lc_messages=C -c client_encoding=UTF8'
    }
    return params


def create_database():
    """Create database if not exists"""
    db_name = db_config_v2.DB_CONFIG['database']
    
    try:
        conn = psycopg2.connect(**get_connection_params('postgres'))
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        
        if exists:
            logger.info(f"Database {db_name} already exists")
        else:
            # 如果启用了表空间，先创建表空间，再创建数据库
            tablespace_clause = ""
            if TABLESPACE_CONFIG['enabled'] and TABLESPACE_CONFIG['location']:
                tablespace_name = TABLESPACE_CONFIG['name']
                location = TABLESPACE_CONFIG['location']
                
                logger.info(f"\n{'='*80}")
                logger.info(f"创建表空间（用于存储整个数据库）")
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
            
            logger.info(f"\n创建数据库: {db_name}")
            if tablespace_clause:
                logger.info(f"数据库存储位置: {TABLESPACE_CONFIG['location']}")
                cursor.execute(f"CREATE DATABASE {db_name} WITH ENCODING 'UTF8'{tablespace_clause}")
            else:
                cursor.execute(f"CREATE DATABASE {db_name} WITH ENCODING 'UTF8'")
            
            logger.info(f"✓ 数据库 {db_name} 创建成功")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        sys.exit(1)


def create_tables(tables: list = None):
    """Create tables (UNLOGGED, no indexes, TEXT type for data)"""
    try:
        conn = psycopg2.connect(**get_connection_params(db_config_v2.DB_CONFIG['database']))
        conn.autocommit = True
        cursor = conn.cursor()
        
        # 获取表空间配置（表会继承数据库的表空间，也可以单独指定）
        tablespace_clause = ""
        if TABLESPACE_CONFIG['enabled'] and TABLESPACE_CONFIG['location']:
            tablespace_name = TABLESPACE_CONFIG['name']
            tablespace_clause = f" TABLESPACE {tablespace_name}"
        
        tables_to_create = tables if tables else FIELD_TABLES
        
        logger.info(f"\n{'='*80}")
        logger.info(f"创建表 (UNLOGGED + TEXT类型，极速模式)")
        logger.info(f"目标表: {', '.join(tables_to_create)}")
        if TABLESPACE_CONFIG['enabled']:
            logger.info(f"存储位置: {TABLESPACE_CONFIG['location']} (通过表空间)")
        logger.info(f"{'='*80}\n")
        
        # 不同表使用不同的主键列名和类型
        TABLE_PRIMARY_KEY_MAP = {
            'authors': ('authorid', 'BIGINT'),
            'publication_venues': ('publicationvenueid', 'TEXT'),  # UUID字符串
        }
        
        for table_name in tables_to_create:
            if table_name not in FIELD_TABLES:
                logger.warning(f"跳过无效表: {table_name}")
                continue
            
            logger.info(f"创建表: {table_name}")
            
            # paper_ids表特殊处理：数据合并状态追踪表（极速导入优化版）
            if table_name == 'paper_ids':
                cursor.execute(f"""
                    CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                        corpusid BIGINT PRIMARY KEY,
                        
                        -- 10个数据源状态字段 (0=未开始, 1=成功, 2=失败)
                        papers SMALLINT DEFAULT 0,
                        abstracts SMALLINT DEFAULT 0,
                        tldrs SMALLINT DEFAULT 0,
                        s2orc SMALLINT DEFAULT 0,
                        s2orc_v2 SMALLINT DEFAULT 0,
                        citations SMALLINT DEFAULT 0,
                        authors SMALLINT DEFAULT 0,
                        embeddings_specter_v1 SMALLINT DEFAULT 0,
                        embeddings_specter_v2 SMALLINT DEFAULT 0,
                        publication_venues SMALLINT DEFAULT 0
                        
                        -- 注意：merge_status 列在数据导入完成后添加（性能优化）
                        -- 注意：时间戳字段已删除（极速插入优化）
                    ){tablespace_clause}
                """)
                logger.info(f"  ✓ 表 {table_name} 创建成功 (主键: corpusid, 10个数据源字段)")
                logger.info(f"  ⚡ 极速模式：无时间戳，无 merge_status（数据导入完成后添加）")
            
            # citations表特殊处理：自增主键 + citingcorpusid字段
            elif table_name == 'citations':
                cursor.execute(f"""
                    CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                        id BIGSERIAL PRIMARY KEY,
                        citingcorpusid BIGINT,
                        data TEXT,
                        insert_time TIMESTAMP DEFAULT NOW(),
                        update_time TIMESTAMP DEFAULT NOW()
                    ){tablespace_clause}
                """)
                # 为citingcorpusid创建索引（用于查询）
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_citations_citingcorpusid 
                    ON {table_name}(citingcorpusid)
                """)
                logger.info(f"  ✓ 表 {table_name} 创建成功 (主键: id BIGSERIAL, 索引: citingcorpusid)")
            else:
                # 其他表：使用配置的主键
                primary_key, key_type = TABLE_PRIMARY_KEY_MAP.get(table_name, ('corpusid', 'BIGINT'))
                
                cursor.execute(f"""
                    CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                        {primary_key} {key_type} PRIMARY KEY,
                        data TEXT,
                        insert_time TIMESTAMP DEFAULT NOW(),
                        update_time TIMESTAMP DEFAULT NOW()
                    ){tablespace_clause}
                """)
                logger.info(f"  ✓ 表 {table_name} 创建成功 (主键: {primary_key} {key_type})")
            
            cursor.execute(f"ALTER TABLE {table_name} SET (autovacuum_enabled = false)")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"已创建表: {len(tables_to_create)}/{len(FIELD_TABLES)}")
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
    parser.add_argument('--machine', type=str, choices=['machine1', 'machine2', 'machine3', 'machine0'],
                       help='Machine ID')
    
    args = parser.parse_args()
    
    if args.init:
        # 根据机器ID更新数据库配置
        if args.machine:
            db_config = get_db_config(args.machine)
            db_config_v2.DB_CONFIG.update(db_config)
            logger.info(f"Machine: {args.machine}")
            logger.info(f"Database: {db_config_v2.DB_CONFIG['database']}\n")
        
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
