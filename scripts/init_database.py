#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库初始化脚本 - 一键创建数据库和表
"""

import sys
import logging
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.config.db_config_v2 import DB_CONFIG, FIELD_TABLES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_database():
    """创建数据库（如果不存在）"""
    db_name = DB_CONFIG['database']
    
    # 连接到postgres数据库
    conn_params = DB_CONFIG.copy()
    conn_params['database'] = 'postgres'
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # 检查数据库是否存在
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        
        if exists:
            logger.info(f"✓ 数据库 {db_name} 已存在")
        else:
            logger.info(f"创建数据库: {db_name}")
            cursor.execute(f"CREATE DATABASE {db_name}")
            logger.info(f"✓ 数据库 {db_name} 创建成功")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"创建数据库失败: {e}")
        sys.exit(1)


def create_tables(tables: list = None):
    """
    创建表（UNLOGGED，无索引）
    
    Args:
        tables: 要创建的表名列表。如果为None，创建所有表
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # 确定要创建的表
        tables_to_create = tables if tables else FIELD_TABLES
        
        logger.info(f"\n{'='*80}")
        logger.info(f"创建表（UNLOGGED模式，极速插入）")
        logger.info(f"目标表: {', '.join(tables_to_create)}")
        logger.info(f"{'='*80}\n")
        
        for table_name in tables_to_create:
            if table_name not in FIELD_TABLES:
                logger.warning(f"⚠️  跳过无效表名: {table_name}")
                continue
            
            logger.info(f"创建表: {table_name}")
            
            # 创建UNLOGGED表（无WAL日志，极速插入）
            cursor.execute(f"""
                CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                    corpusid BIGINT PRIMARY KEY,
                    data JSONB,
                    insert_time TIMESTAMP DEFAULT NOW(),
                    update_time TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # 禁用autovacuum（批量插入期间）
            cursor.execute(f"ALTER TABLE {table_name} SET (autovacuum_enabled = false)")
            
            logger.info(f"  ✓ {table_name} 创建成功（UNLOGGED）")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"✓ 表创建完成 ({len(tables_to_create)}/{len(FIELD_TABLES)})")
        logger.info(f"{'='*80}\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"创建表失败: {e}")
        sys.exit(1)




def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='初始化数据库和表',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  # 创建所有表
  python scripts/init_database.py --init
  
  # 只创建指定的表（用于分布式处理）
  python scripts/init_database.py --init --tables embeddings_specter_v1 s2orc
  
  # 使用机器配置（推荐）
  python scripts/init_database.py --init --machine machine1
        """
    )
    parser.add_argument('--init', action='store_true', help='创建数据库和表')
    parser.add_argument('--tables', nargs='+', help='要创建的表名列表（空格分隔）')
    parser.add_argument('--machine', type=str, choices=['machine1', 'machine2', 'machine3'],
                       help='机器ID（自动加载对应的表配置）')
    
    args = parser.parse_args()
    
    if args.init:
        create_database()
        
        # 确定要创建的表
        tables = None
        if args.machine:
            # 从机器配置加载
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from machine_config import get_machine_config
            config = get_machine_config(args.machine)
            tables = config['tables']
            logger.info(f"✓ 加载机器配置: {args.machine}")
            logger.info(f"  {config['description']}")
        elif args.tables:
            tables = args.tables
        
        create_tables(tables)
        logger.info("\n✅ 数据库初始化完成！")
        
        if args.machine:
            logger.info(f"下一步：python scripts/batch_process_machine.py --machine {args.machine} --base-dir \"E:\\path\\to\\s2orc\"\n")
        else:
            logger.info("下一步：python scripts/stream_gz_to_db_optimized.py --dir \"E:\\path\" --table papers\n")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

