#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库完成脚本 - 批量插入完成后的收尾工作
将UNLOGGED表转为LOGGED，创建索引，执行VACUUM
"""

import sys
import time
import logging
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.config.db_config_v2 import DB_CONFIG, FIELD_TABLES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def finalize_table(table_name: str):
    """完成表的收尾工作：LOGGED + 索引 + VACUUM"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        logger.info(f"\n{'='*80}")
        logger.info(f"完成表 {table_name} 的收尾工作")
        logger.info(f"{'='*80}\n")
        
        # 1. 转换为LOGGED
        logger.info("1. 转换为LOGGED（恢复数据安全）...")
        start_time = time.time()
        cursor.execute(f"ALTER TABLE {table_name} SET LOGGED")
        elapsed = time.time() - start_time
        logger.info(f"   ✓ 完成 ({elapsed:.1f}秒)")
        
        # 2. 创建索引（如果需要）
        logger.info("\n2. 创建索引...")
        
        # corpusid上的索引（主键已有，跳过）
        
        # data字段的GIN索引（可选，用于JSONB查询）
        logger.info("   创建JSONB GIN索引...")
        start_time = time.time()
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_data ON {table_name} USING GIN (data)")
        elapsed = time.time() - start_time
        logger.info(f"   ✓ 完成 ({elapsed:.1f}秒)")
        
        # 3. 启用autovacuum
        logger.info("\n3. 启用autovacuum...")
        cursor.execute(f"ALTER TABLE {table_name} SET (autovacuum_enabled = true)")
        logger.info("   ✓ 已启用")
        
        # 4. 执行VACUUM ANALYZE
        logger.info("\n4. 执行VACUUM ANALYZE（更新统计信息）...")
        start_time = time.time()
        cursor.execute(f"VACUUM ANALYZE {table_name}")
        elapsed = time.time() - start_time
        logger.info(f"   ✓ 完成 ({elapsed:.1f}秒)")
        
        # 5. 查看统计信息
        logger.info("\n5. 表统计信息:")
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logger.info(f"   记录数: {count:,}")
        
        cursor.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{table_name}'))")
        size = cursor.fetchone()[0]
        logger.info(f"   表大小: {size}")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"✓ 表 {table_name} 已完成所有收尾工作")
        logger.info(f"{'='*80}\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"收尾工作失败: {e}")
        sys.exit(1)




def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='完成表的收尾工作（批量插入后）',
        epilog="""
示例：
  # 完成单个表
  python scripts/finalize_database.py --table papers
  
  # 完成所有表
  python scripts/finalize_database.py --all
        """
    )
    
    parser.add_argument('--table', type=str, choices=FIELD_TABLES, help='目标表名')
    parser.add_argument('--all', action='store_true', help='完成所有表')
    
    args = parser.parse_args()
    
    if args.all:
        logger.info("开始完成所有表的收尾工作...\n")
        for table in FIELD_TABLES:
            finalize_table(table)
        logger.info("\n✅ 所有表已完成收尾工作！\n")
    elif args.table:
        finalize_table(args.table)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

