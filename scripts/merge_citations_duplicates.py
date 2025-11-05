#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Citations 表特殊去重与合并脚本
基于 citingcorpusid 去重，将重复记录的 data 字段合并为 JSON 数组
最终表结构: citingcorpusid (主键), data (TEXT)
"""

import sys
import time
import logging
from pathlib import Path

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent.parent))
import db_config
from db_config import get_db_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def merge_citations_duplicates():
    """
    合并 citations 表中相同 citingcorpusid 的记录
    将 data 字段合并为 JSON 数组
    """
    try:
        conn = psycopg2.connect(**db_config.DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        logger.info("="*80)
        logger.info("Citations 表去重与合并")
        logger.info("="*80)
        
        logger.info(f"\n策略: 直接尝试调整表结构（快速失败模式）")
        logger.info(f"  尝试删除冗余字段并添加主键")
        logger.info(f"  如果失败 → 说明有重复，执行合并\n")
        
        logger.info(f"步骤 1/2: 尝试调整表结构...")
        start_time = time.time()
        
        try:
            # 删除冗余字段
            cursor.execute("ALTER TABLE citations DROP COLUMN id")
            cursor.execute("ALTER TABLE citations DROP COLUMN insert_time")
            cursor.execute("ALTER TABLE citations DROP COLUMN update_time")
            logger.info(f"  ✓ 删除字段: id, insert_time, update_time")
            
            # 尝试添加主键
            cursor.execute("ALTER TABLE citations ADD PRIMARY KEY (citingcorpusid)")
            elapsed = time.time() - start_time
            
            logger.info(f"  ✓ citingcorpusid 设为主键")
            logger.info(f"  ✓ 数据无重复，完成！")
            logger.info(f"    耗时: {elapsed:.2f} 秒")
            
        except psycopg2.errors.UniqueViolation:
            elapsed = time.time() - start_time
            conn.rollback()
            
            logger.info(f"  ✗ 检测到重复数据（耗时 {elapsed:.2f} 秒）")
            logger.info(f"\n步骤 2/2: 合并重复记录（data 字段合并为 JSON 数组）...")
            start_time = time.time()
            
            temp_table = "citations_temp_merged"
            
            logger.info(f"  1. 创建合并临时表: {temp_table}")
            cursor.execute(f"""
                CREATE TABLE {temp_table} AS
                SELECT 
                    citingcorpusid,
                    CASE 
                        WHEN COUNT(*) = 1 THEN MIN(data)
                        ELSE '[' || STRING_AGG(data, ',') || ']'
                    END as data
                FROM citations
                GROUP BY citingcorpusid
            """)
            
            logger.info(f"  2. 删除原表")
            cursor.execute("DROP TABLE citations")
            
            logger.info(f"  3. 重命名临时表")
            cursor.execute(f"ALTER TABLE {temp_table} RENAME TO citations")
            
            logger.info(f"  4. 添加主键")
            cursor.execute("ALTER TABLE citations ADD PRIMARY KEY (citingcorpusid)")
            
            elapsed = time.time() - start_time
            logger.info(f"  ✓ 合并完成，耗时: {elapsed:.2f} 秒")
            
            cursor.execute("ANALYZE citations")
            logger.info(f"  ✓ 统计信息更新完成")
        
        logger.info("\n" + "="*80)
        logger.info("Citations 表合并与索引恢复完成！")
        logger.info("="*80)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Citations 表去重与合并（data字段合并为JSON数组）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例：
  # 默认连接到 machine0 数据库 (s2orc_d0)
  python scripts/merge_citations_duplicates.py
  
  # 指定机器（连接到相应数据库）
  python scripts/merge_citations_duplicates.py --machine machine3
  
说明：
  - 基于 citingcorpusid 去重
  - 相同 citingcorpusid 的 data 字段合并为 JSON 数组
  - 删除字段: id, insert_time, update_time
  - 将 citingcorpusid 设为主键（自动创建索引）
  - 最终表结构: citingcorpusid (主键), data (TEXT)
        """
    )
    
    parser.add_argument('--machine', type=str,
                       choices=['machine1', 'machine2', 'machine3', 'machine0'],
                       help='机器ID（指定数据库连接）')
    
    args = parser.parse_args()
    
    if args.machine:
        db_config = get_db_config(args.machine)
        db_config.DB_CONFIG.update(db_config)
        logger.info(f"数据库: {db_config.DB_CONFIG['database']} (机器: {args.machine})\n")
    else:
        logger.info(f"数据库: {db_config.DB_CONFIG['database']}\n")
    
    merge_citations_duplicates()


if __name__ == '__main__':
    main()

