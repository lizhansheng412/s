#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
为 paper_ids 表添加 merge_status 列和索引（数据导入完成后执行）
"""

import sys
import logging
import time
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.config.db_config_v2 import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def finalize_paper_ids_table():
    """添加 merge_status 列和索引（数据导入完成后执行）"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        logger.info("="*80)
        logger.info("完善 paper_ids 表结构（添加 merge_status 列和索引）")
        logger.info("="*80)
        
        # 检查表是否存在
        cursor.execute("""
            SELECT COUNT(*) FROM paper_ids
        """)
        count = cursor.fetchone()[0]
        logger.info(f"当前表中记录数: {count:,}")
        
        # 步骤1：检查 merge_status 列是否已存在
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.columns 
            WHERE table_name = 'paper_ids' AND column_name = 'merge_status'
        """)
        column_exists = cursor.fetchone()[0] > 0
        
        if not column_exists:
            logger.info("\n步骤 1/2: 添加 merge_status 计算列...")
            start_time = time.time()
            
            cursor.execute("""
                ALTER TABLE paper_ids ADD COLUMN merge_status SMALLINT GENERATED ALWAYS AS (
                    CASE
                        WHEN papers=2 OR abstracts=2 OR tldrs=2 OR s2orc=2 OR s2orc_v2=2 
                             OR citations=2 OR authors=2 OR embeddings_specter_v1=2 
                             OR embeddings_specter_v2=2 OR publication_venues=2 
                        THEN 3
                        WHEN papers=1 AND abstracts=1 AND tldrs=1 AND s2orc=1 AND s2orc_v2=1 
                             AND citations=1 AND authors=1 AND embeddings_specter_v1=1 
                             AND embeddings_specter_v2=1 AND publication_venues=1 
                        THEN 2
                        WHEN papers=0 AND abstracts=0 AND tldrs=0 AND s2orc=0 AND s2orc_v2=0 
                             AND citations=0 AND authors=0 AND embeddings_specter_v1=0 
                             AND embeddings_specter_v2=0 AND publication_venues=0 
                        THEN 0
                        ELSE 1
                    END
                ) STORED
            """)
            
            elapsed = time.time() - start_time
            logger.info(f"✓ merge_status 列添加成功，耗时: {elapsed:.2f} 秒")
        else:
            logger.info("\n步骤 1/2: merge_status 列已存在，跳过")
        
        # 步骤2：检查索引是否已存在
        cursor.execute("""
            SELECT COUNT(*) FROM pg_indexes 
            WHERE tablename = 'paper_ids' 
            AND indexname = 'idx_paper_ids_merge_status'
        """)
        index_exists = cursor.fetchone()[0] > 0
        
        if not index_exists:
            logger.info("\n步骤 2/2: 创建 merge_status 索引...")
            start_time = time.time()
            
            cursor.execute("""
                CREATE INDEX idx_paper_ids_merge_status 
                ON paper_ids(merge_status)
            """)
            
            elapsed = time.time() - start_time
            logger.info(f"✓ 索引创建成功，耗时: {elapsed:.2f} 秒")
        else:
            logger.info("\n步骤 2/2: 索引已存在，跳过")
        
        # 分析表（更新统计信息）
        logger.info("\n更新表统计信息...")
        cursor.execute("ANALYZE paper_ids")
        logger.info("✓ 统计信息更新完成")
        
        # 显示统计
        cursor.execute("""
            SELECT merge_status, COUNT(*) 
            FROM paper_ids 
            GROUP BY merge_status 
            ORDER BY merge_status
        """)
        
        logger.info("\n当前状态统计:")
        status_names = {0: '未开始', 1: '进行中', 2: '已完成', 3: '有错误'}
        for status, cnt in cursor.fetchall():
            logger.info(f"  {status_names.get(status, f'未知({status})')}: {cnt:,} 条")
        
        logger.info("\n" + "="*80)
        logger.info("表结构完善完成！")
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
        description='为 paper_ids 表添加 merge_status 列和索引（数据导入完成后执行）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用场景：
  1. 完成所有 corpusid 数据导入后执行
  2. 添加 merge_status 计算列
  3. 创建 merge_status 索引，加速后续查询
  
示例：
  python scripts/create_paper_ids_index.py
        """
    )
    
    args = parser.parse_args()
    finalize_paper_ids_table()


if __name__ == '__main__':
    main()

