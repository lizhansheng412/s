#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
为 paper_ids 表添加自增字段（用于批次断点续传）
"""

import sys
import time
import logging
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.config import db_config_v2
from database.config.db_config_v2 import get_db_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def add_sequence_column():
    """为 paper_ids 添加自增字段"""
    try:
        # 连接到 machine0 数据库
        db_config = get_db_config('machine0')
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True
        cursor = conn.cursor()
        
        logger.info("="*80)
        logger.info("为 paper_ids 表添加自增字段")
        logger.info("="*80)
        logger.info(f"数据库: {db_config['database']}\n")
        
        # 检查字段是否已存在
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'paper_ids' AND column_name = 'id'
        """)
        
        if cursor.fetchone():
            logger.info("✓ 字段 'id' 已存在，跳过添加")
            cursor.close()
            conn.close()
            return
        
        # 添加自增字段（快速操作，不重写表）
        logger.info("添加自增字段: id BIGSERIAL")
        start_time = time.time()
        
        cursor.execute("ALTER TABLE paper_ids ADD COLUMN id BIGSERIAL")
        
        elapsed = time.time() - start_time
        logger.info(f"✓ 添加完成，耗时: {elapsed:.2f} 秒\n")
        
        # 获取记录数
        cursor.execute("SELECT COUNT(*) FROM paper_ids")
        count = cursor.fetchone()[0]
        logger.info(f"表记录数: {count:,}")
        
        # 创建索引（可选，用于快速按ID范围查询）
        logger.info("\n创建索引: idx_paper_ids_id")
        start_time = time.time()
        
        cursor.execute("CREATE INDEX idx_paper_ids_id ON paper_ids(id)")
        
        elapsed = time.time() - start_time
        logger.info(f"✓ 索引创建完成，耗时: {elapsed:.2f} 秒")
        
        logger.info("\n" + "="*80)
        logger.info("自增字段添加完成！")
        logger.info("="*80)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    add_sequence_column()

