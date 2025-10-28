#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一表索引优化工具
支持所有表的主键和索引移除/恢复，极速导入优化
"""

import sys
import time
import logging
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.config import db_config_v2
from database.config.db_config_v2 import FIELD_TABLES, get_db_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TABLE_CONFIG = {
    'paper_ids': {
        'primary_key': 'corpusid',
        'indexes': []
    },
    'citations': {
        'primary_key': 'id',
        'indexes': ['idx_citations_citingcorpusid']
    },
    'authors': {
        'primary_key': 'authorid',
        'indexes': []
    },
    'publication_venues': {
        'primary_key': 'publicationvenueid',
        'indexes': []
    },
}

DEFAULT_CONFIG = {
    'primary_key': 'corpusid',
    'indexes': []
}


def get_table_config(table_name: str) -> dict:
    """获取表配置"""
    return TABLE_CONFIG.get(table_name, DEFAULT_CONFIG)


def remove_indexes(table_name: str):
    """移除表的所有索引（主键+额外索引）"""
    if table_name not in FIELD_TABLES:
        logger.error(f"无效的表名: {table_name}")
        logger.info(f"支持的表: {', '.join(FIELD_TABLES)}")
        sys.exit(1)
    
    try:
        conn = psycopg2.connect(**db_config_v2.DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        logger.info("="*80)
        logger.info(f"移除 {table_name} 表的所有索引（极速导入模式）")
        logger.info("="*80)
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logger.info(f"当前记录数: {count:,}")
        
        config = get_table_config(table_name)
        removed_count = 0
        
        cursor.execute("""
            SELECT constraint_name 
            FROM information_schema.table_constraints 
            WHERE table_name = %s AND constraint_type = 'PRIMARY KEY'
        """, (table_name,))
        pk_exists = cursor.fetchone()
        
        if pk_exists:
            constraint_name = pk_exists[0]
            logger.info(f"\n移除主键约束: {constraint_name}")
            cursor.execute(f"ALTER TABLE {table_name} DROP CONSTRAINT {constraint_name}")
            logger.info(f"  ✓ 主键已删除")
            removed_count += 1
        else:
            logger.info("\n主键约束不存在")
        
        for idx_name in config['indexes']:
            cursor.execute("""
                SELECT indexname FROM pg_indexes 
                WHERE tablename = %s AND indexname = %s
            """, (table_name, idx_name))
            
            if cursor.fetchone():
                logger.info(f"\n移除索引: {idx_name}")
                cursor.execute(f"DROP INDEX IF EXISTS {idx_name}")
                logger.info(f"  ✓ 索引已删除")
                removed_count += 1
            else:
                logger.info(f"\n索引不存在: {idx_name}")
        
        logger.info("\n" + "="*80)
        if removed_count > 0:
            logger.info(f"✓ 已移除 {removed_count} 个约束/索引")
            logger.info("⚡ 现在可以极速插入（预计 50,000+ 条/秒）")
        else:
            logger.info("✓ 无需移除索引")
        logger.info("="*80)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def restore_indexes(table_name: str):
    """恢复表的所有索引（智能去重+添加主键+额外索引）"""
    if table_name not in FIELD_TABLES:
        logger.error(f"无效的表名: {table_name}")
        logger.info(f"支持的表: {', '.join(FIELD_TABLES)}")
        sys.exit(1)
    
    try:
        conn = psycopg2.connect(**db_config_v2.DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        logger.info("="*80)
        logger.info(f"恢复 {table_name} 表的所有索引（智能去重并建立索引）")
        logger.info("="*80)
        
        config = get_table_config(table_name)
        pk_column = config['primary_key']
        
        # 检查主键是否已存在
        cursor.execute("""
            SELECT constraint_name 
            FROM information_schema.table_constraints 
            WHERE table_name = %s AND constraint_type = 'PRIMARY KEY'
        """, (table_name,))
        pk_exists = cursor.fetchone()
        
        if pk_exists:
            logger.info(f"\n✓ 表已有主键约束: {pk_exists[0]}")
            logger.info(f"  主键字段: {pk_column}")
            logger.info(f"  跳过索引恢复操作（表已完成处理）")
            
            # 更新统计信息
            logger.info(f"\n更新表统计信息...")
            cursor.execute(f"ANALYZE {table_name}")
            logger.info(f"  ✓ 完成")
            
            logger.info("\n" + "="*80)
            logger.info("表已完成处理！")
            logger.info("="*80)
            return
        
        logger.info(f"\n策略: 直接尝试添加主键（快速失败模式）")
        logger.info(f"  如果成功 → 无重复，完成")
        logger.info(f"  如果失败 → 有重复，执行去重\n")
        
        logger.info(f"步骤 1/3: 尝试添加主键 ({pk_column})...")
        start_time = time.time()
        
        try:
            cursor.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column})")
            elapsed = time.time() - start_time
            
            logger.info(f"  ✓ 主键添加成功！数据无重复")
            logger.info(f"    耗时: {elapsed:.2f} 秒")
            
            # 恢复额外索引（如果有）
            if config['indexes']:
                logger.info(f"\n步骤 2/3: 恢复额外索引...")
                for idx_name in config['indexes']:
                    idx_start = time.time()
                    if table_name == 'citations' and idx_name == 'idx_citations_citingcorpusid':
                        cursor.execute(f"""
                            CREATE INDEX IF NOT EXISTS {idx_name} 
                            ON {table_name}(citingcorpusid)
                        """)
                        idx_elapsed = time.time() - idx_start
                        logger.info(f"  ✓ 索引 {idx_name} 创建完成，耗时: {idx_elapsed:.2f} 秒")
            else:
                logger.info(f"\n步骤 2/3: 无需创建额外索引")
            
            logger.info(f"\n步骤 3/3: 更新统计信息...")
            cursor.execute(f"ANALYZE {table_name}")
            logger.info(f"  ✓ 完成")
            
        except psycopg2.errors.UniqueViolation:
            elapsed = time.time() - start_time
            conn.rollback()
            
            logger.info(f"  ✗ 检测到重复数据（耗时 {elapsed:.2f} 秒）")
            logger.info(f"\n步骤 2/3: 去重并添加主键...")
            start_time = time.time()
            
            temp_table = f"{table_name}_temp_dedup"
            
            logger.info(f"  1. 创建去重临时表: {temp_table}")
            cursor.execute(f"""
                CREATE TABLE {temp_table} AS 
                SELECT DISTINCT ON ({pk_column}) *
                FROM {table_name}
                ORDER BY {pk_column}
            """)
            
            logger.info(f"  2. 删除原表")
            cursor.execute(f"DROP TABLE {table_name}")
            
            logger.info(f"  3. 重命名临时表")
            cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
            
            logger.info(f"  4. 添加主键")
            cursor.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column})")
            
            elapsed = time.time() - start_time
            logger.info(f"  ✓ 去重完成，耗时: {elapsed:.2f} 秒")
            
            # 恢复额外索引（如果有）
            if config['indexes']:
                logger.info(f"\n步骤 3/3: 恢复额外索引...")
                for idx_name in config['indexes']:
                    idx_start = time.time()
                    if table_name == 'citations' and idx_name == 'idx_citations_citingcorpusid':
                        cursor.execute(f"""
                            CREATE INDEX IF NOT EXISTS {idx_name} 
                            ON {table_name}(citingcorpusid)
                        """)
                        idx_elapsed = time.time() - idx_start
                        logger.info(f"  ✓ 索引 {idx_name} 创建完成，耗时: {idx_elapsed:.2f} 秒")
            
            cursor.execute(f"ANALYZE {table_name}")
            logger.info(f"  ✓ 统计信息更新完成")
        
        logger.info("\n" + "="*80)
        logger.info("索引恢复完成！")
        logger.info("="*80)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def remove_indexes_batch(tables: list):
    """批量移除多个表的索引"""
    logger.info("\n" + "="*80)
    logger.info(f"批量移除索引：{len(tables)} 个表")
    logger.info("="*80)
    
    for i, table_name in enumerate(tables, 1):
        logger.info(f"\n[{i}/{len(tables)}] 处理表: {table_name}")
        remove_indexes(table_name)
    
    logger.info("\n" + "="*80)
    logger.info(f"✓ 批量操作完成：已处理 {len(tables)} 个表")
    logger.info("="*80)


def restore_indexes_batch(tables: list):
    """批量恢复多个表的索引"""
    logger.info("\n" + "="*80)
    logger.info(f"批量恢复索引：{len(tables)} 个表")
    logger.info("="*80)
    
    for i, table_name in enumerate(tables, 1):
        logger.info(f"\n[{i}/{len(tables)}] 处理表: {table_name}")
        restore_indexes(table_name)
    
    logger.info("\n" + "="*80)
    logger.info(f"✓ 批量操作完成：已处理 {len(tables)} 个表")
    logger.info("="*80)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='统一表索引优化工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例：

单表操作：
  python scripts/optimize_table_indexes.py --table papers --remove-indexes
  python scripts/optimize_table_indexes.py --table papers --restore-indexes

批量操作（指定多个表）：
  python scripts/optimize_table_indexes.py --tables papers abstracts authors --remove-indexes
  python scripts/optimize_table_indexes.py --tables papers abstracts authors --restore-indexes

机器配置批量操作：
  python scripts/optimize_table_indexes.py --machine machine3 --remove-indexes
  python scripts/optimize_table_indexes.py --machine machine3 --restore-indexes

支持的表：
  papers, abstracts, tldrs, authors, publication_venues, 
  citations, paper_ids, s2orc, s2orc_v2, 
  embeddings_specter_v1, embeddings_specter_v2
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--table', type=str, help='单个表名')
    group.add_argument('--tables', nargs='+', help='多个表名')
    group.add_argument('--machine', type=str, 
                      choices=['machine1', 'machine2', 'machine3', 'machine0'],
                      help='机器ID（处理该机器的所有表）')
    
    parser.add_argument('--db-machine', type=str,
                       choices=['machine1', 'machine2', 'machine3', 'machine0'],
                       help='指定数据库（用于单表/多表操作时指定数据库连接）')
    
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--remove-indexes', action='store_true',
                       help='移除索引（开始导入前）')
    action.add_argument('--restore-indexes', action='store_true',
                       help='恢复索引（导入完成后）')
    
    args = parser.parse_args()
    
    # 处理数据库配置
    if args.machine:
        # 使用 --machine 时，自动设置数据库
        db_config = get_db_config(args.machine)
        db_config_v2.DB_CONFIG.update(db_config)
        
        from machine_config import get_machine_config
        config = get_machine_config(args.machine)
        tables = config['tables']
        logger.info(f"机器配置: {args.machine}")
        logger.info(f"数据库: {db_config_v2.DB_CONFIG['database']}")
        logger.info(f"描述: {config['description']}")
        logger.info(f"包含表: {', '.join(tables)}\n")
    else:
        # 单表或多表操作
        if args.db_machine:
            # 如果指定了 --db-machine，更新数据库配置
            db_config = get_db_config(args.db_machine)
            db_config_v2.DB_CONFIG.update(db_config)
            logger.info(f"数据库: {db_config_v2.DB_CONFIG['database']} (机器: {args.db_machine})\n")
        
        if args.table:
            tables = [args.table]
        elif args.tables:
            tables = args.tables
    
    if args.remove_indexes:
        if len(tables) == 1:
            remove_indexes(tables[0])
        else:
            remove_indexes_batch(tables)
    elif args.restore_indexes:
        if len(tables) == 1:
            restore_indexes(tables[0])
        else:
            restore_indexes_batch(tables)


if __name__ == '__main__':
    main()

