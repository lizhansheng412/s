#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询辅助脚本 V2 - 分表方案
提供高效的查询接口
"""

import sys
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

import psycopg2
from psycopg2 import sql, pool
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from database.config.db_config_v2 import (
    DB_CONFIG,
    POOL_CONFIG,
    MAIN_TABLE,
    FIELD_TABLES,
    LOG_CONFIG,
    get_corpusid_partition,
)

# 配置日志
logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['log_level']),
    format=LOG_CONFIG['log_format'],
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class ConnectionPool:
    """数据库连接池"""
    
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def _ensure_pool(self):
        if self._pool is None:
            try:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=POOL_CONFIG['min_size'],
                    maxconn=POOL_CONFIG['max_size'],
                    **DB_CONFIG
                )
                logger.info(f"✓ 连接池初始化: {POOL_CONFIG['min_size']}-{POOL_CONFIG['max_size']} 连接")
            except Exception as e:
                logger.error(f"✗ 连接池初始化失败: {e}")
                raise
    
    @contextmanager
    def get_connection(self):
        self._ensure_pool()
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)
    
    def close_all(self):
        if self._pool:
            self._pool.closeall()


# 全局连接池
connection_pool = ConnectionPool()


class CorpusQueryV2:
    """语料库查询类 V2"""
    
    def __init__(self, conn: Optional[connection] = None):
        self.conn = conn
        self.use_pool = conn is None
    
    @contextmanager
    def _get_connection(self):
        if self.use_pool:
            with connection_pool.get_connection() as conn:
                yield conn
        else:
            yield self.conn
    
    def get_from_table(
        self, 
        table_name: str, 
        corpusid: int
    ) -> Optional[Dict[str, Any]]:
        """
        从指定表查询单条记录
        
        Args:
            table_name: 表名
            corpusid: corpusid
        """
        if table_name not in FIELD_TABLES:
            raise ValueError(f"无效的表名: {table_name}")
        
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = sql.SQL("SELECT * FROM {} WHERE corpusid = %s").format(
                    sql.Identifier(table_name)
                )
                cur.execute(query, (corpusid,))
                result = cur.fetchone()
                return dict(result) if result else None
    
    def get_multi_fields(
        self,
        corpusid: int,
        tables: List[str]
    ) -> Dict[str, Any]:
        """
        从多个表查询同一corpusid的数据
        
        Args:
            corpusid: corpusid
            tables: 表名列表
        
        Returns:
            {'papers': {...}, 'abstracts': {...}, ...}
        """
        result = {'corpusid': corpusid}
        
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for table_name in tables:
                    if table_name not in FIELD_TABLES:
                        continue
                    
                    query = sql.SQL("SELECT data FROM {} WHERE corpusid = %s").format(
                        sql.Identifier(table_name)
                    )
                    cur.execute(query, (corpusid,))
                    row = cur.fetchone()
                    
                    result[table_name] = row['data'] if row else None
        
        return result
    
    def exists(self, corpusid: int, table_name: Optional[str] = None) -> bool:
        """
        检查corpusid是否存在
        
        Args:
            corpusid: corpusid
            table_name: 表名（None表示检查主表）
        """
        target_table = table_name if table_name else MAIN_TABLE
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                query = sql.SQL(
                    "SELECT EXISTS(SELECT 1 FROM {} WHERE corpusid = %s)"
                ).format(sql.Identifier(target_table))
                
                cur.execute(query, (corpusid,))
                return cur.fetchone()[0]
    
    def count_table(self, table_name: str) -> int:
        """统计表记录数"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                query = sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(table_name)
                )
                cur.execute(query)
                return cur.fetchone()[0]
    
    def get_partition_info(self, corpusid: int) -> Dict[str, Any]:
        """获取分区信息"""
        partition_id = get_corpusid_partition(corpusid)
        
        return {
            'corpusid': corpusid,
            'partition_id': partition_id,
            'partition_name_example': f'papers_p{partition_id}',
        }


class CorpusStatsV2:
    """语料库统计类 V2"""
    
    def __init__(self, conn: Optional[connection] = None):
        self.conn = conn
        self.use_pool = conn is None
    
    @contextmanager
    def _get_connection(self):
        if self.use_pool:
            with connection_pool.get_connection() as conn:
                yield conn
        else:
            yield self.conn
    
    def get_all_stats(self) -> Dict[str, int]:
        """获取所有表的统计信息"""
        stats = {}
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # 主表统计
                cur.execute(f"SELECT COUNT(*) FROM {MAIN_TABLE}")
                stats['corpus_registry'] = cur.fetchone()[0]
                
                # 字段表统计
                for table in FIELD_TABLES:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    stats[table] = cur.fetchone()[0]
        
        return stats
    
    def get_table_sizes(self) -> Dict[str, Dict[str, str]]:
        """获取表大小"""
        sizes = {}
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                for table in [MAIN_TABLE] + FIELD_TABLES:
                    cur.execute(
                        """
                        SELECT 
                            pg_size_pretty(pg_total_relation_size(%s)) AS total,
                            pg_size_pretty(pg_relation_size(%s)) AS table,
                            pg_size_pretty(pg_indexes_size(%s)) AS indexes
                        """,
                        (table, table, table)
                    )
                    result = cur.fetchone()
                    sizes[table] = {
                        'total': result[0],
                        'table': result[1],
                        'indexes': result[2]
                    }
        
        return sizes
    
    def get_database_stats(self) -> Dict[str, str]:
        """获取数据库整体统计"""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM get_database_stats()")
                results = cur.fetchall()
                return {row['metric']: row['value'] for row in results}


def create_connection() -> connection:
    """创建单个数据库连接"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✓ 数据库连接成功")
        return conn
    except Exception as e:
        logger.error(f"✗ 数据库连接失败: {e}")
        raise


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='查询 S2ORC 数据库 V2')
    parser.add_argument('--corpusid', type=int, help='查询指定corpusid')
    parser.add_argument('--table', type=str, choices=FIELD_TABLES, help='查询指定表')
    parser.add_argument('--stats', action='store_true', help='显示统计信息')
    
    args = parser.parse_args()
    
    try:
        if args.stats:
            stats = CorpusStatsV2()
            
            logger.info("\n数据库统计信息:")
            logger.info("=" * 80)
            
            # 记录数统计
            all_stats = stats.get_all_stats()
            logger.info("\n记录数统计:")
            for table, count in all_stats.items():
                logger.info(f"  {table:30s}: {count:,}")
            
            # 表大小统计
            sizes = stats.get_table_sizes()
            logger.info("\n表大小统计:")
            for table, size_info in sizes.items():
                logger.info(f"  {table}:")
                logger.info(f"    总大小: {size_info['total']}")
                logger.info(f"    表大小: {size_info['table']}")
                logger.info(f"    索引大小: {size_info['indexes']}")
            
            # 数据库统计
            db_stats = stats.get_database_stats()
            logger.info("\n数据库整体统计:")
            for metric, value in db_stats.items():
                logger.info(f"  {metric}: {value}")
            
            logger.info("=" * 80)
        
        elif args.corpusid:
            query = CorpusQueryV2()
            
            if args.table:
                # 查询指定表
                result = query.get_from_table(args.table, args.corpusid)
                if result:
                    logger.info(f"\ncorpusid={args.corpusid} 在 {args.table} 中的数据:")
                    logger.info(json.dumps(result, indent=2, ensure_ascii=False, default=str))
                else:
                    logger.info(f"corpusid={args.corpusid} 在 {args.table} 中不存在")
            else:
                # 查询所有表
                result = query.get_multi_fields(args.corpusid, FIELD_TABLES)
                logger.info(f"\ncorpusid={args.corpusid} 的完整数据:")
                logger.info(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        
        else:
            parser.print_help()
    
    except KeyboardInterrupt:
        logger.warning("\n✗ 用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ 查询失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        connection_pool.close_all()


if __name__ == '__main__':
    main()

