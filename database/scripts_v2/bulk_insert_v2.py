#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量插入脚本 V2 - 分表方案
策略：INSERT优先（最快），失败重跑时用UPSERT
"""

import sys
import json
import time
import logging
from pathlib import Path
from typing import List, Tuple, Dict, Any
from io import StringIO

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2.extensions import connection

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from database.config.db_config_v2 import (
    DB_CONFIG,
    MAIN_TABLE,
    FIELD_TABLES,
    BULK_INSERT_CONFIG,
    BULK_UPSERT_CONFIG,
    LOG_CONFIG,
)

# 配置日志
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['log_level']),
    format=LOG_CONFIG['log_format'],
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_CONFIG['log_file'], encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class BulkInserterV2:
    """批量插入器 V2 - 支持INSERT和UPSERT两种模式"""
    
    def __init__(self, conn: connection):
        self.conn = conn
        self.batch_size = BULK_INSERT_CONFIG['batch_size']
        self.commit_interval = BULK_INSERT_CONFIG['commit_interval']
        
        logger.info(f"初始化批量插入器 V2: 批次={self.batch_size}")
    
    def insert_to_registry(self, corpusids: List[int], is_papers: bool = False, use_upsert: bool = False):
        """
        插入到主表 corpus_registry
        
        Args:
            corpusids: corpusid列表
            is_papers: 是否标记为papers表中的数据
            use_upsert: 是否使用UPSERT模式
        """
        if not corpusids:
            return 0
        
        start_time = time.time()
        
        try:
            if use_upsert:
                # UPSERT模式
                execute_values(
                    self.conn.cursor(),
                    f"""
                    INSERT INTO {MAIN_TABLE} (corpusid, is_exist_papers)
                    VALUES %s
                    ON CONFLICT (corpusid) DO UPDATE SET
                        is_exist_papers = EXCLUDED.is_exist_papers OR {MAIN_TABLE}.is_exist_papers,
                        update_time = NOW()
                    """,
                    [(cid, is_papers) for cid in corpusids],
                    template="(%s, %s)",
                    page_size=self.batch_size
                )
            else:
                # INSERT模式（COPY）
                buffer = StringIO()
                for cid in corpusids:
                    buffer.write(f"{cid}\t{is_papers}\t\\N\t\\N\n")
                buffer.seek(0)
                
                with self.conn.cursor() as cur:
                    cur.copy_expert(
                        f"""
                        COPY {MAIN_TABLE} (corpusid, is_exist_papers, insert_time, update_time)
                        FROM STDIN WITH (FORMAT TEXT, NULL '\\N', DELIMITER E'\\t')
                        """,
                        buffer
                    )
            
            elapsed = time.time() - start_time
            logger.info(f"✓ 主表插入 {len(corpusids):,} 条 (耗时 {elapsed:.2f}秒)")
            return len(corpusids)
        
        except Exception as e:
            logger.error(f"✗ 主表插入失败: {e}")
            raise
    
    def insert_to_field_table(
        self, 
        table_name: str, 
        records: List[Tuple[int, Dict[str, Any]]], 
        use_upsert: bool = False
    ):
        """
        插入到字段表
        
        Args:
            table_name: 表名
            records: [(corpusid, json_data), ...]
            use_upsert: 是否使用UPSERT模式
        """
        if not records:
            return 0
        
        if table_name not in FIELD_TABLES:
            raise ValueError(f"无效的表名: {table_name}")
        
        start_time = time.time()
        
        try:
            if use_upsert:
                # UPSERT模式
                execute_values(
                    self.conn.cursor(),
                    f"""
                    INSERT INTO {table_name} (corpusid, data)
                    VALUES %s
                    ON CONFLICT (corpusid) DO UPDATE SET
                        data = EXCLUDED.data,
                        update_time = NOW()
                    WHERE {table_name}.data IS DISTINCT FROM EXCLUDED.data
                    """,
                    [(cid, json.dumps(data, ensure_ascii=False)) for cid, data in records],
                    template="(%s, %s::jsonb)",
                    page_size=BULK_UPSERT_CONFIG['batch_size']
                )
            else:
                # INSERT模式（COPY）
                buffer = StringIO()
                for cid, data in records:
                    json_str = json.dumps(data, ensure_ascii=False).replace('\\', '\\\\').replace('\n', '\\n').replace('\t', '\\t')
                    buffer.write(f"{cid}\t{json_str}\t\\N\t\\N\n")
                buffer.seek(0)
                
                with self.conn.cursor() as cur:
                    cur.copy_expert(
                        f"""
                        COPY {table_name} (corpusid, data, insert_time, update_time)
                        FROM STDIN WITH (FORMAT TEXT, NULL '\\N', DELIMITER E'\\t')
                        """,
                        buffer
                    )
            
            elapsed = time.time() - start_time
            rate = len(records) / elapsed if elapsed > 0 else 0
            mode = "UPSERT" if use_upsert else "INSERT"
            logger.info(f"✓ {table_name}: {mode} {len(records):,} 条 ({rate:.0f}条/秒)")
            return len(records)
        
        except Exception as e:
            logger.error(f"✗ {table_name} 插入失败: {e}")
            raise
    
    def batch_insert_from_file(
        self,
        table_name: str,
        jsonl_file: str,
        corpusid_key: str = 'corpusid',
        use_upsert: bool = False,
        update_registry: bool = True
    ):
        """
        从JSONL文件批量插入
        
        Args:
            table_name: 目标表名
            jsonl_file: JSONL文件路径
            corpusid_key: corpusid字段名
            use_upsert: 是否使用UPSERT模式
            update_registry: 是否同步更新主表
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"批量插入: {table_name}")
        logger.info(f"文件: {jsonl_file}")
        logger.info(f"模式: {'UPSERT' if use_upsert else 'INSERT'}")
        logger.info(f"{'='*80}")
        
        start_time = time.time()
        total_inserted = 0
        batch = []
        corpusids_set = set()
        
        try:
            self.conn.autocommit = False
            
            # 禁用触发器（INSERT模式）
            if not use_upsert:
                with self.conn.cursor() as cur:
                    cur.execute(f"SELECT disable_table_triggers('{table_name}')")
                logger.info("✓ 触发器已禁用")
            
            # 读取并批量插入
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line_no, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        corpusid = data.get(corpusid_key)
                        
                        if not corpusid:
                            continue
                        
                        corpusid = int(corpusid)
                        corpusids_set.add(corpusid)
                        
                        # 移除corpusid字段，剩余作为data
                        json_data = {k: v for k, v in data.items() if k != corpusid_key}
                        batch.append((corpusid, json_data))
                        
                        # 批次满了，执行插入
                        if len(batch) >= self.batch_size:
                            inserted = self.insert_to_field_table(table_name, batch, use_upsert)
                            total_inserted += inserted
                            batch = []
                            
                            # 定期提交
                            if total_inserted % self.commit_interval == 0:
                                self.conn.commit()
                                logger.info(f"  进度: {total_inserted:,} 条已插入")
                    
                    except json.JSONDecodeError:
                        logger.warning(f"  跳过无效JSON (行{line_no})")
                        continue
            
            # 插入剩余数据
            if batch:
                inserted = self.insert_to_field_table(table_name, batch, use_upsert)
                total_inserted += inserted
            
            self.conn.commit()
            
            # 同步更新主表
            if update_registry and corpusids_set:
                logger.info(f"同步主表: {len(corpusids_set):,} 个corpusid")
                is_papers = (table_name == 'papers')
                self.insert_to_registry(list(corpusids_set), is_papers, use_upsert)
                self.conn.commit()
            
            # 启用触发器
            if not use_upsert:
                with self.conn.cursor() as cur:
                    cur.execute(f"SELECT enable_table_triggers('{table_name}')")
                logger.info("✓ 触发器已启用")
            
            elapsed = time.time() - start_time
            rate = total_inserted / elapsed if elapsed > 0 else 0
            
            logger.info(f"\n{'='*80}")
            logger.info(f"✓ 插入完成！")
            logger.info(f"  表: {table_name}")
            logger.info(f"  总数: {total_inserted:,} 条")
            logger.info(f"  耗时: {elapsed:.2f}秒 ({elapsed/60:.1f}分钟)")
            logger.info(f"  速率: {rate:.0f} 条/秒")
            logger.info(f"{'='*80}\n")
            
            return total_inserted
        
        except Exception as e:
            logger.error(f"✗ 批量插入失败: {e}")
            self.conn.rollback()
            
            # 确保启用触发器
            if not use_upsert:
                try:
                    with self.conn.cursor() as cur:
                        cur.execute(f"SELECT enable_table_triggers('{table_name}')")
                    self.conn.commit()
                except:
                    pass
            
            raise
        
        finally:
            self.conn.autocommit = True


def create_connection() -> connection:
    """创建数据库连接"""
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
    
    parser = argparse.ArgumentParser(
        description='批量插入数据到 S2ORC 数据库 V2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 首次插入papers数据（INSERT模式，最快）
  python database/scripts_v2/bulk_insert_v2.py --table papers --file papers.jsonl
  
  # 失败重跑（UPSERT模式）
  python database/scripts_v2/bulk_insert_v2.py --table papers --file papers.jsonl --upsert
  
  # 插入其他字段表
  python database/scripts_v2/bulk_insert_v2.py --table abstracts --file abstracts.jsonl
        """
    )
    
    parser.add_argument('--table', type=str, required=True,
                       choices=FIELD_TABLES,
                       help='目标表名')
    parser.add_argument('--file', type=str, required=True,
                       help='JSONL文件路径')
    parser.add_argument('--corpusid-key', type=str, default='corpusid',
                       help='corpusid字段名（默认: corpusid）')
    parser.add_argument('--upsert', action='store_true',
                       help='使用UPSERT模式（失败重跑时用）')
    parser.add_argument('--no-registry', action='store_true',
                       help='不同步更新主表')
    
    args = parser.parse_args()
    
    try:
        conn = create_connection()
        try:
            inserter = BulkInserterV2(conn)
            count = inserter.batch_insert_from_file(
                table_name=args.table,
                jsonl_file=args.file,
                corpusid_key=args.corpusid_key,
                use_upsert=args.upsert,
                update_registry=not args.no_registry
            )
            
            logger.info(f"✓ 成功插入 {count:,} 条记录到表 {args.table}")
        
        finally:
            conn.close()
    
    except KeyboardInterrupt:
        logger.warning("\n✗ 用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ 批量插入失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

