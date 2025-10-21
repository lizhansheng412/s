#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库初始化脚本 V2 - 分表方案
适配硬件：32GB内存 + 16核CPU
"""

import sys
import time
import logging
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from database.config.db_config_v2 import (
    DB_CONFIG,
    SQL_SCRIPTS,
    MAIN_TABLE,
    FIELD_TABLES,
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


class DatabaseInitializerV2:
    """数据库初始化器 V2"""
    
    def __init__(self, drop_existing: bool = False):
        """
        初始化
        
        Args:
            drop_existing: 是否删除已存在的数据库
        """
        self.db_config = DB_CONFIG.copy()
        self.db_name = self.db_config['database']
        self.drop_existing = drop_existing
        
        logger.info("=" * 80)
        logger.info("S2ORC 语料库数据库初始化工具 V2 - 分表方案")
        logger.info("=" * 80)
        logger.info(f"目标数据库: {self.db_name}")
        logger.info(f"主机: {self.db_config['host']}:{self.db_config['port']}")
        logger.info(f"用户: {self.db_config['user']}")
        logger.info(f"表结构: 1个主表 + 11个字段表 × 64分区 = 768个分区表")
        if drop_existing:
            logger.warning("警告: 将删除已存在的数据库！")
        logger.info("=" * 80)
    
    def connect_postgres(self):
        """连接到postgres数据库"""
        config = self.db_config.copy()
        config['database'] = 'postgres'
        
        try:
            conn = psycopg2.connect(**config)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            logger.info("✓ 已连接到 PostgreSQL 服务器")
            return conn
        except Exception as e:
            logger.error(f"✗ 连接失败: {e}")
            raise
    
    def connect_target_db(self):
        """连接到目标数据库"""
        try:
            conn = psycopg2.connect(**self.db_config)
            logger.info(f"✓ 已连接到数据库: {self.db_name}")
            return conn
        except Exception as e:
            logger.error(f"✗ 连接数据库失败: {e}")
            raise
    
    def database_exists(self, conn) -> bool:
        """检查数据库是否存在"""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.db_name,)
            )
            exists = cur.fetchone() is not None
        
        if exists:
            logger.info(f"数据库 '{self.db_name}' 已存在")
        else:
            logger.info(f"数据库 '{self.db_name}' 不存在")
        
        return exists
    
    def create_database(self, conn):
        """创建数据库"""
        logger.info(f"\n正在创建数据库: {self.db_name}...")
        
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE DATABASE {self.db_name}')
            
            logger.info(f"✓ 数据库 '{self.db_name}' 创建成功")
        except Exception as e:
            logger.error(f"✗ 创建数据库失败: {e}")
            raise
    
    def drop_database(self, conn):
        """删除数据库"""
        logger.warning(f"\n正在删除数据库: {self.db_name}...")
        
        try:
            with conn.cursor() as cur:
                # 终止所有连接
                cur.execute(f"""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = '{self.db_name}'
                    AND pid <> pg_backend_pid()
                """)
                
                # 删除数据库
                cur.execute(f'DROP DATABASE IF EXISTS {self.db_name}')
            
            logger.info(f"✓ 数据库 '{self.db_name}' 已删除")
        except Exception as e:
            logger.error(f"✗ 删除数据库失败: {e}")
            raise
    
    def execute_sql_file(self, conn, sql_file: Path, description: str):
        """执行SQL文件"""
        if not sql_file.exists():
            logger.error(f"✗ SQL文件不存在: {sql_file}")
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        logger.info(f"\n{'=' * 80}")
        logger.info(f"执行: {description}")
        logger.info(f"文件: {sql_file.name}")
        logger.info(f"{'=' * 80}")
        
        start_time = time.time()
        
        try:
            # 读取SQL文件
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 执行SQL
            with conn.cursor() as cur:
                cur.execute(sql_content)
            
            conn.commit()
            
            elapsed = time.time() - start_time
            logger.info(f"✓ {description} 完成 (耗时: {elapsed:.2f}秒)")
        
        except Exception as e:
            logger.error(f"✗ {description} 失败: {e}")
            conn.rollback()
            raise
    
    def verify_installation(self, conn):
        """验证数据库安装"""
        logger.info(f"\n{'=' * 80}")
        logger.info("验证数据库安装...")
        logger.info(f"{'=' * 80}")
        
        try:
            with conn.cursor() as cur:
                # 检查主表
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_tables 
                        WHERE tablename = '{MAIN_TABLE}'
                    )
                """)
                main_exists = cur.fetchone()[0]
                
                if main_exists:
                    logger.info(f"✓ 主表 '{MAIN_TABLE}' 已创建")
                else:
                    logger.error(f"✗ 主表 '{MAIN_TABLE}' 不存在")
                    return False
                
                # 检查字段表
                for table in FIELD_TABLES:
                    cur.execute(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM pg_tables 
                            WHERE tablename = '{table}'
                        )
                    """)
                    exists = cur.fetchone()[0]
                    
                    if exists:
                        logger.info(f"✓ 字段表 '{table}' 已创建")
                    else:
                        logger.error(f"✗ 字段表 '{table}' 不存在")
                        return False
                
                # 检查分区数量
                cur.execute("""
                    SELECT COUNT(*) FROM pg_tables 
                    WHERE tablename LIKE '%_p%'
                    AND schemaname = 'public'
                """)
                partition_count = cur.fetchone()[0]
                logger.info(f"✓ 已创建 {partition_count} 个分区表")
                
                # 检查索引数量
                cur.execute("""
                    SELECT COUNT(*) FROM pg_indexes 
                    WHERE schemaname = 'public'
                """)
                index_count = cur.fetchone()[0]
                logger.info(f"✓ 已创建 {index_count} 个索引")
                
                # 检查触发器数量
                cur.execute("""
                    SELECT COUNT(DISTINCT tgname) FROM pg_trigger t
                    JOIN pg_class c ON t.tgrelid = c.oid
                    WHERE c.relnamespace = 'public'::regnamespace
                    AND NOT t.tgisinternal
                """)
                trigger_count = cur.fetchone()[0]
                logger.info(f"✓ 已创建 {trigger_count} 个触发器")
            
            logger.info(f"\n{'=' * 80}")
            logger.info("✓ 数据库验证通过！")
            logger.info(f"{'=' * 80}")
            return True
        
        except Exception as e:
            logger.error(f"✗ 验证失败: {e}")
            return False
    
    def initialize(self):
        """执行完整的数据库初始化"""
        total_start_time = time.time()
        
        try:
            # 1. 连接到PostgreSQL
            pg_conn = self.connect_postgres()
            
            try:
                # 2. 检查数据库是否存在
                db_exists = self.database_exists(pg_conn)
                
                if db_exists:
                    if self.drop_existing:
                        self.drop_database(pg_conn)
                        self.create_database(pg_conn)
                    else:
                        logger.warning(f"\n数据库 '{self.db_name}' 已存在！")
                        logger.info("使用 --drop 参数可以删除重建数据库")
                        response = input("\n是否继续？这将跳过数据库创建步骤 (y/N): ")
                        if response.lower() != 'y':
                            logger.info("用户取消操作")
                            return False
                else:
                    self.create_database(pg_conn)
            
            finally:
                pg_conn.close()
            
            # 3. 连接到目标数据库
            db_conn = self.connect_target_db()
            
            try:
                # 4. 执行SQL脚本
                
                # 4.1 创建表结构
                self.execute_sql_file(
                    db_conn,
                    SQL_SCRIPTS['create_tables'],
                    "创建表结构（12表 × 64分区 = 768分区表）"
                )
                
                # 4.2 创建索引
                self.execute_sql_file(
                    db_conn,
                    SQL_SCRIPTS['create_indexes'],
                    "创建索引（主键 + GIN + BRIN）"
                )
                
                # 4.3 创建触发器
                self.execute_sql_file(
                    db_conn,
                    SQL_SCRIPTS['create_triggers'],
                    "创建触发器（自动更新update_time）"
                )
                
                # 4.4 优化配置
                self.execute_sql_file(
                    db_conn,
                    SQL_SCRIPTS['optimize_config'],
                    "配置性能优化"
                )
                
                # 5. 验证安装
                if not self.verify_installation(db_conn):
                    logger.error("数据库验证失败")
                    return False
                
                # 6. 显示最终统计
                total_elapsed = time.time() - total_start_time
                
                logger.info(f"\n{'=' * 80}")
                logger.info("✓ 数据库初始化完成！")
                logger.info(f"{'=' * 80}")
                logger.info(f"总耗时: {total_elapsed:.2f}秒 ({total_elapsed/60:.1f}分钟)")
                logger.info(f"\n数据库结构:")
                logger.info(f"  - 主表: {MAIN_TABLE}")
                logger.info(f"  - 字段表: {len(FIELD_TABLES)} 个")
                logger.info(f"  - 分区表: 768 个 (12表 × 64分区)")
                logger.info(f"\n下一步:")
                logger.info(f"  1. 使用 bulk_insert_v2.py 批量插入数据")
                logger.info(f"  2. 使用 query_helpers_v2.py 查询数据")
                logger.info(f"  3. 查看 README_V2.md 了解详细用法")
                logger.info(f"{'=' * 80}\n")
                
                return True
            
            finally:
                db_conn.close()
        
        except KeyboardInterrupt:
            logger.warning("\n\n用户中断操作")
            return False
        
        except Exception as e:
            logger.error(f"\n\n初始化失败: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='初始化 S2ORC 语料库数据库 V2 - 分表方案',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 初始化新数据库
  python database/scripts_v2/init_database_v2.py
  
  # 删除并重建数据库（危险操作！）
  python database/scripts_v2/init_database_v2.py --drop

注意:
  - 确保 PostgreSQL 服务器正在运行
  - 确保用户有创建数据库的权限
  - --drop 参数会删除所有现有数据！
  - 适配硬件：32GB内存 + 16核CPU
        """
    )
    
    parser.add_argument(
        '--drop',
        action='store_true',
        help='删除已存在的数据库并重建（危险操作！）'
    )
    
    args = parser.parse_args()
    
    # 如果使用--drop，显示警告
    if args.drop:
        print("\n" + "!" * 80)
        print("警告: 您正在使用 --drop 参数！")
        print("这将删除数据库中的所有现有数据！")
        print("!" * 80)
        response = input("\n确定要继续吗? (yes/N): ")
        if response.lower() != 'yes':
            print("操作已取消")
            sys.exit(0)
    
    # 执行初始化
    initializer = DatabaseInitializerV2(drop_existing=args.drop)
    success = initializer.initialize()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

