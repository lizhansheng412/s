"""
初始化通用临时表
用于存储从gz文件解压后的数据
适用于所有数据集（s2orc, s2orc_v2, embeddings_specter_v1, embeddings_specter_v2, citations）
"""
import sys
from pathlib import Path

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import psycopg2
from db_config import get_db_config, MACHINE_DB_MAP


INDEX_DEFINITIONS = [
    (
        "idx_temp_import_corpusid",
        lambda table: f"CREATE INDEX IF NOT EXISTS idx_temp_import_corpusid ON {table} (corpusid);",
    ),
    (
        "idx_temp_import_is_done",
        lambda table: f"CREATE INDEX IF NOT EXISTS idx_temp_import_is_done ON {table} (is_done);",
    ),
]

# 通用临时表名
TEMP_TABLE = "temp_import"
# gz文件导入记录表
GZ_LOG_TABLE = "gz_import_log"
# 数据集类型枚举
DATASET_TYPES = ['s2orc', 's2orc_v2', 'embeddings_specter_v1', 'embeddings_specter_v2', 'citations']


def init_temp_table(machine_id='machine0'):
    """
    创建通用临时表
    
    表结构：
    - corpusid: BIGINT (无主键，无索引，提高插入速度)
    - specter_v1: TEXT, 存储 embeddings_specter_v1 数据集的字段值
    - specter_v2: TEXT, 存储 embeddings_specter_v2 数据集的字段值
    - content: TEXT, 存储 s2orc 和 s2orc_v2 数据集的字段值
    - citations: TEXT, 存储 citations 数据集的引用字段值
    - references: TEXT, 存储 citations 数据集的参考字段值
    - is_done: BOOLEAN, 标记是否已处理完成
    
    使用UNLOGGED表提高性能（不写WAL日志）
    """
    conn = None
    cursor = None
    
    try:
        db_config = get_db_config(machine_id)
        print(f"连接到数据库 [{machine_id}: {db_config['database']}:{db_config['port']}]...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 创建UNLOGGED表（更快，适合临时数据）
        print(f"创建{TEMP_TABLE}表...")
        create_table_sql = f"""
        CREATE UNLOGGED TABLE IF NOT EXISTS {TEMP_TABLE} (
            corpusid BIGINT NOT NULL,
            specter_v1 TEXT,
            specter_v2 TEXT,
            content TEXT,
            citations TEXT,
            "references" TEXT,
            is_done BOOLEAN DEFAULT FALSE
        );
        """
        cursor.execute(create_table_sql)
        
        # 不创建主键和索引，提高插入速度
        # 只在需要查询时才创建索引
        
        conn.commit()
        print(f"[OK] {TEMP_TABLE}表创建成功！")
        
        # 显示表结构
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = '{TEMP_TABLE}'
            ORDER BY ordinal_position;
        """)
        
        print("\n表结构：")
        print("-" * 80)
        for row in cursor.fetchall():
            print(f"  {row[0]:<20} {row[1]:<20} NULL={row[2]:<5} DEFAULT={row[3]}")
        print("-" * 80)
        print("\n注意：UNLOGGED表，无主键/索引，优化插入速度")
        print("如需在批量插入完成后建立索引，可再次运行脚本并使用 --create-indexes 参数")
        
    except Exception as e:
        print(f"[FAIL] 创建表失败: {e}")
        if conn:
            conn.rollback()
        raise
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def drop_temp_table(machine_id='machine0'):
    """删除通用临时表（慎用）"""
    conn = None
    cursor = None
    
    try:
        db_config = get_db_config(machine_id)
        print(f"连接到数据库 [{machine_id}: {db_config['database']}:{db_config['port']}]...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 删除表
        print(f"删除{TEMP_TABLE}表...")
        cursor.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE} CASCADE;")
        
        conn.commit()
        print(f"[OK] {TEMP_TABLE}表已删除！")
        
    except Exception as e:
        print(f"[FAIL] 删除表失败: {e}")
        if conn:
            conn.rollback()
        raise
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def truncate_temp_table(machine_id='machine0'):
    """清空通用临时表并删除索引"""
    conn = None
    cursor = None
    
    try:
        db_config = get_db_config(machine_id)
        print(f"连接到数据库 [{machine_id}: {db_config['database']}:{db_config['port']}]...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 删除索引
        print(f"删除{TEMP_TABLE}的索引...")
        for index_name, _ in INDEX_DEFINITIONS:
            cursor.execute(f"DROP INDEX IF EXISTS {index_name};")
            print(f"  - 已删除索引: {index_name}")
        
        # 清空表
        print(f"清空{TEMP_TABLE}表...")
        cursor.execute(f"TRUNCATE TABLE {TEMP_TABLE};")
        
        conn.commit()
        print(f"[OK] {TEMP_TABLE}表已清空！索引已删除，导入完成后请运行 --create-indexes 重建索引")
        
    except Exception as e:
        print(f"[FAIL] 清空表失败: {e}")
        if conn:
            conn.rollback()
        raise
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def create_indexes(machine_id='machine0'):
    """为临时表创建所需索引"""
    conn = None
    cursor = None

    try:
        db_config = get_db_config(machine_id)
        print(f"连接到数据库 [{machine_id}: {db_config['database']}:{db_config['port']}]...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        for index_name, sql_builder in INDEX_DEFINITIONS:
            sql = sql_builder(TEMP_TABLE)
            print(f"创建索引 {index_name}...")
            cursor.execute(sql)

        conn.commit()
        print("[OK] 所有索引创建完成！")

    except Exception as e:
        print(f"[FAIL] 创建索引失败: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def init_gz_log_table(machine_id='machine0'):
    """
    创建gz文件导入记录表
    
    表结构：
    - id: SERIAL PRIMARY KEY
    - filename: VARCHAR(255), gz文件名
    - data_type: VARCHAR(50), 数据集类型（s2orc等）
    - imported_at: TIMESTAMP, 导入时间
    - UNIQUE(filename, data_type): 确保同一文件+类型组合只记录一次
    """
    conn = None
    cursor = None
    
    try:
        db_config = get_db_config(machine_id)
        print(f"连接到数据库 [{machine_id}: {db_config['database']}:{db_config['port']}]...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 创建记录表
        print(f"创建{GZ_LOG_TABLE}表...")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {GZ_LOG_TABLE} (
            id SERIAL PRIMARY KEY,
            filename VARCHAR(255) NOT NULL,
            data_type VARCHAR(50) NOT NULL,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(filename, data_type)
        );
        """
        cursor.execute(create_table_sql)
        
        # 创建索引以加速查询
        cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_gz_log_data_type 
        ON {GZ_LOG_TABLE} (data_type);
        """)
        
        conn.commit()
        print(f"[OK] {GZ_LOG_TABLE}表创建成功！")
        
        # 显示表结构
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = '{GZ_LOG_TABLE}'
            ORDER BY ordinal_position;
        """)
        
        print("\n表结构：")
        print("-" * 80)
        for row in cursor.fetchall():
            print(f"  {row[0]:<20} {row[1]:<30} NULL={row[2]:<5} DEFAULT={row[3]}")
        print("-" * 80)
        print(f"\n支持的数据集类型: {', '.join(DATASET_TYPES)}")
        
    except Exception as e:
        print(f"[FAIL] 创建表失败: {e}")
        if conn:
            conn.rollback()
        raise
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def clear_gz_log_table(machine_id='machine0'):
    """清空gz文件导入记录表"""
    conn = None
    cursor = None
    
    try:
        db_config = get_db_config(machine_id)
        print(f"连接到数据库 [{machine_id}: {db_config['database']}:{db_config['port']}]...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print(f"清空{GZ_LOG_TABLE}表...")
        cursor.execute(f"TRUNCATE TABLE {GZ_LOG_TABLE} RESTART IDENTITY;")
        
        conn.commit()
        print(f"[OK] {GZ_LOG_TABLE}表已清空！")
        
    except Exception as e:
        print(f"[FAIL] 清空表失败: {e}")
        if conn:
            conn.rollback()
        raise
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="初始化通用临时表")
    parser.add_argument("--machine", default="machine0", choices=list(MACHINE_DB_MAP.keys()), 
                        help=f"目标机器 (默认: machine0)")
    parser.add_argument("--drop", action="store_true", help="删除表（慎用）")
    parser.add_argument("--truncate", action="store_true", help="清空表")
    parser.add_argument(
        "--create-indexes",
        action="store_true",
        help="为 temp_import.corpusid 和 is_done 创建索引",
    )
    parser.add_argument(
        "--init-log-table",
        action="store_true",
        help="创建gz文件导入记录表",
    )
    parser.add_argument(
        "--clear-log",
        action="store_true",
        help="清空gz文件导入记录表",
    )
    args = parser.parse_args()
    
    machine_id = args.machine
    
    if args.drop:
        response = input(f"确认要删除{TEMP_TABLE}表吗？(yes/no): ")
        if response.lower() == "yes":
            drop_temp_table(machine_id)
        else:
            print("已取消")
    elif args.truncate:
        truncate_temp_table(machine_id)
    elif args.create_indexes:
        create_indexes(machine_id)
    elif args.init_log_table:
        init_gz_log_table(machine_id)
    elif args.clear_log:
        clear_gz_log_table(machine_id)
    else:
        init_temp_table(machine_id)

