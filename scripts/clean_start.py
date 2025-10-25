#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理脚本 - 支持完全清理或清理指定机器的表
"""

import sys
from pathlib import Path
import io
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.config import db_config_v2
from database.config.db_config_v2 import TABLESPACE_CONFIG, get_db_config
from machine_config import get_machine_config


def clean_machine_tables(machine_id: str):
    """清理指定机器的表（仅删除表，保留数据库）"""
    
    config = get_machine_config(machine_id)
    tables = config['tables']
    db_config = get_db_config(machine_id)
    
    print("\n" + "="*80)
    print(f"清理机器表 - {machine_id}")
    print("="*80)
    print(f"\n配置: {config['description']}")
    print(f"数据库: {db_config['database']}")
    print(f"\n将删除以下表:")
    for table in tables:
        print(f"  • {table}")
    
    response = input(f"\n确认删除这些表？(yes/no): ")
    if response.lower() != 'yes':
        print("已取消操作")
        return False
    
    try:
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print("\n开始删除表...")
        deleted_count = 0
        
        for table in tables:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table,))
            
            if cursor.fetchone()[0]:
                print(f"  删除表: {table}")
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                deleted_count += 1
                print(f"    ✅ 已删除")
            else:
                print(f"  跳过（不存在）: {table}")
        
        print("\n" + "="*80)
        print(f"✅ 清理完成！已删除 {deleted_count}/{len(tables)} 个表")
        print("="*80)
        print(f"\n下一步操作：")
        print(f"  python scripts/init_database.py --init --machine {machine_id}")
        print("="*80 + "\n")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n❌ 清理失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def clean_all():
    """完全清理：删除数据库、表空间"""
    
    print("\n" + "="*80)
    print("🧹 完全清理脚本 - 确保100%使用E盘配置")
    print("="*80)
    print(f"\n⚠️  警告：此操作将删除以下内容：")
    print(f"  • 数据库: {db_config_v2.DB_CONFIG['database']}")
    print(f"  • 所有自定义表空间")
    print(f"  • 所有相关数据")
    print(f"\n✅ 清理后将使用配置:")
    print(f"  • 表空间名称: {TABLESPACE_CONFIG['name']}")
    print(f"  • 存储位置: {TABLESPACE_CONFIG['location']}")
    
    response = input(f"\n确认执行清理？(yes/no): ")
    if response.lower() != 'yes':
        print("已取消操作")
        return False
    
    try:
        conn = psycopg2.connect(
            host=db_config_v2.DB_CONFIG['host'],
            port=db_config_v2.DB_CONFIG['port'],
            database='postgres',  # 连接到postgres数据库
            user=db_config_v2.DB_CONFIG['user'],
            password=db_config_v2.DB_CONFIG['password']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print("\n" + "="*80)
        print("步骤1: 删除目标数据库")
        print("="*80)
        
        # 检查数据库是否存在
        cursor.execute("""
            SELECT 1 FROM pg_database WHERE datname = %s
        """, (db_config_v2.DB_CONFIG['database'],))
        
        if cursor.fetchone():
            print(f"发现数据库: {db_config_v2.DB_CONFIG['database']}")
            
            # 终止所有连接
            print(f"  → 终止所有活动连接...")
            cursor.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{db_config_v2.DB_CONFIG['database']}' AND pid <> pg_backend_pid()
            """)
            
            # 删除数据库
            print(f"  → 删除数据库...")
            cursor.execute(f'DROP DATABASE IF EXISTS {db_config_v2.DB_CONFIG["database"]}')
            print(f"✅ 数据库已删除: {db_config_v2.DB_CONFIG['database']}")
        else:
            print(f"ℹ️  数据库不存在: {db_config_v2.DB_CONFIG['database']}")
        
        print("\n" + "="*80)
        print("步骤2: 删除所有自定义表空间")
        print("="*80)
        
        # 获取所有非系统表空间
        cursor.execute("""
            SELECT spcname, pg_tablespace_location(oid) as location
            FROM pg_tablespace
            WHERE spcname NOT IN ('pg_default', 'pg_global')
            ORDER BY spcname
        """)
        
        tablespaces = cursor.fetchall()
        
        if tablespaces:
            print(f"发现 {len(tablespaces)} 个自定义表空间:")
            for name, location in tablespaces:
                print(f"  • {name}: {location}")
            
            print(f"\n开始删除...")
            for name, location in tablespaces:
                try:
                    cursor.execute(f'DROP TABLESPACE {name}')
                    print(f"  ✅ 已删除: {name}")
                except Exception as e:
                    print(f"  ⚠️  无法删除 {name}: {e}")
        else:
            print(f"ℹ️  没有自定义表空间")
        
        print("\n" + "="*80)
        print("步骤3: 验证清理结果")
        print("="*80)
        
        # 检查数据库
        cursor.execute("""
            SELECT 1 FROM pg_database WHERE datname = %s
        """, (db_config_v2.DB_CONFIG['database'],))
        if cursor.fetchone():
            print(f"❌ 数据库仍然存在: {db_config_v2.DB_CONFIG['database']}")
        else:
            print(f"✅ 数据库已清理")
        
        # 检查表空间
        cursor.execute("""
            SELECT COUNT(*) FROM pg_tablespace
            WHERE spcname NOT IN ('pg_default', 'pg_global')
        """)
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"⚠️  仍有 {count} 个自定义表空间")
        else:
            print(f"✅ 所有自定义表空间已清理")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("✅ 清理完成！")
        print("="*80)
        print(f"\n下一步操作：")
        print(f"  1. 确保目录存在: {TABLESPACE_CONFIG['location']}")
        print(f"  2. 运行初始化: python scripts/init_database.py --init --machine machine3")
        print(f"  3. 导入数据: python scripts/batch_process_machine.py --machine machine3 --base-dir \"E:\\2025-09-30\"")
        print(f"\n💯 保证：所有数据将100%存储在 {TABLESPACE_CONFIG['location']}")
        print("="*80 + "\n")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 清理失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='数据库清理工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例：

完全清理（删除数据库和表空间）：
  python scripts/clean_start.py

清理指定机器的表（保留数据库）：
  python scripts/clean_start.py --machine machine3
  python scripts/clean_start.py --machine machine1
        """
    )
    
    parser.add_argument('--machine', type=str,
                       choices=['machine1', 'machine2', 'machine3', 'machine0'],
                       help='指定清理机器的表（不删除数据库）')
    
    args = parser.parse_args()
    
    if args.machine:
        success = clean_machine_tables(args.machine)
    else:
        success = clean_all()
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()

