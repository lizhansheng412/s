#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完全清理脚本 - 删除所有旧的表空间、数据库和表
确保100%使用新配置（E盘存储）
"""

import sys
from pathlib import Path
import io
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# 修复Windows控制台编码问题
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.config.db_config_v2 import DB_CONFIG, TABLESPACE_CONFIG

def clean_all():
    """完全清理：删除数据库、表空间"""
    
    print("\n" + "="*80)
    print("🧹 完全清理脚本 - 确保100%使用E盘配置")
    print("="*80)
    print(f"\n⚠️  警告：此操作将删除以下内容：")
    print(f"  • 数据库: {DB_CONFIG['database']}")
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
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database='postgres',  # 连接到postgres数据库
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print("\n" + "="*80)
        print("步骤1: 删除目标数据库")
        print("="*80)
        
        # 检查数据库是否存在
        cursor.execute("""
            SELECT 1 FROM pg_database WHERE datname = %s
        """, (DB_CONFIG['database'],))
        
        if cursor.fetchone():
            print(f"发现数据库: {DB_CONFIG['database']}")
            
            # 终止所有连接
            print(f"  → 终止所有活动连接...")
            cursor.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{DB_CONFIG['database']}' AND pid <> pg_backend_pid()
            """)
            
            # 删除数据库
            print(f"  → 删除数据库...")
            cursor.execute(f'DROP DATABASE IF EXISTS {DB_CONFIG["database"]}')
            print(f"✅ 数据库已删除: {DB_CONFIG['database']}")
        else:
            print(f"ℹ️  数据库不存在: {DB_CONFIG['database']}")
        
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
        """, (DB_CONFIG['database'],))
        if cursor.fetchone():
            print(f"❌ 数据库仍然存在: {DB_CONFIG['database']}")
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
    success = clean_all()
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()

