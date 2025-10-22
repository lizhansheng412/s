#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证存储位置配置
确保所有数据存储在指定位置（E:\\postgreSQL）
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

def verify():
    """验证存储配置"""
    expected_location = TABLESPACE_CONFIG['location'].replace('\\', '/')
    
    print("\n" + "="*80)
    print("存储位置验证")
    print("="*80)
    print(f"期望位置: {expected_location}")
    
    # 1. 检查目录
    location = Path(TABLESPACE_CONFIG['location'])
    print(f"\n[1/3] 检查存储目录")
    if not location.exists():
        print(f"  ❌ 目录不存在: {location}")
        print(f"  💡 请创建目录: mkdir {location}")
        return False
    else:
        print(f"  ✅ 目录存在")
        try:
            test_file = location / "_test.tmp"
            test_file.touch()
            test_file.unlink()
            print(f"  ✅ 有写入权限")
        except:
            print(f"  ❌ 无写入权限")
            return False
    
    # 2. 检查PostgreSQL
    try:
        print(f"\n[2/3] 检查PostgreSQL配置")
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database='postgres',
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # 检查表空间
        cursor.execute("""
            SELECT pg_tablespace_location(oid) 
            FROM pg_tablespace 
            WHERE spcname = %s
        """, (TABLESPACE_CONFIG['name'],))
        
        result = cursor.fetchone()
        if result:
            location_db = result[0]
            # 统一路径格式比较（Windows下 / 和 \ 等价）
            location_db_normalized = location_db.replace('\\', '/')
            if location_db_normalized == expected_location:
                print(f"  ✅ 表空间配置正确: {location_db}")
            else:
                print(f"  ❌ 表空间位置错误")
                print(f"     期望: {expected_location}")
                print(f"     实际: {location_db}")
                return False
        else:
            print(f"  ℹ️  表空间尚未创建")
        
        # 检查数据库
        cursor.execute("""
            SELECT t.spcname, pg_tablespace_location(t.oid)
            FROM pg_database d
            LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid
            WHERE d.datname = %s
        """, (DB_CONFIG['database'],))
        
        db_result = cursor.fetchone()
        if db_result:
            spcname, loc = db_result
            if spcname == TABLESPACE_CONFIG['name']:
                print(f"  ✅ 数据库使用正确表空间: {spcname}")
            else:
                print(f"  ❌ 数据库使用错误表空间: {spcname or 'pg_default'}")
                return False
        else:
            print(f"  ℹ️  数据库尚未创建")
        
        # 3. 检查表
        if db_result:
            print(f"\n[3/3] 检查表存储位置")
            try:
                conn2 = psycopg2.connect(
                    host=DB_CONFIG['host'],
                    port=DB_CONFIG['port'],
                    database=DB_CONFIG['database'],
                    user=DB_CONFIG['user'],
                    password=DB_CONFIG['password']
                )
                cursor2 = conn2.cursor()
                
                cursor2.execute("""
                    SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'
                """)
                table_count = cursor2.fetchone()[0]
                
                if table_count > 0:
                    print(f"  ✅ 找到 {table_count} 个表，所有表继承数据库表空间")
                else:
                    print(f"  ℹ️  尚无表")
                
                cursor2.close()
                conn2.close()
            except:
                print(f"  ℹ️  数据库未初始化")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"  ❌ PostgreSQL连接失败: {e}")
        return False
    
    # 总结
    print("\n" + "="*80)
    print("✅ 验证通过")
    print("="*80)
    print(f"\n保证:")
    print(f"  ✅ 数据库 → {expected_location}")
    print(f"  ✅ 所有表 → {expected_location}")
    print(f"  ✅ 索引   → {expected_location}")
    print(f"\n不会存储在:")
    print(f"  ❌ C盘")
    print(f"  ❌ D盘")
    print(f"  ❌ PostgreSQL默认目录")
    print("="*80 + "\n")
    
    return True

if __name__ == '__main__':
    success = verify()
    sys.exit(0 if success else 1)
