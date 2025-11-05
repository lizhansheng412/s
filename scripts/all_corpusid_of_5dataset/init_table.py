#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化 final_delivery 表
阶段1：创建无约束表（极速导入） - 只有 corpusid 字段
阶段2：去重、建主键（--finalize）
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import psycopg2
import db_config

TABLE_NAME = 'final_delivery'


def create_table():
    """创建表（无主键，极速导入模式）- 只有 corpusid 字段"""
    try:
        config = db_config.DB_CONFIG
        print(f"📡 连接到: {config['database']}@{config['host']}:{config['port']}")
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{TABLE_NAME}'
            );
        """)
        
        if cursor.fetchone()[0]:
            print("⚠️  表已存在")
            response = input("删除并重建？(yes/no): ").strip().lower()
            if response != 'yes':
                print("取消操作")
                cursor.close()
                conn.close()
                return
            cursor.execute(f"DROP TABLE {TABLE_NAME} CASCADE;")
            conn.commit()
            print("✅ 旧表已删除")
        
        print("创建表（无主键、无索引模式）...")
        cursor.execute(f"""
            CREATE TABLE {TABLE_NAME} (
                corpusid BIGINT NOT NULL
            ) WITH (
                fillfactor = 100,
                autovacuum_enabled = false
            );
        """)
        
        conn.commit()
        print("✅ 表创建成功（极速导入模式）")
        print("💡 导入后运行: python scripts/all_corpusid_of_5dataset/init_table.py --finalize")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ 数据库错误: {e}")
        sys.exit(1)


def finalize_table():
    """导入完成后：极速去重、建主键（优化版）"""
    import time
    conn = None
    cursor = None
    try:
        config = db_config.DB_CONFIG
        print(f"📡 连接到: {config['database']}@{config['host']}:{config['port']}")
        conn = psycopg2.connect(**config)
        conn.autocommit = False  # 使用事务模式
        cursor = conn.cursor()
        
        print("\n⚡ 极速去重模式（跳过统计，直接处理）")
        print("="*50)
        
        # 针对8核32GB的激进优化
        print("🔧 优化数据库参数（8核32GB配置）...")
        try:
            cursor.execute("SET maintenance_work_mem = '8GB'")      # 去重和建索引用（25%内存）
            cursor.execute("SET work_mem = '4GB'")                  # 查询排序用（12.5%内存）
            cursor.execute("SET temp_buffers = '4GB'")              # 临时缓冲区
            cursor.execute("SET max_parallel_workers_per_gather = 6")  # 并行查询（留2核给系统）
            cursor.execute("SET max_parallel_maintenance_workers = 6") # 并行维护
            cursor.execute("SET effective_cache_size = '24GB'")     # 可用缓存（75%内存）
            print("   ✓ 内存和并行参数已优化（8核6并行）")
        except Exception as e:
            print(f"   ⚠️ 部分参数设置失败（可忽略）: {e}")
            # 参数设置失败不影响主流程，继续执行
        
        print("\n🚀 执行去重并建立主键（一步到位）...")
        start_time = time.time()
        
        # 极速方案：直接创建带主键的去重表
        cursor.execute(f"""
            CREATE TABLE {TABLE_NAME}_new (
                corpusid BIGINT PRIMARY KEY
            ) WITH (fillfactor = 100);
        """)
        
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME}_new (corpusid)
            SELECT DISTINCT corpusid 
            FROM {TABLE_NAME}
            ON CONFLICT (corpusid) DO NOTHING;
        """)
        
        # 替换表
        cursor.execute(f"DROP TABLE {TABLE_NAME};")
        cursor.execute(f"ALTER TABLE {TABLE_NAME}_new RENAME TO {TABLE_NAME};")
        
        elapsed = time.time() - start_time
        print(f"✅ 去重和建主键完成！耗时: {elapsed:.1f} 秒")
        
        # 更新统计信息
        print("\n📊 更新统计信息...")
        cursor.execute(f"ANALYZE {TABLE_NAME};")
        print("   ✓ 统计信息已更新")
        
        # 获取精确统计
        print("\n📊 统计最终结果...")
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
        final_count = cursor.fetchone()[0]
        print(f"   最终记录数: {final_count:,} 条")
        
        # 提交所有更改
        conn.commit()
        print("   ✓ 事务已提交")
        
        print("\n" + "="*50)
        print(f"✅ 去重完成！")
        print(f"   最终记录数: {final_count:,}")
        print(f"   总耗时: {elapsed:.1f} 秒")
        print(f"="*50)
        
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        
    except psycopg2.Error as e:
        print(f"\n❌ 数据库错误: {e}")
        if conn:
            try:
                conn.rollback()
                print("   ⚠️ 事务已回滚")
            except:
                pass
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 未预期错误: {e}")
        if conn:
            try:
                conn.rollback()
                print("   ⚠️ 事务已回滚")
            except:
                pass
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # 确保资源释放
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='初始化 final_delivery 表')
    parser.add_argument('--finalize', action='store_true', 
                       help='导入完成后：去重、建主键')
    
    args = parser.parse_args()
    
    if args.finalize:
        finalize_table()
    else:
        create_table()


if __name__ == '__main__':
    main()
