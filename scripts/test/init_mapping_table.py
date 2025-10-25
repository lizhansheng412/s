#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化大数据集表 corpus_bigdataset
存储4个大数据集的唯一 corpusid（无主键，极速导入）
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import psycopg2
from database.config import get_db_config


def create_mapping_table():
    """创建大数据集表（无主键，极速导入模式）"""
    try:
        # 始终连接本机数据库（machine1 的 5431 端口）
        db_config = get_db_config('machine1')
        print(f"📡 连接到数据库: {db_config['database']}@{db_config['host']}:{db_config['port']}")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'corpus_bigdataset'
            );
        """)
        
        if cursor.fetchone()[0]:
            print("⚠️  表已存在")
            response = input("删除并重建？(yes/no): ").strip().lower()
            if response != 'yes':
                print("取消操作")
                return
            cursor.execute("DROP TABLE corpus_bigdataset CASCADE;")
            conn.commit()
            print("✅ 旧表已删除")
        
        # 创建表（无主键，极速导入）
        print("创建大数据集表（无主键模式）...")
        cursor.execute("""
            CREATE TABLE corpus_bigdataset (
                corpusid BIGINT NOT NULL
            ) WITH (
                fillfactor = 100,
                autovacuum_enabled = false
            );
        """)
        
        conn.commit()
        print("✅ 表创建成功（无主键，极速导入模式）")
        print("💡 导入完成后运行: python scripts/test/init_mapping_table.py --add-pk")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ 数据库错误: {e}")
        sys.exit(1)


def add_primary_key():
    """导入完成后添加主键（快速去重+建索引）"""
    import time
    try:
        # 始终连接本机数据库（machine1 的 5431 端口）
        db_config = get_db_config('machine1')
        print(f"📡 连接到数据库: {db_config['database']}@{db_config['host']}:{db_config['port']}")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 统计原始数据
        print("📊 统计原始数据...")
        cursor.execute("SELECT COUNT(*) FROM corpus_bigdataset;")
        original_count = cursor.fetchone()[0]
        print(f"   原始记录: {original_count:,} 条")
        
        # 快速去重：创建新表 + SELECT DISTINCT（比 DELETE 快很多）
        print("\n🚀 快速去重中（使用 SELECT DISTINCT）...")
        start_time = time.time()
        
        cursor.execute("""
            CREATE TABLE corpus_bigdataset_temp AS
            SELECT DISTINCT corpusid 
            FROM corpus_bigdataset;
        """)
        
        dedup_time = time.time() - start_time
        print(f"✅ 去重完成（耗时: {dedup_time:.1f}秒）")
        
        # 统计去重后数据
        cursor.execute("SELECT COUNT(*) FROM corpus_bigdataset_temp;")
        unique_count = cursor.fetchone()[0]
        deleted = original_count - unique_count
        print(f"   去重后记录: {unique_count:,} 条")
        print(f"   删除重复: {deleted:,} 条 ({deleted/original_count*100:.1f}%)")
        
        # 替换旧表
        print("\n🔄 替换旧表...")
        cursor.execute("DROP TABLE corpus_bigdataset;")
        cursor.execute("ALTER TABLE corpus_bigdataset_temp RENAME TO corpus_bigdataset;")
        print("✅ 表替换完成")
        
        # 添加主键（一次性建索引）
        print("\n🔑 添加主键...")
        start_time = time.time()
        cursor.execute("ALTER TABLE corpus_bigdataset ADD PRIMARY KEY (corpusid);")
        index_time = time.time() - start_time
        print(f"✅ 主键添加完成（耗时: {index_time:.1f}秒）")
        
        # 启用自动清理
        cursor.execute("ALTER TABLE corpus_bigdataset SET (autovacuum_enabled = true);")
        
        # 分析表（更新统计信息）
        print("\n📈 更新统计信息...")
        cursor.execute("ANALYZE corpus_bigdataset;")
        
        conn.commit()
        
        print(f"\n" + "="*60)
        print(f"✅ 所有操作完成！")
        print(f"="*60)
        print(f"📊 最终统计:")
        print(f"   - 唯一 corpusid: {unique_count:,} 条")
        print(f"   - 去重率: {deleted/original_count*100:.1f}%")
        print(f"   - 总耗时: {dedup_time + index_time:.1f}秒")
        print(f"="*60)
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ 错误: {e}")
        sys.exit(1)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='初始化大数据集表（操作本机 Machine1 数据库）')
    parser.add_argument('--add-pk', action='store_true', 
                       help='导入完成后添加主键（一次性建索引+快速去重）')
    
    args = parser.parse_args()
    
    if args.add_pk:
        add_primary_key()
    else:
        create_mapping_table()


if __name__ == '__main__':
    main()
