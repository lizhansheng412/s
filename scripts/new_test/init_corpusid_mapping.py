#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化 corpus_new_bigdataset 表
阶段1：创建无约束表（极速导入）
阶段2：去重、建主键、优化（--finalize）
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import psycopg2
from database.config import get_db_config


TABLE_NAME = 'corpus_new_bigdataset'


def create_table():
    """创建表（无主键，极速导入模式）"""
    try:
        db_config = get_db_config('machine1')
        print(f"📡 连接到: {db_config['database']}@{db_config['host']}:{db_config['port']}")
        conn = psycopg2.connect(**db_config)
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
                return
            cursor.execute(f"DROP TABLE {TABLE_NAME} CASCADE;")
            conn.commit()
            print("✅ 旧表已删除")
        
        print("创建表（无主键、无索引模式）...")
        cursor.execute(f"""
            CREATE TABLE {TABLE_NAME} (
                corpusid BIGINT NOT NULL,
                embeddings_specter_v1 TEXT,
                embeddings_specter_v2 TEXT,
                s2orc TEXT,
                s2orc_v2 TEXT,
                citation TEXT
            ) WITH (
                fillfactor = 100,
                autovacuum_enabled = false
            );
        """)
        
        conn.commit()
        print("✅ 表创建成功（极速导入模式）")
        print("💡 导入后运行: python scripts/new_test/init_corpusid_mapping.py --finalize")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ 数据库错误: {e}")
        sys.exit(1)


def finalize_table():
    """导入完成后：去重、建主键、优化"""
    import time
    try:
        db_config = get_db_config('machine1')
        print(f"📡 连接到: {db_config['database']}@{db_config['host']}:{db_config['port']}")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("📊 统计原始数据...")
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
        original_count = cursor.fetchone()[0]
        print(f"   原始记录: {original_count:,} 条")
        
        print("\n🚀 去重并合并字段...")
        start_time = time.time()
        
        # 使用窗口函数合并同一个corpusid的多行数据
        cursor.execute(f"""
            CREATE TABLE {TABLE_NAME}_temp AS
            SELECT 
                corpusid,
                MAX(embeddings_specter_v1) AS embeddings_specter_v1,
                MAX(embeddings_specter_v2) AS embeddings_specter_v2,
                MAX(s2orc) AS s2orc,
                MAX(s2orc_v2) AS s2orc_v2,
                MAX(citation) AS citation
            FROM {TABLE_NAME}
            GROUP BY corpusid;
        """)
        
        dedup_time = time.time() - start_time
        print(f"✅ 去重完成 ({dedup_time:.1f}秒)")
        
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}_temp;")
        unique_count = cursor.fetchone()[0]
        deleted = original_count - unique_count
        print(f"   去重后: {unique_count:,} 条")
        if deleted > 0:
            print(f"   合并重复: {deleted:,} 条 ({deleted/original_count*100:.1f}%)")
        
        print("\n🔄 替换旧表...")
        cursor.execute(f"DROP TABLE {TABLE_NAME};")
        cursor.execute(f"ALTER TABLE {TABLE_NAME}_temp RENAME TO {TABLE_NAME};")
        print("✅ 替换完成")
        
        print("\n🔑 添加主键...")
        start_time = time.time()
        cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (corpusid);")
        index_time = time.time() - start_time
        print(f"✅ 主键添加完成 ({index_time:.1f}秒)")
        
        cursor.execute(f"ALTER TABLE {TABLE_NAME} SET (autovacuum_enabled = true);")
        
        print("\n📈 更新统计信息...")
        cursor.execute(f"ANALYZE {TABLE_NAME};")
        
        conn.commit()
        
        print(f"\n" + "="*50)
        print(f"✅ 最终化完成！")
        print(f"唯一 corpusid: {unique_count:,}")
        print(f"总耗时: {dedup_time + index_time:.1f}秒")
        print(f"="*50)
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ 错误: {e}")
        sys.exit(1)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='初始化 corpus_new_bigdataset 表')
    parser.add_argument('--finalize', action='store_true', 
                       help='导入完成后：去重、建主键、优化')
    
    args = parser.parse_args()
    
    if args.finalize:
        finalize_table()
    else:
        create_table()


if __name__ == '__main__':
    main()

