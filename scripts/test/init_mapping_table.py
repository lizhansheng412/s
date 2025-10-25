#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化映射表 corpus_filename_mapping
只存储唯一的 corpusid（无主键，极速导入）
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import psycopg2
from database.config.db_config_v2 import DB_CONFIG


def create_mapping_table():
    """创建映射表（无主键，极速导入模式）"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'corpus_filename_mapping'
            );
        """)
        
        if cursor.fetchone()[0]:
            print("⚠️  表已存在")
            response = input("删除并重建？(yes/no): ").strip().lower()
            if response != 'yes':
                print("取消操作")
                return
            cursor.execute("DROP TABLE corpus_filename_mapping CASCADE;")
            conn.commit()
            print("✅ 旧表已删除")
        
        # 创建表（无主键，极速导入）
        print("创建映射表（无主键模式）...")
        cursor.execute("""
            CREATE TABLE corpus_filename_mapping (
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
    """导入完成后添加主键（一次性建索引）"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("删除重复数据...")
        cursor.execute("""
            DELETE FROM corpus_filename_mapping a
            USING corpus_filename_mapping b
            WHERE a.ctid < b.ctid AND a.corpusid = b.corpusid;
        """)
        deleted = cursor.rowcount
        print(f"✅ 删除 {deleted:,} 条重复记录")
        
        print("添加主键（一次性建索引）...")
        cursor.execute("""
            ALTER TABLE corpus_filename_mapping 
            ADD PRIMARY KEY (corpusid);
        """)
        
        print("启用自动清理...")
        cursor.execute("""
            ALTER TABLE corpus_filename_mapping 
            SET (autovacuum_enabled = true);
        """)
        
        conn.commit()
        
        # 统计
        cursor.execute("SELECT COUNT(*) FROM corpus_filename_mapping;")
        count = cursor.fetchone()[0]
        
        print(f"\n✅ 主键添加完成！")
        print(f"📊 总记录数: {count:,}")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ 错误: {e}")
        sys.exit(1)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='初始化映射表')
    parser.add_argument('--add-pk', action='store_true', 
                       help='导入完成后添加主键（一次性建索引）')
    
    args = parser.parse_args()
    
    if args.add_pk:
        add_primary_key()
    else:
        create_mapping_table()


if __name__ == '__main__':
    main()
