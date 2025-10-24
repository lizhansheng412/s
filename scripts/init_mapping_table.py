#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化映射表 corpus_filename_mapping

用于存储 corpusid → filename 的轻量级索引
专门服务于大数据集：embeddings_specter_v1/v2, s2orc/s2orc_v2
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from database.config.db_config_v2 import DB_CONFIG


def create_mapping_table():
    """
    创建映射表 corpus_filename_mapping
    
    表结构：
      - corpusid (BIGINT, PRIMARY KEY): 论文ID
      - filename (TEXT): GZ文件名
      
    说明：
      - corpusid 是主键，唯一不可重复
      - filename 可以重复（一个GZ文件包含多条记录）
      - 不需要 insert_time/update_time（轻量级索引）
    """
    try:
        print("连接数据库...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 检查表是否已存在
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'corpus_filename_mapping'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print("⚠️  表 corpus_filename_mapping 已存在")
            response = input("是否删除并重建？(yes/no): ").strip().lower()
            if response == 'yes':
                print("删除旧表...")
                cursor.execute("DROP TABLE corpus_filename_mapping CASCADE;")
                conn.commit()
                print("✅ 旧表已删除")
            else:
                print("取消操作")
                cursor.close()
                conn.close()
                return
        
        # 创建映射表（性能优化：紧凑存储 + 自动清理）
        print("创建映射表 corpus_filename_mapping...")
        cursor.execute("""
            CREATE TABLE corpus_filename_mapping (
                corpusid BIGINT PRIMARY KEY,
                filename TEXT NOT NULL
            ) WITH (
                fillfactor = 100,
                autovacuum_enabled = true,
                autovacuum_vacuum_scale_factor = 0.01,
                autovacuum_analyze_scale_factor = 0.005
            );
        """)
        print("  ✅ 使用紧凑存储（fillfactor=100，无预留空间）")
        print("  ✅ 启用自动清理（及时回收空间）")
        print("  ✅ 无额外索引（只有主键索引，最大化插入速度）")
        
        # 添加注释
        cursor.execute("""
            COMMENT ON TABLE corpus_filename_mapping IS 
            '轻量级索引：corpusid → filename 映射表，用于快速定位大数据集中的记录';
        """)
        
        cursor.execute("""
            COMMENT ON COLUMN corpus_filename_mapping.corpusid IS 
            '论文ID（主键，唯一）';
        """)
        
        cursor.execute("""
            COMMENT ON COLUMN corpus_filename_mapping.filename IS 
            'GZ文件名（可重复，一个文件包含多条记录）';
        """)
        
        conn.commit()
        
        # 验证表结构
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'corpus_filename_mapping'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        
        print("\n✅ 映射表创建成功！")
        print("\n表结构：")
        print("="*60)
        for col_name, col_type, nullable in columns:
            null_str = "NULL" if nullable == 'YES' else "NOT NULL"
            pk_str = " (PRIMARY KEY)" if col_name == 'corpusid' else ""
            print(f"  {col_name:<15} {col_type:<20} {null_str}{pk_str}")
        print("="*60)
        
        # 显示索引（只有主键索引）
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'corpus_filename_mapping';
        """)
        
        indexes = cursor.fetchall()
        print("\n索引：")
        print("="*60)
        if indexes:
            for idx_name, idx_def in indexes:
                print(f"  {idx_name} (主键索引)")
        print("  ℹ️  无额外索引，最大化插入速度")
        print("="*60)
        
        print("\n💡 用途说明：")
        print("   - 不存储完整数据，只存储 corpusid → filename 的映射关系")
        print("   - 查询时根据 corpusid 找到对应的 gz 文件，再从文件中读取完整数据")
        print("   - 大幅减少数据库存储空间和插入时间")
        
        print("\n📊 预估容量：")
        print("   - 每条记录约 20-30 字节（corpusid + filename）")
        print("   - 1亿条记录约 2-3 GB（远小于完整数据）")
        
        print("\n⚡ 性能预期（无额外索引优化）：")
        print("   - 插入速度：30000-60000 条/秒（比有索引快1.5-2倍）")
        print("   - 1亿条记录预计插入时间：30-60 分钟")
        print("   - 存储空间：约2-3GB（无索引额外开销）")
        print("   - 查询速度：主键查询极快（O(log n)）")
        
        print("\n🚀 下一步：")
        print("   1. 使用 scripts/test/stream_gz_to_mapping_table.py 提取索引")
        print("   2. 或使用 scripts/test/batch_process_machine_mapping_test.py 批量处理")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ 数据库错误: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='初始化映射表 corpus_filename_mapping',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
说明：
  创建轻量级索引表 corpus_filename_mapping，用于存储：
    - corpusid: 论文ID（主键）
    - filename: GZ文件名
  
  适用场景：
    - 大数据集（embeddings_specter_v1/v2, s2orc/s2orc_v2）
    - 不存储完整数据，只建立索引
    - 查询时根据 corpusid 找到文件，再读取完整数据

示例：
  python scripts/init_mapping_table.py
        """
    )
    
    args = parser.parse_args()
    
    create_mapping_table()


if __name__ == '__main__':
    main()

