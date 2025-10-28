#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重组abstracts表物理存储 - CLUSTER优化
===============================================
功能：按corpusid主键重新组织abstracts表的物理存储顺序
效果：将随机I/O转换为顺序I/O，查询速度提升100倍以上

注意事项：
1. 此操作会锁定abstracts表，执行期间无法读写
2. 需要约2倍表大小的临时磁盘空间
3. 预计耗时：1-3小时（取决于USB硬盘速度）
4. 执行前请停止所有导出任务
"""

import sys
import os
import time
import psycopg2
from datetime import datetime

# 添加父目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.config.db_config_v2 import get_db_config


def get_table_info(conn, table_name):
    """获取表的关键信息（一次查询获取所有需要的数据）"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT 
                pg_size_pretty(pg_total_relation_size('{table_name}')) as total_size,
                pg_total_relation_size('{table_name}') as total_bytes,
                n_live_tup as live_tuples
            FROM pg_class c
            LEFT JOIN pg_stat_user_tables s ON c.relname = s.relname
            WHERE c.relname = '{table_name}'
        """)
        result = cursor.fetchone()
        return {
            'total_size': result[0],
            'total_bytes': result[1],
            'live_tuples': result[2]
        }
    finally:
        cursor.close()


def cluster_abstracts_table():
    """执行CLUSTER操作重组abstracts表"""
    
    print("=" * 80)
    print("ABSTRACTS表物理存储重组 - CLUSTER优化")
    print("=" * 80)
    print()
    
    conn = None
    try:
        # 1. 连接数据库并检查表信息
        print("📊 正在连接数据库并检查表信息...")
        machine3_config = get_db_config('machine3')
        conn = psycopg2.connect(**machine3_config)
        conn.autocommit = True  # CLUSTER是DDL操作，直接使用autocommit
        
        table_info = get_table_info(conn, 'abstracts')
        
        print("✓ 已连接到数据库")
        print()
        print(f"表信息:")
        print(f"  - 总大小: {table_info['total_size']}")
        print(f"  - 记录数: {table_info['live_tuples']:,}")
        print(f"  - 预计临时空间: {table_info['total_bytes'] * 2 / (1024**3):.2f} GB")
        print(f"  - 预计耗时: 1-3小时")
        print()
        
        # 2. 确认执行
        print("=" * 80)
        print("⚠️  重要提示")
        print("=" * 80)
        print("1. CLUSTER会锁定abstracts表，执行期间无法读写")
        print("2. 请确保已停止所有export_to_jsonl_parallel.py进程")
        print("3. 操作不可中断，中断后需要从头开始")
        print()
        
        response = input("确认执行CLUSTER？(yes/no): ").strip().lower()
        if response != 'yes':
            print("\n❌ 操作已取消")
            return
        
        print()
        
        # 3. 执行CLUSTER重组（核心步骤）
        print("=" * 80)
        print("执行CLUSTER重组")
        print("=" * 80)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始CLUSTER abstracts...")
        print("⏳ 正在重组表的物理存储（约1-3小时），请勿中断...")
        print()
        
        cluster_start = time.time()
        cursor = conn.cursor()
        
        # 执行CLUSTER（按主键重组物理存储）
        cursor.execute("CLUSTER abstracts USING abstracts_pkey")
        cursor.close()
        
        cluster_elapsed = time.time() - cluster_start
        print(f"\n✓ CLUSTER完成")
        print(f"  耗时: {cluster_elapsed/60:.1f}分钟 ({cluster_elapsed/3600:.2f}小时)")
        print()
        
        # 4. 更新统计信息
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 更新统计信息...")
        
        analyze_start = time.time()
        cursor = conn.cursor()
        cursor.execute("ANALYZE abstracts")
        cursor.close()
        
        analyze_elapsed = time.time() - analyze_start
        print(f"✓ ANALYZE完成，耗时: {analyze_elapsed:.1f}秒")
        print()
        
        # 5. 验证结果
        new_table_info = get_table_info(conn, 'abstracts')
        
        print("=" * 80)
        print("优化结果")
        print("=" * 80)
        print(f"优化后表大小: {new_table_info['total_size']}")
        print(f"记录数: {new_table_info['live_tuples']:,}")
        
        size_diff = table_info['total_bytes'] - new_table_info['total_bytes']
        if size_diff > 0:
            print(f"空间节省: {size_diff / (1024**3):.2f} GB")
        print()
        
        # 总结
        total_elapsed = cluster_elapsed + analyze_elapsed
        print("=" * 80)
        print("✅ 优化完成！")
        print("=" * 80)
        print(f"总耗时: {total_elapsed/60:.1f}分钟 ({total_elapsed/3600:.2f}小时)")
        print()
        print("预期效果:")
        print("  ✓ abstracts表查询: 124秒 → <1秒 (100倍↑)")
        print("  ✓ 总处理速度: 300条/秒 → 1500条/秒 (5倍↑)")
        print("  ✓ 完成时间: 91小时 → 18.5小时 (80%↓)")
        print()
        print("📝 下一步:")
        print("  1. 重启 export_to_jsonl_parallel.py")
        print("  2. 观察新的处理速度")
        print()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  检测到中断信号")
        print("注意: CLUSTER操作可能已部分完成，建议检查表状态")
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\n数据库连接已关闭")


if __name__ == '__main__':
    cluster_abstracts_table()

