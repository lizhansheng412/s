#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step One - 构建完整的 corpusid 表
功能：
1. 解压 paper-ids 数据集的所有 gz 文件
2. 提取每行的 corpusid 字段
3. 使用 COPY 批量插入到 PostgreSQL 数据库
4. 完成后对 corpusid 排序并建立主键索引
"""

import sys
import gzip
import time
import tempfile
from pathlib import Path
from io import StringIO
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import orjson
import psycopg2
from psycopg2 import sql
from tqdm import tqdm

from step_scripts.step_one.machine_db_config import get_db_config
from step_scripts.step_one.init_process_table import ProcessRecorder, DatasetType

# =============================================================================
# 配置
# =============================================================================

TABLE_NAME = 'full_corpusid'
DATA_FOLDER = Path(r'D:\2025-09-30\paper-ids')
BATCH_SIZE = 500000  # 每批次处理的行数（针对2亿+数据优化）

# =============================================================================
# 数据库操作
# =============================================================================

def create_table_if_not_exists(cursor):
    """创建表（如果不存在）- 无索引模式用于快速插入"""
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{TABLE_NAME}'
        );
    """)
    
    if cursor.fetchone()[0]:
        print(f"⚠️  表 {TABLE_NAME} 已存在")
        response = input("是否删除并重建？(yes/no): ").strip().lower()
        if response != 'yes':
            return False
        cursor.execute(f"DROP TABLE {TABLE_NAME} CASCADE;")
    
    cursor.execute(f"""
        CREATE TABLE {TABLE_NAME} (
            corpusid BIGINT NOT NULL
        ) WITH (
            fillfactor = 100,
            autovacuum_enabled = false
        );
    """)
    return True

def process_gz_file(gz_path, cursor, conn):
    """
    处理单个 gz 文件，提取 corpusid 并批量插入
    
    Args:
        gz_path: gz 文件路径
        cursor: 数据库游标
        conn: 数据库连接
    
    Returns:
        插入的记录数
    """
    total_inserted = 0
    batch_buffer = []
    
    try:
        with gzip.open(gz_path, 'rb') as f:
            for line in f:
                if not line.strip():
                    continue
                
                try:
                    data = orjson.loads(line)
                    corpusid = data.get('corpusid')
                    
                    if corpusid is not None:
                        batch_buffer.append(str(corpusid))
                        
                        # 达到批次大小时执行插入
                        if len(batch_buffer) >= BATCH_SIZE:
                            insert_batch(cursor, batch_buffer)
                            total_inserted += len(batch_buffer)
                            batch_buffer = []
                            conn.commit()
                
                except Exception as e:
                    print(f"⚠️  解析行失败: {e}")
                    continue
        
        # 插入剩余数据
        if batch_buffer:
            insert_batch(cursor, batch_buffer)
            total_inserted += len(batch_buffer)
            conn.commit()
    
    except Exception as e:
        print(f"❌ 处理文件失败 {gz_path.name}: {e}")
        conn.rollback()
        raise
    
    return total_inserted

def insert_batch(cursor, corpusid_list):
    """使用 COPY 批量插入数据"""
    buffer = StringIO()
    for corpusid in corpusid_list:
        buffer.write(f"{corpusid}\n")
    buffer.seek(0)
    
    cursor.copy_from(buffer, TABLE_NAME, columns=('corpusid',))

def build_index_and_sort(cursor, conn):
    """排序并建立主键索引"""
    print("\n" + "="*70)
    print("📊 数据优化中...")
    
    # 统计原始记录数
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
    total_count = cursor.fetchone()[0]
    print(f"原始记录数: {total_count:,}")
    
    # 创建去重排序表并建立主键
    print("去重、排序、建立主键...")
    start_time = time.time()
    temp_table = f"{TABLE_NAME}_new"
    
    cursor.execute(f"""
        CREATE TABLE {temp_table} (
            corpusid BIGINT PRIMARY KEY
        ) WITH (fillfactor = 100);
    """)
    
    cursor.execute(f"""
        INSERT INTO {temp_table} (corpusid)
        SELECT DISTINCT corpusid 
        FROM {TABLE_NAME}
        ON CONFLICT (corpusid) DO NOTHING;
    """)
    
    cursor.execute(f"DROP TABLE {TABLE_NAME};")
    cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {TABLE_NAME};")
    
    cursor.execute(f"""
        ALTER TABLE {TABLE_NAME}
        SET (autovacuum_enabled = true);
    """)
    
    cursor.execute(f"ANALYZE {TABLE_NAME};")
    conn.commit()
    
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
    final_count = cursor.fetchone()[0]
    elapsed = time.time() - start_time
    
    print(f"最终记录数: {final_count:,} (去重: {total_count - final_count:,})")
    print(f"优化耗时: {elapsed:.1f}秒")
    print("="*70)

# =============================================================================
# 主流程
# =============================================================================

def main():
    """主函数"""
    print("="*70)
    print("Step One - 构建完整的 corpusid 表")
    print(f"数据目录: {DATA_FOLDER}")
    print(f"批次大小: {BATCH_SIZE:,}")
    print("="*70)
    
    # 检查数据目录
    if not DATA_FOLDER.exists():
        print(f"❌ 数据目录不存在: {DATA_FOLDER}")
        return
    
    # 初始化断点续传记录器
    recorder = ProcessRecorder(machine='machine2')
    
    # 获取所有 gz 文件
    gz_files = sorted(DATA_FOLDER.glob("*.gz"))
    if not gz_files:
        print(f"❌ 未找到 gz 文件")
        recorder.close()
        return
    
    # 过滤已处理的文件
    pending_files = []
    skipped_count = 0
    for gz_file in gz_files:
        if recorder.is_processed(gz_file.name, DatasetType.PAPERS):  # paper-ids 使用 PAPERS 类型
            skipped_count += 1
        else:
            pending_files.append(gz_file)
    
    print(f"找到 {len(gz_files)} 个 gz 文件")
    print(f"已处理: {skipped_count} 个 | 待处理: {len(pending_files)} 个\n")
    
    if not pending_files:
        print("✓ 所有文件已处理完成")
        recorder.close()
        return
    
    # 连接数据库
    try:
        config = get_db_config('machine2')
        print(f"连接数据库: {config['database']}@{config['host']}:{config['port']}")
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        # 创建表
        if not create_table_if_not_exists(cursor):
            cursor.close()
            conn.close()
            recorder.close()
            return
        conn.commit()
        
        # 处理待处理的 gz 文件
        print("\n" + "="*70)
        print("开始处理 gz 文件")
        print("="*70)
        
        total_records = 0
        start_time = time.time()
        
        # 使用tqdm显示进度和预估时间
        with tqdm(total=len(pending_files), desc="处理进度", unit="file", 
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            for idx, gz_file in enumerate(pending_files, 1):
                file_start = time.time()
                records = process_gz_file(gz_file, cursor, conn)
                file_elapsed = time.time() - file_start
                total_records += records
                
                # 记录文件已处理（只有当前文件完全处理完才记录）
                recorder.add_record(gz_file.name, DatasetType.PAPERS)
                
                # 计算预估剩余时间
                elapsed = time.time() - start_time
                avg_time_per_file = elapsed / idx
                remaining_files = len(pending_files) - idx
                eta_seconds = avg_time_per_file * remaining_files
                eta_str = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
                
                pbar.set_postfix({
                    '当前': f'{records:,}条',
                    '总计': f'{total_records:,}条',
                    '速度': f'{records/file_elapsed:.0f}条/秒',
                    '预计剩余': eta_str
                })
                pbar.update(1)
        
        elapsed = time.time() - start_time
        print(f"\n总记录数: {total_records:,}")
        print(f"总耗时: {elapsed:.1f}秒 | 平均速度: {total_records/elapsed:.0f}条/秒")
        
        # 排序并建立索引
        build_index_and_sort(cursor, conn)
        
        # 关闭连接
        cursor.close()
        conn.close()
        recorder.close()
        print("\n✅ 所有操作完成！")
        
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        if 'recorder' in locals():
            recorder.close()
        return

if __name__ == '__main__':
    main()