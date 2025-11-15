#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step One - 构建 papers/abstracts/tldrs 分区表
功能：
1. 创建分区主表和子分区表（按 corpusid 范围分区）
2. 解压 papers/abstracts/tldrs 数据集的所有 gz 文件
3. 提取 corpusid 和完整 JSON 数据
4. 使用 COPY 批量插入到分区表
5. 完成后对每个分区的 corpusid 建立索引
"""

import sys
import gzip
import time
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

# 数据集配置
DATASETS = {
    'papers': {
        'folder': Path(r'D:\2025-09-30\papers'),
        'table': 'papers',
        'dataset_type': DatasetType.PAPERS,
        'description': 'Papers 数据集',
        'estimated_size_gb': 250,
        'estimated_rows': 182133000,  # 实际1.82亿行
        'partition_size': 10000000  # 每分区1000万corpusid范围，共30个分区，平均~607万行/分区，~8.3GB/分区
    },
    'abstracts': {
        'folder': Path(r'D:\2025-09-30\abstracts'),
        'table': 'abstracts',
        'dataset_type': DatasetType.ABSTRACTS,
        'description': 'Abstracts 数据集',
        'estimated_size_gb': 88,
        'estimated_rows': 35327610,  # 实际3533万行
        'partition_size': 30000000  # 每分区3000万corpusid范围，共10个分区，平均~353万行/分区，~8.8GB/分区
    },
    'tldrs': {
        'folder': Path(r'D:\2025-09-30\tldrs'),
        'table': 'tldrs',
        'dataset_type': DatasetType.TLDRS,
        'description': 'TLDRs 数据集',
        'estimated_size_gb': 88,
        'estimated_rows': 71655840,  # 实际7166万行
        'partition_size': 30000000  # 每分区3000万corpusid范围，共10个分区，平均~717万行/分区，~8.8GB/分区
    }
}

# 全局分区配置
PARTITION_CONFIG = {
    'min_corpusid': 0,
    'max_corpusid': 300000000,  # 3亿（覆盖所有可能的corpusid）
}

BATCH_SIZE = 500000  # 每批次处理的行数

# =============================================================================
# 分区表管理
# =============================================================================

def create_partitioned_table(cursor, table_name, partition_size):
    """
    创建分区主表和所有子分区表
    
    Args:
        cursor: 数据库游标
        table_name: 表名
        partition_size: 每个分区的记录数
    """
    # 检查主表是否存在
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
        );
    """)
    
    if cursor.fetchone()[0]:
        print(f"⚠️  表 {table_name} 已存在")
        response = input("是否删除并重建？(yes/no): ").strip().lower()
        if response != 'yes':
            return False
        cursor.execute(f"DROP TABLE {table_name} CASCADE;")
        print(f"已删除旧表 {table_name}")
    
    # 创建分区主表（使用TEXT类型存储JSON，速度更快）
    print(f"\n创建分区主表 {table_name}...")
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            corpusid BIGINT NOT NULL,
            data TEXT NOT NULL
        ) PARTITION BY RANGE (corpusid);
    """)
    
    # 计算分区数量
    min_id = PARTITION_CONFIG['min_corpusid']
    max_id = PARTITION_CONFIG['max_corpusid']
    
    partitions = []
    current_min = min_id
    partition_num = 0
    
    print(f"创建分区表（每分区 {partition_size:,} 条记录）...")
    
    while current_min < max_id:
        current_max = min(current_min + partition_size, max_id)
        partition_name = f"{table_name}_p{partition_num}"
        
        cursor.execute(f"""
            CREATE TABLE {partition_name} PARTITION OF {table_name}
            FOR VALUES FROM ({current_min}) TO ({current_max});
        """)
        
        partitions.append({
            'name': partition_name,
            'min': current_min,
            'max': current_max
        })
        
        current_min = current_max
        partition_num += 1
    
    # 创建默认分区（捕获超出范围的数据）
    default_partition = f"{table_name}_default"
    cursor.execute(f"""
        CREATE TABLE {default_partition} PARTITION OF {table_name}
        DEFAULT;
    """)
    
    print(f"✅ 创建了 {len(partitions)} 个分区 + 1 个默认分区")
    print(f"   分区范围: {min_id:,} - {max_id:,}")
    print(f"   每分区大小: {partition_size:,} 条记录")
    
    return True

def build_partition_indexes(cursor, conn, table_name):
    """
    为所有分区建立索引
    
    Args:
        cursor: 数据库游标
        conn: 数据库连接
        table_name: 表名
    """
    print(f"\n{'='*70}")
    print(f"为 {table_name} 的所有分区建立索引...")
    
    # 获取所有分区表
    cursor.execute(f"""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename LIKE '{table_name}_p%'
        ORDER BY tablename;
    """)
    
    partitions = [row[0] for row in cursor.fetchall()]
    
    if not partitions:
        print("⚠️  未找到分区表")
        return
    
    print(f"找到 {len(partitions)} 个分区")
    
    start_time = time.time()
    
    with tqdm(total=len(partitions), desc="建立索引", unit="分区") as pbar:
        for partition in partitions:
            try:
                # 为每个分区建立主键索引
                cursor.execute(f"""
                    ALTER TABLE {partition}
                    ADD PRIMARY KEY (corpusid);
                """)
                conn.commit()
                pbar.update(1)
            except Exception as e:
                print(f"\n⚠️  分区 {partition} 索引创建失败: {e}")
                conn.rollback()
    
    elapsed = time.time() - start_time
    print(f"✅ 索引创建完成 (耗时: {elapsed:.1f}秒)")
    
    # 更新统计信息
    print("更新统计信息...")
    cursor.execute(f"ANALYZE {table_name};")
    conn.commit()
    
    print(f"{'='*70}")

# =============================================================================
# 数据处理
# =============================================================================

def process_gz_file(gz_path, cursor, conn, table_name):
    """
    处理单个 gz 文件，提取数据并批量插入
    
    Args:
        gz_path: gz 文件路径
        cursor: 数据库游标
        conn: 数据库连接
        table_name: 表名
    
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
                        # 将完整的 JSON 数据转换为字符串
                        json_str = orjson.dumps(data).decode('utf-8')
                        batch_buffer.append((corpusid, json_str))
                        
                        # 达到批次大小时执行插入
                        if len(batch_buffer) >= BATCH_SIZE:
                            insert_batch(cursor, table_name, batch_buffer)
                            total_inserted += len(batch_buffer)
                            batch_buffer = []
                            conn.commit()
                
                except Exception as e:
                    # 跳过解析失败的行
                    continue
        
        # 插入剩余数据
        if batch_buffer:
            insert_batch(cursor, table_name, batch_buffer)
            total_inserted += len(batch_buffer)
            conn.commit()
    
    except Exception as e:
        print(f"❌ 处理文件失败 {gz_path.name}: {e}")
        conn.rollback()
        raise
    
    return total_inserted

def insert_batch(cursor, table_name, data_list):
    """使用 COPY 批量插入数据"""
    buffer = StringIO()
    for corpusid, json_data in data_list:
        # 转义特殊字符
        json_escaped = json_data.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        buffer.write(f"{corpusid}\t{json_escaped}\n")
    buffer.seek(0)
    
    cursor.copy_from(buffer, table_name, columns=('corpusid', 'data'))

# =============================================================================
# 主流程
# =============================================================================

def process_dataset(dataset_name, dataset_config, conn, cursor):
    """
    处理单个数据集（支持断点续传）
    
    Args:
        dataset_name: 数据集名称
        dataset_config: 数据集配置
        conn: 数据库连接
        cursor: 数据库游标
    """
    folder = dataset_config['folder']
    table_name = dataset_config['table']
    dataset_type = dataset_config['dataset_type']
    description = dataset_config['description']
    
    print("\n" + "="*70)
    print(f"处理数据集: {description}")
    print(f"数据目录: {folder}")
    print(f"目标表: {table_name}")
    print("="*70)
    
    # 检查数据目录
    if not folder.exists():
        print(f"⚠️  数据目录不存在，跳过: {folder}")
        return
    
    # 初始化断点续传记录器
    recorder = ProcessRecorder(machine='machine2')
    
    # 获取所有 gz 文件
    gz_files = sorted(folder.glob("*.gz"))
    if not gz_files:
        print(f"⚠️  未找到 gz 文件，跳过")
        recorder.close()
        return
    
    # 过滤已处理的文件
    pending_files = []
    skipped_count = 0
    for gz_file in gz_files:
        if recorder.is_processed(gz_file.name, dataset_type):
            skipped_count += 1
        else:
            pending_files.append(gz_file)
    
    print(f"找到 {len(gz_files)} 个 gz 文件")
    print(f"已处理: {skipped_count} 个 | 待处理: {len(pending_files)} 个")
    
    if not pending_files:
        print("✓ 所有文件已处理完成")
        recorder.close()
        return
    
    # 创建分区表（使用数据集特定的分区大小）
    partition_size = dataset_config['partition_size']
    if not create_partitioned_table(cursor, table_name, partition_size):
        print("跳过该数据集")
        return
    conn.commit()
    
    # 处理待处理的 gz 文件
    print(f"\n开始处理 gz 文件...")
    
    total_records = 0
    start_time = time.time()
    
    with tqdm(total=len(pending_files), desc="处理进度", unit="file",
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
        for idx, gz_file in enumerate(pending_files, 1):
            file_start = time.time()
            records = process_gz_file(gz_file, cursor, conn, table_name)
            file_elapsed = time.time() - file_start
            total_records += records
            
            # 记录文件已处理（只有当前文件完全处理完才记录）
            recorder.add_record(gz_file.name, dataset_type)
            
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
    recorder.close()
    
    # 建立索引
    build_partition_indexes(cursor, conn, table_name)
    
    print(f"\n✅ {description} 处理完成！")

def main():
    """主函数"""
    print("="*70)
    print("Step One - 构建 papers/abstracts/tldrs 分区表")
    print(f"批次大小: {BATCH_SIZE:,}")
    print(f"数据类型: corpusid (BIGINT) + data (TEXT)")
    print("="*70)
    
    # 选择要处理的数据集
    print("\n可用数据集:")
    for idx, (name, config) in enumerate(DATASETS.items(), 1):
        print(f"  {idx}. {config['description']} ({name})")
    print(f"  {len(DATASETS)+1}. 全部处理")
    
    choice = input("\n请选择要处理的数据集 (输入数字): ").strip()
    
    try:
        choice_num = int(choice)
        if choice_num == len(DATASETS) + 1:
            selected_datasets = list(DATASETS.items())
        elif 1 <= choice_num <= len(DATASETS):
            dataset_name = list(DATASETS.keys())[choice_num - 1]
            selected_datasets = [(dataset_name, DATASETS[dataset_name])]
        else:
            print("❌ 无效选择")
            return
    except ValueError:
        print("❌ 无效输入")
        return
    
    # 连接数据库
    try:
        config = get_db_config('machine2')
        print(f"\n连接数据库: {config['database']}@{config['host']}:{config['port']}")
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        # 处理选中的数据集
        for dataset_name, dataset_config in selected_datasets:
            process_dataset(dataset_name, dataset_config, conn, cursor)
        
        # 关闭连接
        cursor.close()
        conn.close()
        print("\n" + "="*70)
        print("✅ 所有操作完成！")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return

if __name__ == '__main__':
    main()