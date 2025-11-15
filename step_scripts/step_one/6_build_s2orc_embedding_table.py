#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step One - 构建 s2orc 和 embeddings 表

数据集配置：
- s2orc: 1500GB, 3000万行 -> machine0 (E盘)
- s2orc_v2: 650GB, 1000万行 -> machine0 (E盘)
- embeddings_specter_v1: 2200GB, 约3000万行 -> machine3 (E盘)
- embeddings_specter_v2: 2200GB, 约3000万行 -> machine3 (E盘)

核心流程（3个独立阶段）：
【阶段0】创建分区表
【阶段1】导入 gz 文件（提取 corpusid 和对应字段）
【阶段2】创建索引

每个阶段独立执行，可通过命令行参数控制
"""

import sys
import gzip
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import orjson
import psycopg2
from tqdm import tqdm

from step_scripts.step_one.machine_db_config import get_db_config
from step_scripts.step_one.init_process_table import ProcessRecorder, DatasetType

# =============================================================================
# 数据集配置
# =============================================================================

DATASETS = {
    's2orc': {
        'table_name': 's2orc',
        'data_folder': Path(r'E:\2025-09-30\s2orc'),
        'machine': 'machine0',
        'dataset_type': DatasetType.S2ORC,
        'estimated_size_gb': 1500,
        'estimated_rows': 30000000,
        'partition_count': 150,  # 每分区200万ID，约20万行，~10GB
        'partition_size': 2000000,  # 200万ID/分区
    },
    's2orc_v2': {
        'table_name': 's2orc_v2',
        'data_folder': Path(r'E:\2025-09-30\s2orc_v2'),
        'machine': 'machine0',
        'dataset_type': DatasetType.S2ORC_V2,
        'estimated_size_gb': 650,
        'estimated_rows': 10000000,
        'partition_count': 65,  # 每分区约462万ID，约15.4万行，~10GB
        'partition_size': 4620000,  # 462万ID/分区
    },
    'embeddings_specter_v1': {
        'table_name': 'embeddings_specter_v1',
        'data_folder': Path(r'E:\2025-09-30\embeddings-specter_v1'),
        'machine': 'machine3',
        'dataset_type': DatasetType.EMBEDDINGS_SPECTER_V1,
        'estimated_size_gb': 2200,
        'estimated_rows': 30000000,
        'partition_count': 220,  # 每分区约136万ID，约13.6万行，~10GB
        'partition_size': 1360000,  # 136万ID/分区
    },
    'embeddings_specter_v2': {
        'table_name': 'embeddings_specter_v2',
        'data_folder': Path(r'E:\2025-09-30\embeddings-specter_v2'),
        'machine': 'machine3',
        'dataset_type': DatasetType.EMBEDDINGS_SPECTER_V2,
        'estimated_size_gb': 2200,
        'estimated_rows': 30000000,
        'partition_count': 220,  # 每分区约136万ID，约13.6万行，~10GB
        'partition_size': 1360000,  # 136万ID/分区
    }
}

BATCH_SIZE = 50000  # 每批次处理的行数

# =============================================================================
# 阶段0：创建表
# =============================================================================

def create_table(dataset_key, cursor, conn):
    """创建分区表（corpusid + data）"""
    dataset = DATASETS[dataset_key]
    table_name = dataset['table_name']
    
    print(f"\n【阶段0】创建 {table_name} 分区表...")
    print(f"  预估大小: {dataset['estimated_size_gb']}GB")
    print(f"  预估行数: {dataset['estimated_rows']:,}行")
    print(f"  分区数量: {dataset['partition_count']}个")
    
    # 检查表是否存在
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
            print("跳过创建")
            return
        cursor.execute(f"DROP TABLE {table_name} CASCADE;")
        print(f"已删除旧表")
    
    # 创建分区主表（只有 corpusid 和 data 两个字段）
    print("创建分区主表...")
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            corpusid BIGINT NOT NULL,
            data TEXT NOT NULL
        ) PARTITION BY RANGE (corpusid);
    """)
    
    # 创建所有分区（使用数据集特定的分区大小）
    partition_size = dataset['partition_size']
    min_id = 0
    max_id = 300000000  # 3亿corpusid范围
    
    print(f"创建分区（每分区{partition_size:,}个ID范围）...")
    
    partitions = []
    current_min = min_id
    partition_num = 0
    
    with tqdm(total=dataset['partition_count'], desc="创建分区", unit="分区") as pbar:
        while current_min < max_id:
            current_max = min(current_min + partition_size, max_id)
            partition_name = f"{table_name}_p{partition_num}"
            
            cursor.execute(f"""
                CREATE TABLE {partition_name} PARTITION OF {table_name}
                FOR VALUES FROM ({current_min}) TO ({current_max})
                WITH (fillfactor = 100, autovacuum_enabled = false);
            """)
            
            partitions.append(partition_name)
            current_min = current_max
            partition_num += 1
            pbar.update(1)
    
    # 创建默认分区
    default_partition = f"{table_name}_default"
    cursor.execute(f"""
        CREATE TABLE {default_partition} PARTITION OF {table_name}
        DEFAULT WITH (fillfactor = 100, autovacuum_enabled = false);
    """)
    
    conn.commit()
    print(f"✅ 表创建成功：{len(partitions)}个分区 + 1个默认分区")

# =============================================================================
# 阶段1：导入数据
# =============================================================================

def import_dataset_gz(dataset_key, cursor, conn):
    """导入数据集的所有 gz 文件（支持断点续传）"""
    dataset = DATASETS[dataset_key]
    table_name = dataset['table_name']
    data_folder = dataset['data_folder']
    machine = dataset['machine']
    dataset_type = dataset['dataset_type']
    
    print(f"\n【阶段1】导入 {table_name} 数据...")
    
    # 初始化断点续传记录器
    recorder = ProcessRecorder(machine=machine)
    
    # 获取所有 gz 文件
    gz_files = sorted(data_folder.glob("*.gz"))
    if not gz_files:
        raise FileNotFoundError(f"未找到 gz 文件: {data_folder}")
    
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
    
    # 优化数据库配置
    cursor.execute("SET synchronous_commit = OFF")
    cursor.execute("SET work_mem = '512MB'")
    
    total_records = 0
    start_time = time.time()
    
    with tqdm(total=len(pending_files), desc="导入进度", unit="file") as pbar:
        for gz_file in pending_files:
            file_count = 0
            batch_buffer = []
            
            with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        line_stripped = line.strip()
                        if not line_stripped:
                            continue
                        
                        # 解析JSON获取corpusid
                        data = orjson.loads(line_stripped)
                        corpusid = data.get('corpusid')
                        
                        # 检查 corpusid 是否存在
                        if corpusid is not None:
                            # 存储整行JSON数据
                            batch_buffer.append((corpusid, line_stripped))
                            file_count += 1
                            
                            # 批量插入
                            if len(batch_buffer) >= BATCH_SIZE:
                                insert_batch(cursor, table_name, batch_buffer)
                                total_records += len(batch_buffer)
                                batch_buffer = []
                                conn.commit()
                    except:
                        continue
            
            # 插入剩余数据
            if batch_buffer:
                insert_batch(cursor, table_name, batch_buffer)
                total_records += len(batch_buffer)
                conn.commit()
            
            # 记录文件已处理（只有当前文件完全处理完才记录）
            recorder.add_record(gz_file.name, dataset_type)
            
            pbar.set_postfix_str(f"总计: {total_records:,}条")
            pbar.update(1)
    
    elapsed = time.time() - start_time
    speed = total_records / elapsed if elapsed > 0 else 0
    print(f"\n✅ 导入完成: {total_records:,}条 | 耗时: {elapsed:.1f}秒 | 速度: {speed:.0f}条/秒")
    recorder.close()

def insert_batch(cursor, table_name, data_list):
    """批量插入数据（corpusid + data）"""
    from io import StringIO
    buffer = StringIO()
    for corpusid, data in data_list:
        # 转义特殊字符
        data_escaped = data.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        buffer.write(f"{corpusid}\t{data_escaped}\n")
    buffer.seek(0)
    cursor.copy_from(buffer, table_name, columns=('corpusid', 'data'))

# =============================================================================
# 阶段2：创建索引
# =============================================================================

def create_indexes(dataset_key, cursor, conn):
    """为所有分区创建索引"""
    dataset = DATASETS[dataset_key]
    table_name = dataset['table_name']
    
    print(f"\n【阶段2】创建 {table_name} 索引...")
    
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
    
    # 优化索引构建参数
    cursor.execute("SET maintenance_work_mem = '8GB'")
    cursor.execute("SET max_parallel_maintenance_workers = 8")
    
    # 为每个分区创建索引
    with tqdm(total=len(partitions), desc="创建索引", unit="分区") as pbar:
        for partition in partitions:
            # 创建 corpusid 索引
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {partition}_corpusid_idx 
                ON {partition} (corpusid)
            """)
            pbar.update(1)
            conn.commit()
    
    # 收集统计信息
    print("收集统计信息...")
    cursor.execute(f"ANALYZE {table_name}")
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"✅ 索引创建完成：{len(partitions)}个分区 | 耗时: {elapsed:.1f}秒")

# =============================================================================
# 主流程
# =============================================================================

def main():
    """主函数"""
    print("="*70)
    print("Step One - 构建 s2orc 和 embeddings 表")
    print("="*70)
    
    # 选择数据集
    print("\n可用数据集:")
    for idx, (key, dataset) in enumerate(DATASETS.items()):
        print(f"  {idx}. {dataset['table_name']} ({dataset['estimated_size_gb']}GB, {dataset['estimated_rows']:,}行) -> {dataset['machine']}")
    
    dataset_choice = input("\n请选择数据集 (输入数字): ").strip()
    
    try:
        dataset_key = list(DATASETS.keys())[int(dataset_choice)]
        dataset = DATASETS[dataset_key]
    except (ValueError, IndexError):
        print("❌ 无效选择")
        return
    
    # 选择要执行的阶段
    print(f"\n数据集: {dataset['table_name']}")
    print(f"数据目录: {dataset['data_folder']}")
    print(f"目标机器: {dataset['machine']}")
    print("\n可执行阶段:")
    print("  0. 创建分区表")
    print("  1. 导入 gz 文件")
    print("  2. 创建索引")
    print("  3. 全部执行（阶段0-2）")
    
    stage_choice = input("\n请选择要执行的阶段 (输入数字): ").strip()
    
    # 连接数据库
    try:
        machine = dataset['machine']
        config = get_db_config(machine)
        print(f"\n连接数据库: {config['database']}@{config['host']}:{config['port']}")
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        overall_start = time.time()
        
        if stage_choice == '0':
            create_table(dataset_key, cursor, conn)
        elif stage_choice == '1':
            import_dataset_gz(dataset_key, cursor, conn)
        elif stage_choice == '2':
            create_indexes(dataset_key, cursor, conn)
        elif stage_choice == '3':
            create_table(dataset_key, cursor, conn)
            import_dataset_gz(dataset_key, cursor, conn)
            create_indexes(dataset_key, cursor, conn)
        else:
            print("❌ 无效选择")
            return
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        total_time = time.time() - overall_start
        print("\n" + "="*70)
        print(f"✅ 操作完成 | 总耗时: {total_time/60:.1f}分钟")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
            conn.close()

if __name__ == '__main__':
    main()
