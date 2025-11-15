#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step One - 构建 citations 和 references 表

核心流程（5个独立阶段）：
【阶段0】创建 citation_raw 表
【阶段1】导入 gz 文件到 citation_raw 表（citingcorpusid, citedcorpusid）
【阶段2】创建索引（idx_citation_citing, idx_citation_cited）
【阶段3】构造 temp_references: corpusid -> array[citedcorpusid]
【阶段4】构造 temp_citations: corpusid -> array[citingcorpusid]

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
# 配置
# =============================================================================

CITATION_RAW_TABLE = 'citation_raw'
DATA_FOLDER = Path(r'D:\2025-09-30\citations')
BATCH_SIZE = 500000  # 每批次处理的行数

# 分区配置（citation_raw表：160GB, 30亿行）
PARTITION_CONFIG = {
    'min_id': 0,
    'max_id': 300000000,  # 3亿（corpusid范围）
    'partition_size': 10000000  # 每分区1000万ID范围，共30个分区，平均~1亿行，~5.3GB
}

# =============================================================================
# 阶段0：创建表
# =============================================================================

def create_citation_raw_table(cursor, conn):
    """创建 citation_raw 分区表（按 citingcorpusid 范围分区）"""
    print("\n【阶段0】创建 citation_raw 分区表...")
    
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{CITATION_RAW_TABLE}'
        );
    """)
    
    if cursor.fetchone()[0]:
        print(f"⚠️  表 {CITATION_RAW_TABLE} 已存在")
        response = input("是否删除并重建？(yes/no): ").strip().lower()
        if response != 'yes':
            print("跳过创建")
            return
        cursor.execute(f"DROP TABLE {CITATION_RAW_TABLE} CASCADE;")
        print(f"已删除旧表")
    
    # 创建分区主表
    print("创建分区主表...")
    cursor.execute(f"""
        CREATE TABLE {CITATION_RAW_TABLE} (
            citingcorpusid BIGINT NOT NULL,
            citedcorpusid BIGINT NOT NULL
        ) PARTITION BY RANGE (citingcorpusid);
    """)
    
    # 创建所有分区
    min_id = PARTITION_CONFIG['min_id']
    max_id = PARTITION_CONFIG['max_id']
    partition_size = PARTITION_CONFIG['partition_size']
    
    print(f"创建分区（每分区{partition_size:,}个ID范围）...")
    
    partitions = []
    current_min = min_id
    partition_num = 0
    
    while current_min < max_id:
        current_max = min(current_min + partition_size, max_id)
        partition_name = f"{CITATION_RAW_TABLE}_p{partition_num}"
        
        cursor.execute(f"""
            CREATE TABLE {partition_name} PARTITION OF {CITATION_RAW_TABLE}
            FOR VALUES FROM ({current_min}) TO ({current_max})
            WITH (fillfactor = 100, autovacuum_enabled = false);
        """)
        
        partitions.append(partition_name)
        current_min = current_max
        partition_num += 1
    
    # 创建默认分区
    default_partition = f"{CITATION_RAW_TABLE}_default"
    cursor.execute(f"""
        CREATE TABLE {default_partition} PARTITION OF {CITATION_RAW_TABLE}
        DEFAULT WITH (fillfactor = 100, autovacuum_enabled = false);
    """)
    
    conn.commit()
    print(f"✅ 表创建成功：{len(partitions)}个分区 + 1个默认分区")

# =============================================================================
# 阶段1：导入数据
# =============================================================================

def import_citations_gz(cursor, conn):
    """导入所有 gz 文件到 citation_raw 表（支持断点续传）"""
    print("\n【阶段1】导入 citations 数据...")
    
    # 初始化断点续传记录器
    recorder = ProcessRecorder(machine='machine2')
    
    # 获取所有 gz 文件
    gz_files = sorted(DATA_FOLDER.glob("*.gz"))
    if not gz_files:
        raise FileNotFoundError(f"未找到 gz 文件: {DATA_FOLDER}")
    
    # 过滤已处理的文件
    pending_files = []
    skipped_count = 0
    for gz_file in gz_files:
        if recorder.is_processed(gz_file.name, DatasetType.CITATIONS):
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
            file_start = time.time()
            batch_buffer = []
            
            with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = orjson.loads(line.strip())
                        citing = data.get('citingcorpusid')
                        cited = data.get('citedcorpusid')
                        
                        if citing is not None and cited is not None:
                            batch_buffer.append((citing, cited))
                            
                            # 批量插入
                            if len(batch_buffer) >= BATCH_SIZE:
                                insert_batch(cursor, batch_buffer)
                                total_records += len(batch_buffer)
                                batch_buffer = []
                                conn.commit()
                    except:
                        continue
            
            # 插入剩余数据
            if batch_buffer:
                insert_batch(cursor, batch_buffer)
                total_records += len(batch_buffer)
                conn.commit()
            
            # 记录文件已处理（只有当前文件完全处理完才记录）
            recorder.add_record(gz_file.name, DatasetType.CITATIONS)
            
            file_time = time.time() - file_start
            pbar.set_postfix_str(f"总计: {total_records:,}条")
            pbar.update(1)
    
    elapsed = time.time() - start_time
    speed = total_records / elapsed if elapsed > 0 else 0
    print(f"\n✅ 导入完成: {total_records:,}条 | 耗时: {elapsed:.1f}秒 | 速度: {speed:.0f}条/秒")
    recorder.close()

def insert_batch(cursor, data_list):
    """批量插入数据"""
    from io import StringIO
    buffer = StringIO()
    for citing, cited in data_list:
        buffer.write(f"{citing}\t{cited}\n")
    buffer.seek(0)
    cursor.copy_from(buffer, CITATION_RAW_TABLE, columns=('citingcorpusid', 'citedcorpusid'))

# =============================================================================
# 阶段2：创建索引
# =============================================================================

def create_indexes(cursor, conn):
    """为所有分区创建索引"""
    print("\n【阶段2】创建索引...")
    
    # 获取所有分区表
    cursor.execute(f"""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename LIKE '{CITATION_RAW_TABLE}_p%'
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
    with tqdm(total=len(partitions) * 2, desc="创建索引", unit="索引") as pbar:
        for partition in partitions:
            # 创建 citingcorpusid 索引
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {partition}_citing_idx 
                ON {partition} (citingcorpusid)
            """)
            pbar.update(1)
            
            # 创建 citedcorpusid 索引
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {partition}_cited_idx 
                ON {partition} (citedcorpusid)
            """)
            pbar.update(1)
            
            conn.commit()
    
    # 收集统计信息
    print("收集统计信息...")
    cursor.execute(f"ANALYZE {CITATION_RAW_TABLE}")
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"✅ 索引创建完成：{len(partitions)}个分区 × 2个索引 | 耗时: {elapsed:.1f}秒")

# =============================================================================
# 阶段3：构造 references
# =============================================================================

def build_references(cursor, conn):
    """构造 temp_references: corpusid -> array[citedcorpusid]"""
    print("\n【阶段3】构造 temp_references...")
    
    # 检查是否已存在
    cursor.execute("SELECT to_regclass('temp_references')")
    if cursor.fetchone()[0]:
        cursor.execute("SELECT COUNT(*) FROM temp_references")
        count = cursor.fetchone()[0]
        print(f"⚠️  temp_references 已存在（{count:,}条）")
        response = input("是否重建？(yes/no): ").strip().lower()
        if response != 'yes':
            print("跳过重建")
            return
    
    start_time = time.time()
    
    # 优化配置
    cursor.execute("SET work_mem = '8GB'")
    cursor.execute("SET temp_buffers = '2GB'")
    
    # 构建缓存表
    print("聚合数据（citingcorpusid -> array[citedcorpusid]）...")
    cursor.execute("DROP TABLE IF EXISTS temp_references")
    cursor.execute(f"""
        CREATE UNLOGGED TABLE temp_references AS
        SELECT 
            citingcorpusid AS corpusid,
            array_agg(citedcorpusid) AS ref_ids
        FROM {CITATION_RAW_TABLE}
        GROUP BY citingcorpusid
    """)
    
    # 创建索引
    print("创建索引...")
    cursor.execute("SET maintenance_work_mem = '4GB'")
    cursor.execute("CREATE INDEX idx_temp_references_corpusid ON temp_references (corpusid)")
    
    # 统计结果
    cursor.execute("SELECT COUNT(*) FROM temp_references")
    count = cursor.fetchone()[0]
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"✅ 完成: {count:,}条 | 耗时: {elapsed:.1f}秒")

# =============================================================================
# 阶段4：构造 citations
# =============================================================================

def build_citations(cursor, conn):
    """构造 temp_citations: corpusid -> array[citingcorpusid]"""
    print("\n【阶段4】构造 temp_citations...")
    
    # 检查是否已存在
    cursor.execute("SELECT to_regclass('temp_citations')")
    if cursor.fetchone()[0]:
        cursor.execute("SELECT COUNT(*) FROM temp_citations")
        count = cursor.fetchone()[0]
        print(f"⚠️  temp_citations 已存在（{count:,}条）")
        response = input("是否重建？(yes/no): ").strip().lower()
        if response != 'yes':
            print("跳过重建")
            return
    
    start_time = time.time()
    
    # 优化配置
    cursor.execute("SET work_mem = '8GB'")
    cursor.execute("SET temp_buffers = '2GB'")
    
    # 构建缓存表
    print("聚合数据（citedcorpusid -> array[citingcorpusid]）...")
    cursor.execute("DROP TABLE IF EXISTS temp_citations")
    cursor.execute(f"""
        CREATE UNLOGGED TABLE temp_citations AS
        SELECT 
            citedcorpusid AS corpusid,
            array_agg(citingcorpusid) AS cite_ids
        FROM {CITATION_RAW_TABLE}
        GROUP BY citedcorpusid
    """)
    
    # 创建索引
    print("创建索引...")
    cursor.execute("SET maintenance_work_mem = '4GB'")
    cursor.execute("CREATE INDEX idx_temp_citations_corpusid ON temp_citations (corpusid)")
    
    # 统计结果
    cursor.execute("SELECT COUNT(*) FROM temp_citations")
    count = cursor.fetchone()[0]
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"✅ 完成: {count:,}条 | 耗时: {elapsed:.1f}秒")

# =============================================================================
# 主流程
# =============================================================================

def main():
    """主函数"""
    print("="*70)
    print("Step One - 构建 citations 和 references 表")
    print(f"数据目录: {DATA_FOLDER}")
    print(f"批次大小: {BATCH_SIZE:,}")
    print("="*70)
    
    # 选择要执行的阶段
    print("\n可执行阶段:")
    print("  0. 创建 citation_raw 表")
    print("  1. 导入 gz 文件")
    print("  2. 创建索引")
    print("  3. 构造 temp_references")
    print("  4. 构造 temp_citations")
    print("  5. 全部执行（阶段0-4）")
    
    choice = input("\n请选择要执行的阶段 (输入数字): ").strip()
    
    # 连接数据库
    try:
        config = get_db_config('machine2')
        print(f"\n连接数据库: {config['database']}@{config['host']}:{config['port']}")
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        overall_start = time.time()
        
        if choice == '0':
            create_citation_raw_table(cursor, conn)
        elif choice == '1':
            import_citations_gz(cursor, conn)
        elif choice == '2':
            create_indexes(cursor, conn)
        elif choice == '3':
            build_references(cursor, conn)
        elif choice == '4':
            build_citations(cursor, conn)
        elif choice == '5':
            create_citation_raw_table(cursor, conn)
            import_citations_gz(cursor, conn)
            create_indexes(cursor, conn)
            build_references(cursor, conn)
            build_citations(cursor, conn)
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
