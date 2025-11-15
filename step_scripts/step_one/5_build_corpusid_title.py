#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step One - 构建 corpusid_mapping_title 表

核心流程（3个独立阶段）：
【阶段0】创建 corpusid_mapping_title 表
【阶段1】导入 papers gz 文件（提取 corpusid 和 title）
【阶段2】创建主键索引（去重）

每个阶段独立执行，支持断点续传
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

# =============================================================================
# 配置
# =============================================================================

MAPPING_TABLE = 'corpusid_mapping_title'
LOG_TABLE = 'corpusid_title_import_log'
DATA_FOLDER = Path(r'D:\2025-09-30\papers')

# =============================================================================
# 阶段0：创建表
# =============================================================================

def create_tables(cursor, conn):
    """创建 corpusid_mapping_title 表和导入日志表"""
    print("\n【阶段0】创建表...")
    
    # 创建映射表（不创建主键，延迟到导入完成后）
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {MAPPING_TABLE} (
            corpusid BIGINT NOT NULL,
            title TEXT
        )
    """)
    
    # 创建导入日志表
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
            filename TEXT PRIMARY KEY,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    
    # 检查记录数
    cursor.execute(f"SELECT COUNT(*) FROM {MAPPING_TABLE}")
    count = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(*) FROM {LOG_TABLE}")
    log_count = cursor.fetchone()[0]
    
    print(f"✅ 表创建成功")
    print(f"  {MAPPING_TABLE}: {count:,}条")
    print(f"  {LOG_TABLE}: {log_count:,}个已导入文件")

# =============================================================================
# 阶段1：导入数据
# =============================================================================

def import_papers_gz(cursor, conn):
    """导入所有 papers gz 文件"""
    print("\n【阶段1】导入 papers 数据...")
    
    # 获取所有 gz 文件
    gz_files = sorted(DATA_FOLDER.glob("*.gz"))
    if not gz_files:
        raise FileNotFoundError(f"未找到 gz 文件: {DATA_FOLDER}")
    
    # 获取已导入的文件
    cursor.execute(f"SELECT filename FROM {LOG_TABLE}")
    imported_files = set(row[0] for row in cursor.fetchall())
    
    # 过滤未导入的文件
    pending_files = [f for f in gz_files if f.name not in imported_files]
    
    if not pending_files:
        print("✅ 所有文件已导入")
        return
    
    print(f"找到 {len(gz_files)} 个文件，待导入 {len(pending_files)} 个")
    
    # 优化数据库配置
    cursor.execute("SET synchronous_commit = OFF")
    cursor.execute("SET work_mem = '512MB'")
    
    # 创建临时表
    cursor.execute("""
        CREATE TEMP TABLE IF NOT EXISTS temp_papers (
            corpusid BIGINT,
            title TEXT
        ) ON COMMIT PRESERVE ROWS
    """)
    
    total_records = 0
    start_time = time.time()
    
    with tqdm(total=len(pending_files), desc="导入进度", unit="file") as pbar:
        for gz_file in pending_files:
            file_count = 0
            
            # 读取并插入数据
            with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
                batch = []
                for line in f:
                    try:
                        data = orjson.loads(line.strip())
                        corpusid = data.get('corpusid')
                        title = data.get('title', '')
                        
                        if corpusid is not None:
                            batch.append((corpusid, title or ''))
                            file_count += 1
                            
                            # 批量插入
                            if len(batch) >= 50000:
                                insert_batch(cursor, batch)
                                batch = []
                    except:
                        continue
                
                # 插入剩余数据
                if batch:
                    insert_batch(cursor, batch)
            
            # 从临时表插入到目标表
            cursor.execute(f"""
                INSERT INTO {MAPPING_TABLE} (corpusid, title)
                SELECT corpusid, title FROM temp_papers
            """)
            
            # 清空临时表
            cursor.execute("TRUNCATE temp_papers")
            
            # 记录已导入的文件
            cursor.execute(
                f"INSERT INTO {LOG_TABLE} (filename) VALUES (%s)",
                (gz_file.name,)
            )
            
            conn.commit()
            total_records += file_count
            
            pbar.set_postfix_str(f"总计: {total_records:,}条")
            pbar.update(1)
    
    elapsed = time.time() - start_time
    speed = total_records / elapsed if elapsed > 0 else 0
    print(f"\n✅ 导入完成: {total_records:,}条 | 耗时: {elapsed:.1f}秒 | 速度: {speed:.0f}条/秒")

def insert_batch(cursor, data_list):
    """批量插入数据到临时表"""
    from io import StringIO
    buffer = StringIO()
    for corpusid, title in data_list:
        # 转义特殊字符
        title_escaped = str(title).replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        buffer.write(f"{corpusid}\t{title_escaped}\n")
    buffer.seek(0)
    cursor.copy_from(buffer, 'temp_papers', columns=('corpusid', 'title'))

# =============================================================================
# 阶段2：创建主键索引
# =============================================================================

def create_primary_key(cursor, conn):
    """创建主键索引（去重）"""
    print("\n【阶段2】创建主键索引...")
    
    # 检查主键是否存在
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM pg_constraint 
            WHERE conrelid = %s::regclass AND contype = 'p'
        )
    """, (MAPPING_TABLE,))
    
    if cursor.fetchone()[0]:
        print("✅ 主键已存在，跳过创建")
        return
    
    start_time = time.time()
    
    # 优化参数
    cursor.execute("SET maintenance_work_mem = '4GB'")
    
    print("删除重复记录...")
    cursor.execute(f"""
        DELETE FROM {MAPPING_TABLE} a USING (
            SELECT MIN(ctid) as ctid, corpusid
            FROM {MAPPING_TABLE}
            GROUP BY corpusid
            HAVING COUNT(*) > 1
        ) b
        WHERE a.corpusid = b.corpusid AND a.ctid <> b.ctid
    """)
    dup_count = cursor.rowcount
    if dup_count > 0:
        print(f"  删除重复: {dup_count:,}条")
    
    print("创建主键...")
    cursor.execute(f"ALTER TABLE {MAPPING_TABLE} ADD PRIMARY KEY (corpusid)")
    
    print("收集统计信息...")
    cursor.execute(f"ANALYZE {MAPPING_TABLE}")
    
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"✅ 主键创建完成 | 耗时: {elapsed:.1f}秒")

# =============================================================================
# 主流程
# =============================================================================

def main():
    """主函数"""
    print("="*70)
    print("Step One - 构建 corpusid_mapping_title 表")
    print(f"数据目录: {DATA_FOLDER}")
    print("="*70)
    
    # 选择要执行的阶段
    print("\n可执行阶段:")
    print("  0. 创建表")
    print("  1. 导入 gz 文件")
    print("  2. 创建主键索引")
    print("  3. 全部执行（阶段0-2）")
    
    choice = input("\n请选择要执行的阶段 (输入数字): ").strip()
    
    # 连接数据库
    try:
        config = get_db_config('machine2')
        print(f"\n连接数据库: {config['database']}@{config['host']}:{config['port']}")
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        overall_start = time.time()
        
        if choice == '0':
            create_tables(cursor, conn)
        elif choice == '1':
            import_papers_gz(cursor, conn)
        elif choice == '2':
            create_primary_key(cursor, conn)
        elif choice == '3':
            create_tables(cursor, conn)
            import_papers_gz(cursor, conn)
            create_primary_key(cursor, conn)
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
