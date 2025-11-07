"""
高性能导入papers数据到corpusid_mapping_title表

处理流程：
1. 创建corpusid_mapping_title表和导入日志表
2. 扫描所有papers gz文件
3. 跳过已导入的文件（断点续传）
4. 提取corpusid和title字段批量导入

性能优化：
- 使用COPY命令批量导入
- 关闭同步提交
- ON CONFLICT DO NOTHING跳过重复
"""
import sys
from pathlib import Path

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import gzip
import orjson
import psycopg2
from datetime import datetime
import time
from tqdm import tqdm
from db_config import get_db_config

MAPPING_TABLE = "corpusid_mapping_title"
LOG_TABLE = "papers_import_log"


class CopyStream:
    """将行迭代器包装为psycopg2 copy_expert可消费的流"""

    def __init__(self, iterator, chunk_size=65536):
        self.iterator = iterator
        self.chunk_size = chunk_size
        self._buffer = bytearray()
        self._exhausted = False

    def readable(self):
        return True

    def read(self, size=-1):
        if size == -1:
            chunks = [bytes(self._buffer)]
            self._buffer.clear()
            for chunk in self.iterator:
                chunks.append(chunk)
            self._exhausted = True
            return b''.join(chunks)

        while len(self._buffer) < max(size, self.chunk_size) and not self._exhausted:
            try:
                self._buffer.extend(next(self.iterator))
            except StopIteration:
                self._exhausted = True
                break

        if not self._buffer:
            return b''

        if size >= len(self._buffer):
            data = bytes(self._buffer)
            self._buffer.clear()
            return data

        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data


def create_tables(cursor):
    """创建corpusid_mapping_title表和导入日志表（不创建主键）"""
    print("\n【创建表】")
    
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
    
    # 检查记录数
    cursor.execute(f"SELECT COUNT(*) FROM {MAPPING_TABLE}")
    count = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(*) FROM {LOG_TABLE}")
    log_count = cursor.fetchone()[0]
    
    print(f"  ✓ {MAPPING_TABLE}: {count:,} 条记录")
    print(f"  ✓ {LOG_TABLE}: {log_count:,} 个已导入文件")


def get_imported_files(cursor):
    """获取已导入的文件列表"""
    cursor.execute(f"SELECT filename FROM {LOG_TABLE}")
    return set(row[0] for row in cursor.fetchall())


def log_imported_file(cursor, filename):
    """记录已导入的文件"""
    cursor.execute(
        f"INSERT INTO {LOG_TABLE} (filename) VALUES (%s) ON CONFLICT (filename) DO NOTHING",
        (filename,)
    )


def import_papers_gz(gz_directory, cursor, conn):
    """导入所有papers gz文件到corpusid_mapping_title表"""
    print("\n【导入papers数据】")
    
    gz_dir = Path(gz_directory)
    all_gz_files = sorted(gz_dir.glob("*.gz"))
    
    if not all_gz_files:
        raise FileNotFoundError(f"在 {gz_directory} 中没有找到gz文件")
    
    print(f"  发现 {len(all_gz_files)} 个gz文件")
    
    # 获取已导入的文件
    imported_files = get_imported_files(cursor)
    gz_files = [f for f in all_gz_files if f.name not in imported_files]
    
    if not gz_files:
        print("  所有文件已导入，无需处理")
        return
    
    print(f"  待导入: {len(gz_files)} 个文件")
    if imported_files:
        print(f"  已跳过: {len(imported_files)} 个文件（已导入）")
    
    # 优化数据库配置
    cursor.execute("SET synchronous_commit = OFF")
    cursor.execute("SET work_mem = '512MB'")
    
    # 创建临时表用于批量导入
    cursor.execute(f"""
        CREATE TEMP TABLE temp_papers (
            corpusid BIGINT,
            title TEXT
        ) ON COMMIT DROP
    """)
    
    copy_sql = "COPY temp_papers (corpusid, title) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '')"
    
    total_records = 0
    total_inserted = 0
    start_time = time.time()
    
    with tqdm(gz_files, desc="  导入进度", unit="file") as pbar:
        for gz_file in pbar:
            file_start = time.time()
            file_count = 0
            
            def row_iterator():
                nonlocal file_count
                with gzip.open(gz_file, 'rt', encoding='utf-8', errors='replace') as f:
                    for line in f:
                        try:
                            data = orjson.loads(line.strip())
                            corpusid = data.get('corpusid')
                            title = data.get('title', '')
                            
                            if corpusid is not None:
                                file_count += 1
                                # 转义特殊字符
                                if title is None:
                                    title = ''
                                title_escaped = str(title).replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                                yield f"{corpusid}\t{title_escaped}\n".encode('utf-8')
                        except Exception:
                            continue
            
            try:
                # 先导入到临时表
                cursor.copy_expert(copy_sql, CopyStream(row_iterator()))
                
                # 从临时表直接插入到目标表（不去重，最后创建主键时统一处理）
                cursor.execute(f"""
                    INSERT INTO {MAPPING_TABLE} (corpusid, title)
                    SELECT corpusid, title FROM temp_papers
                """)
                inserted_count = cursor.rowcount
                
                # 清空临时表供下次使用
                cursor.execute("TRUNCATE temp_papers")
                
                # 记录已导入的文件
                log_imported_file(cursor, gz_file.name)
                
                conn.commit()
                
                total_records += file_count
                total_inserted += inserted_count
                file_time = time.time() - file_start
                
                pbar.set_postfix_str(
                    f"{file_count:,}条/{file_time:.1f}秒 | 总计: {total_inserted:,}条"
                )
                
            except Exception as e:
                print(f"\n  ✗ 文件 {gz_file.name} 导入失败: {e}")
                conn.rollback()
                continue
    
    total_time = time.time() - start_time
    speed = total_records / total_time if total_time > 0 else 0
    
    print(f"\n【导入完成】")
    print(f"  读取记录: {total_records:,} 条")
    print(f"  插入记录: {total_inserted:,} 条")
    print(f"  总耗时: {total_time:.2f} 秒")
    print(f"  速度: {speed:.0f} 条/秒")
    print(f"  注意: 重复记录将在创建主键时统一删除")


def create_index(cursor, conn):
    """创建主键索引（导入完成后）"""
    print("\n【创建主键索引】")
    
    start_time = time.time()
    
    # 设置索引创建优化参数
    cursor.execute("SET maintenance_work_mem = '2GB'")
    
    # 检查主键是否存在
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM pg_constraint 
            WHERE conrelid = %s::regclass AND contype = 'p'
        )
    """, (MAPPING_TABLE,))
    
    if cursor.fetchone()[0]:
        print(f"  ✓ 主键已存在，跳过创建")
    else:
        print(f"  正在创建主键索引...")
        # 先删除重复记录（如果有）
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
            print(f"  删除重复记录: {dup_count:,} 条")
        
        # 创建主键
        cursor.execute(f"ALTER TABLE {MAPPING_TABLE} ADD PRIMARY KEY (corpusid)")
        elapsed = time.time() - start_time
        print(f"  ✓ 主键创建完成 ({elapsed:.2f}秒)")
    
    # 收集统计信息
    print(f"  收集统计信息...")
    cursor.execute(f"ANALYZE {MAPPING_TABLE}")
    conn.commit()


def run_pipeline(gz_directory, machine_id='machine0', skip_index=False):
    """执行完整的papers导入流程"""
    print("=" * 80)
    print("Papers数据导入流程（corpusid_mapping_title表）")
    print("=" * 80)
    print(f"  数据目录: {gz_directory}")
    print(f"  目标机器: {machine_id}")
    print("=" * 80)
    
    overall_start = time.time()
    
    # 连接数据库
    db_config = get_db_config(machine_id)
    print(f"\n连接到数据库 [{machine_id}: {db_config['database']}:{db_config['port']}]...")
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("  ✓ 连接成功")
    
    try:
        # 创建表
        create_tables(cursor)
        conn.commit()
        
        # 导入数据
        import_papers_gz(gz_directory, cursor, conn)
        
        # 创建索引（如果需要）
        if not skip_index:
            create_index(cursor, conn)
        
        # 总结
        total_time = time.time() - overall_start
        print("\n" + "=" * 80)
        print("【处理完成】")
        print("=" * 80)
        print(f"  总耗时: {total_time/60:.1f}分钟 ({total_time:.2f}秒)")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n✗ 处理失败: {e}")
        conn.rollback()
        raise
    
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="高性能导入papers数据到corpusid_mapping_title表",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 导入papers数据（默认machine0）
  python batch_update/import_papers_title.py D:\\gz_temp\\papers
  
  # 指定目标机器
  python batch_update/import_papers_title.py D:\\gz_temp\\papers --machine machine1
  
  # 跳过索引创建（如果已存在）
  python batch_update/import_papers_title.py D:\\gz_temp\\papers --skip-index

注意：
  - 支持断点续传，中断后重新运行会自动跳过已导入文件
  - 预计耗时：30-60分钟（60GB数据）
  - 此脚本应在import_citations.py之前运行
        """
    )
    
    parser.add_argument("gz_directory", help="包含papers gz文件的目录")
    parser.add_argument("--machine", default="machine0", help="目标机器 (默认: machine0)")
    parser.add_argument("--skip-index", action="store_true", help="跳过索引创建")
    
    args = parser.parse_args()
    
    gz_dir = Path(args.gz_directory)
    if not gz_dir.is_dir():
        print(f"错误: {args.gz_directory} 不是有效的目录")
        sys.exit(1)
    
    run_pipeline(args.gz_directory, args.machine, args.skip_index)

