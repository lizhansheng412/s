"""
高性能导入citations数据并更新temp_import表的citations和references字段

处理流程：
1. 扫描所有citations gz文件，导入到citation_raw映射表
2. 创建索引优化查询
3. 聚合references数据（citingcorpusid -> citedcorpusid列表）
4. 聚合citations数据（citedcorpusid -> citingcorpusid列表）
5. 批量更新temp_import表
6. 清理临时表

性能优化：
- 使用COPY命令批量导入
- 延迟创建索引
- SQL层面聚合（避免Python循环）
- 批量UPDATE
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

TEMP_TABLE = "temp_import"
CITATION_RAW_TABLE = "citation_raw"
RUNNING_LOG = Path(__file__).parent.parent / "logs" / "running.log"


def log_performance(stage, **metrics):
    """记录性能日志"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    metrics_str = " | ".join([f"{k}={v}" for k, v in metrics.items()])
    log_line = f"[{timestamp}] {stage} | {metrics_str}\n"
    with open(RUNNING_LOG, 'a', encoding='utf-8') as f:
        f.write(log_line)
    print(f"  {stage}: {metrics_str}")


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


def create_citation_raw_table(cursor, truncate=False):
    """创建citation_raw映射表（不创建索引）"""
    print("\n【阶段0】创建映射表...")
    
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {CITATION_RAW_TABLE} (
            citingcorpusid BIGINT NOT NULL,
            citedcorpusid BIGINT NOT NULL
        ) WITH (autovacuum_enabled = false)
    """)
    
    # 检查表是否为空
    cursor.execute(f"SELECT COUNT(*) FROM {CITATION_RAW_TABLE}")
    count = cursor.fetchone()[0]
    
    if count > 0:
        if truncate:
            print(f"  正在清空表（{count:,} 条记录）...")
            cursor.execute(f"TRUNCATE TABLE {CITATION_RAW_TABLE}")
            print("  ✓ 表已清空")
        else:
            print(f"  ⚠️  表已存在且有 {count:,} 条记录，将继续追加数据")
    else:
        print(f"  ✓ 表创建成功")


def import_citations_gz(gz_directory, cursor, conn):
    """阶段1：导入所有citations gz文件到citation_raw表"""
    print("\n【阶段1】导入citations数据...")
    
    gz_dir = Path(gz_directory)
    all_gz_files = sorted(gz_dir.glob("*.gz"))
    
    if not all_gz_files:
        raise FileNotFoundError(f"在 {gz_directory} 中没有找到gz文件")
    
    print(f"  发现 {len(all_gz_files)} 个gz文件")
    
    # 优化数据库配置
    cursor.execute("SET synchronous_commit = OFF")
    cursor.execute("SET work_mem = '512MB'")
    
    copy_sql = f"COPY {CITATION_RAW_TABLE} (citingcorpusid, citedcorpusid) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')"
    
    total_records = 0
    start_time = time.time()
    
    with tqdm(all_gz_files, desc="  导入进度", unit="file") as pbar:
        for gz_file in pbar:
            file_start = time.time()
            file_count = 0
            
            def row_iterator():
                nonlocal file_count
                with gzip.open(gz_file, 'rt', encoding='utf-8', errors='replace') as f:
                    for line in f:
                        try:
                            data = orjson.loads(line.strip())
                            citing = data.get('citingcorpusid')
                            cited = data.get('citedcorpusid')
                            
                            if citing is not None and cited is not None:
                                file_count += 1
                                yield f"{citing}\t{cited}\n".encode('utf-8')
                        except Exception:
                            continue
            
            try:
                cursor.copy_expert(copy_sql, CopyStream(row_iterator()))
                conn.commit()
                
                total_records += file_count
                file_time = time.time() - file_start
                
                pbar.set_postfix_str(
                    f"当前: {file_count:,}条/{file_time:.1f}秒 | "
                    f"总计: {total_records:,}条"
                )
                
            except Exception as e:
                print(f"\n  ✗ 文件 {gz_file.name} 导入失败: {e}")
                conn.rollback()
                continue
    
    total_time = time.time() - start_time
    speed = total_records / total_time if total_time > 0 else 0
    
    log_performance(
        "阶段1-导入完成",
        files=len(all_gz_files),
        records=f"{total_records:,}",
        time_sec=f"{total_time:.2f}",
        speed_per_sec=f"{speed:.0f}"
    )


def create_indexes(cursor, conn):
    """阶段2：创建索引"""
    print("\n【阶段2】创建索引...")
    
    start_time = time.time()
    
    # 设置索引创建优化参数
    cursor.execute("SET maintenance_work_mem = '2GB'")
    
    print("  创建citingcorpusid索引...")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_citation_citing ON {CITATION_RAW_TABLE} (citingcorpusid)")
    
    print("  创建citedcorpusid索引...")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_citation_cited ON {CITATION_RAW_TABLE} (citedcorpusid)")
    
    print("  收集统计信息...")
    cursor.execute(f"ANALYZE {CITATION_RAW_TABLE}")
    
    conn.commit()
    
    elapsed = time.time() - start_time
    log_performance("阶段2-索引创建", time_sec=f"{elapsed:.2f}")


def build_references(cursor, conn):
    """阶段3：构造references数据（citingcorpusid -> citedcorpusid列表）"""
    print("\n【阶段3】构造references数据...")
    
    start_time = time.time()
    
    # 设置查询优化参数
    cursor.execute("SET work_mem = '512MB'")
    cursor.execute("SET max_parallel_workers_per_gather = 4")
    
    print("  执行SQL聚合查询...")
    cursor.execute(f"""
        CREATE TEMP TABLE temp_references AS
        WITH citing_mapping AS (
            SELECT 
                citingcorpusid,
                array_agg(DISTINCT citedcorpusid) as cited_list
            FROM {CITATION_RAW_TABLE}
            GROUP BY citingcorpusid
        ),
        references_with_titles AS (
            SELECT 
                cm.citingcorpusid as corpusid,
                json_agg(
                    json_build_object(
                        'corpusid', cited_id,
                        'title', COALESCE(cmt.title, '')
                    )
                    ORDER BY cited_id
                ) as references_json
            FROM citing_mapping cm
            CROSS JOIN LATERAL unnest(cm.cited_list) as cited_id
            LEFT JOIN corpusid_mapping_title cmt ON cmt.corpusid = cited_id
            GROUP BY cm.citingcorpusid
        )
        SELECT 
            corpusid,
            references_json::TEXT as references
        FROM references_with_titles
    """)
    
    print("  创建索引...")
    cursor.execute("CREATE INDEX ON temp_references (corpusid)")
    
    # 统计记录数
    cursor.execute("SELECT COUNT(*) FROM temp_references")
    count = cursor.fetchone()[0]
    
    conn.commit()
    
    elapsed = time.time() - start_time
    log_performance("阶段3-references构造", records=f"{count:,}", time_sec=f"{elapsed:.2f}")


def build_citations(cursor, conn):
    """阶段4：构造citations数据（citedcorpusid -> citingcorpusid列表）"""
    print("\n【阶段4】构造citations数据...")
    
    start_time = time.time()
    
    print("  执行SQL聚合查询...")
    cursor.execute(f"""
        CREATE TEMP TABLE temp_citations AS
        WITH cited_mapping AS (
            SELECT 
                citedcorpusid,
                array_agg(DISTINCT citingcorpusid) as citing_list
            FROM {CITATION_RAW_TABLE}
            GROUP BY citedcorpusid
        ),
        citations_with_titles AS (
            SELECT 
                cm.citedcorpusid as corpusid,
                json_agg(
                    json_build_object(
                        'corpusid', citing_id,
                        'title', COALESCE(cmt.title, '')
                    )
                    ORDER BY citing_id
                ) as citations_json
            FROM cited_mapping cm
            CROSS JOIN LATERAL unnest(cm.citing_list) as citing_id
            LEFT JOIN corpusid_mapping_title cmt ON cmt.corpusid = citing_id
            GROUP BY cm.citedcorpusid
        )
        SELECT 
            corpusid,
            citations_json::TEXT as citations
        FROM citations_with_titles
    """)
    
    print("  创建索引...")
    cursor.execute("CREATE INDEX ON temp_citations (corpusid)")
    
    # 统计记录数
    cursor.execute("SELECT COUNT(*) FROM temp_citations")
    count = cursor.fetchone()[0]
    
    conn.commit()
    
    elapsed = time.time() - start_time
    log_performance("阶段4-citations构造", records=f"{count:,}", time_sec=f"{elapsed:.2f}")


def update_temp_import(cursor, conn):
    """阶段5：批量更新temp_import表"""
    print("\n【阶段5】更新temp_import表...")
    
    start_time = time.time()
    
    # 更新references字段
    print("  更新references字段...")
    cursor.execute(f"""
        UPDATE {TEMP_TABLE} ti
        SET references = tr.references
        FROM temp_references tr
        WHERE ti.corpusid = tr.corpusid
    """)
    ref_count = cursor.rowcount
    
    # 更新citations字段
    print("  更新citations字段...")
    cursor.execute(f"""
        UPDATE {TEMP_TABLE} ti
        SET citations = tc.citations
        FROM temp_citations tc
        WHERE ti.corpusid = tc.corpusid
    """)
    cite_count = cursor.rowcount
    
    # 填充没有引用关系的记录为空数组
    print("  填充空值...")
    cursor.execute(f"""
        UPDATE {TEMP_TABLE}
        SET 
            references = CASE 
                WHEN references IS NULL OR references = '' OR references = '{{}}' THEN '[]'
                ELSE references 
            END,
            citations = CASE 
                WHEN citations IS NULL OR citations = '' OR citations = '{{}}' THEN '[]'
                ELSE citations 
            END
        WHERE (references IS NULL OR references = '' OR references = '{{}}')
           OR (citations IS NULL OR citations = '' OR citations = '{{}}')
    """)
    
    conn.commit()
    
    elapsed = time.time() - start_time
    log_performance(
        "阶段5-更新完成",
        references_updated=f"{ref_count:,}",
        citations_updated=f"{cite_count:,}",
        time_sec=f"{elapsed:.2f}"
    )


def cleanup(cursor, conn, keep_citation_raw=False):
    """阶段6：清理临时表"""
    print("\n【阶段6】清理临时表...")
    
    cursor.execute("DROP TABLE IF EXISTS temp_references")
    cursor.execute("DROP TABLE IF EXISTS temp_citations")
    print("  ✓ 已删除temp_references和temp_citations")
    
    if not keep_citation_raw:
        cursor.execute(f"DROP TABLE IF EXISTS {CITATION_RAW_TABLE}")
        print(f"  ✓ 已删除{CITATION_RAW_TABLE}")
    else:
        print(f"  ⓘ 保留{CITATION_RAW_TABLE}表供后续使用")
    
    conn.commit()


def run_full_pipeline(gz_directory, machine_id='machine0', keep_citation_raw=False, truncate=False):
    """执行完整的citations处理流程"""
    print("=" * 80)
    print("Citations数据高性能处理流程")
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
        # 阶段0：创建表
        create_citation_raw_table(cursor, truncate)
        conn.commit()
        
        # 阶段1：导入数据
        import_citations_gz(gz_directory, cursor, conn)
        
        # 阶段2：创建索引
        create_indexes(cursor, conn)
        
        # 阶段3：构造references
        build_references(cursor, conn)
        
        # 阶段4：构造citations
        build_citations(cursor, conn)
        
        # 阶段5：更新temp_import
        update_temp_import(cursor, conn)
        
        # 阶段6：清理
        cleanup(cursor, conn, keep_citation_raw)
        
        # 总结
        total_time = time.time() - overall_start
        print("\n" + "=" * 80)
        print("【处理完成】")
        print("=" * 80)
        print(f"  总耗时: {total_time/60:.1f}分钟 ({total_time:.2f}秒)")
        print("=" * 80)
        
        log_performance("完整流程完成", total_time_min=f"{total_time/60:.1f}")
        
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
        description="高性能导入citations数据并更新temp_import表",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 处理citations数据（默认machine0）
  python batch_update/import_citations.py D:\\gz_temp\\citations
  
  # 清空表重新导入
  python batch_update/import_citations.py D:\\gz_temp\\citations --truncate
  
  # 保留citation_raw表供后续使用
  python batch_update/import_citations.py D:\\gz_temp\\citations --keep-raw

注意：
  - 整个流程预计耗时3-4小时（240GB数据）
  - 确保磁盘空间充足（需要约100GB临时空间）
  - 确保corpusid_mapping_title表已存在
        """
    )
    
    parser.add_argument("gz_directory", help="包含citations gz文件的目录")
    parser.add_argument("--machine", default="machine0", help="目标机器 (默认: machine0)")
    parser.add_argument("--keep-raw", action="store_true", help="保留citation_raw表不删除")
    parser.add_argument("--truncate", action="store_true", help="清空citation_raw表重新导入")
    
    args = parser.parse_args()
    
    gz_dir = Path(args.gz_directory)
    if not gz_dir.is_dir():
        print(f"错误: {args.gz_directory} 不是有效的目录")
        sys.exit(1)
    
    run_full_pipeline(args.gz_directory, args.machine, args.keep_raw, args.truncate)

