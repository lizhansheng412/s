"""
高性能导入citations数据并更新temp_import表的citations和references字段

处理流程：
1. 扫描所有citations gz文件，导入到citation_raw映射表
2. 创建索引优化查询
3. 聚合references数据并写入缓存表（citingcorpusid -> citedcorpusid列表）
4. 聚合citations数据并写入缓存表（citedcorpusid -> citingcorpusid列表）
5. 智能填充temp_import表（自动选择INSERT或UPDATE策略）
6. 清理临时表

性能优化：
- 使用COPY命令批量导入
- 延迟创建索引（先插入后建索引）
- SQL层面聚合（避免Python循环）
- 智能策略：空表用INSERT批量插入，有数据用UPDATE更新
- INSERT模式：分批写入并实时输出进度（可复用缓存，避免重复计算）
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


def create_citation_raw_table(cursor, truncate=False, no_count=False):
    """创建citation_raw映射表（不创建索引）"""
    print("\n【阶段0】创建映射表...")
    
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {CITATION_RAW_TABLE} (
            citingcorpusid BIGINT NOT NULL,
            citedcorpusid BIGINT NOT NULL
        ) WITH (autovacuum_enabled = false)
    """)
    
    # 快速存在性检查，避免对超大表做全表COUNT
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (CITATION_RAW_TABLE,))
    exists = cursor.fetchone()[0]
    if not exists:
        print(f"  ✓ 表创建成功")
        return

    if no_count:
        # 跳过COUNT(*)，直接给出提示
        if truncate:
            print(f"  正在清空表（跳过计数）...")
            cursor.execute(f"TRUNCATE TABLE {CITATION_RAW_TABLE}")
            print("  ✓ 表已清空")
        else:
            print(f"  ⓘ 表已存在（跳过计数），将继续后续阶段")
        return

    # 正常路径：做一次COUNT（表很大时可能较慢）
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
    """阶段2：创建索引（极致优化版）"""
    print("\n【阶段2】创建索引...")
    
    start_time = time.time()
    
    # 极致优化参数
    cursor.execute("SET maintenance_work_mem = '8GB'")  # 提升到8GB
    cursor.execute("SET max_parallel_maintenance_workers = 8")  # 并行构建
    cursor.execute("SET max_parallel_workers = 16")  # 全局并行上限
    
    # 检查索引是否已存在
    cursor.execute("""
        SELECT COUNT(*) FROM pg_indexes 
        WHERE tablename = %s AND indexname IN ('idx_citation_citing', 'idx_citation_cited')
    """, (CITATION_RAW_TABLE,))
    existing_count = cursor.fetchone()[0]
    
    if existing_count == 2:
        print("  ✓ 索引已存在，跳过创建")
    elif existing_count == 1:
        print("  ⚠️  仅有一个索引存在，补建缺失索引...")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_citation_citing ON {CITATION_RAW_TABLE} (citingcorpusid)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_citation_cited ON {CITATION_RAW_TABLE} (citedcorpusid)")
    else:
        print("  创建citingcorpusid索引（并行构建中）...")
        cursor.execute(f"CREATE INDEX idx_citation_citing ON {CITATION_RAW_TABLE} (citingcorpusid)")
        
        print("  创建citedcorpusid索引（并行构建中）...")
        cursor.execute(f"CREATE INDEX idx_citation_cited ON {CITATION_RAW_TABLE} (citedcorpusid)")
    
    print("  收集统计信息...")
    cursor.execute(f"ANALYZE {CITATION_RAW_TABLE}")
    
    conn.commit()
    
    elapsed = time.time() - start_time
    log_performance("阶段2-索引创建", time_sec=f"{elapsed:.2f}")


def build_references(cursor, conn, force=False):
    """阶段3：构造references数据（可复用缓存版）"""
    print("\n【阶段3】构造references数据...")
    
    cursor.execute("SELECT to_regclass('temp_references')")
    exists = cursor.fetchone()[0]
    if exists and not force:
        print("  ⓘ temp_references 已存在，跳过重建（使用 --force-stage3 可强制重建）")
        return
    
    start_time = time.time()
    
    # 🚀 终极极速：使用array_agg代替json_agg（快3-5倍）
    cursor.execute("SET max_parallel_workers_per_gather = 0")
    cursor.execute("SET work_mem = '8GB'")
    cursor.execute("SET temp_buffers = '2GB'")
    cursor.execute("SET hash_mem_multiplier = 2.0")
    cursor.execute("SET enable_hashagg = ON")
    print("  重建缓存表 temp_references ...")
    cursor.execute("DROP TABLE IF EXISTS temp_references")
    cursor.execute(f"""
        CREATE UNLOGGED TABLE temp_references AS
        SELECT 
            citingcorpusid AS corpusid,
            array_agg(citedcorpusid) AS ref_ids
        FROM {CITATION_RAW_TABLE}
        GROUP BY citingcorpusid
    """)
    
    print("  创建索引...")
    cursor.execute("SET maintenance_work_mem = '4GB'")
    cursor.execute("DROP INDEX IF EXISTS idx_temp_references_corpusid")
    cursor.execute("CREATE INDEX idx_temp_references_corpusid ON temp_references (corpusid)")
    
    cursor.execute("""
        SELECT reltuples::bigint 
        FROM pg_class 
        WHERE relname = 'temp_references'
    """)
    count = cursor.fetchone()[0]
    
    conn.commit()
    
    elapsed = time.time() - start_time
    log_performance("阶段3-references构造", records=f"{count:,}", time_sec=f"{elapsed:.2f}")


def build_citations(cursor, conn, force=False):
    """阶段4：构造citations数据（可复用缓存版）"""
    print("\n【阶段4】构造citations数据...")
    
    cursor.execute("SELECT to_regclass('temp_citations')")
    exists = cursor.fetchone()[0]
    if exists and not force:
        print("  ⓘ temp_citations 已存在，跳过重建（使用 --force-stage4 可强制重建）")
        return
    
    start_time = time.time()
    
    print("  重建缓存表 temp_citations ...")
    cursor.execute("DROP TABLE IF EXISTS temp_citations")
    cursor.execute(f"""
        CREATE UNLOGGED TABLE temp_citations AS
        SELECT 
            citedcorpusid AS corpusid,
            array_agg(citingcorpusid) AS cite_ids
        FROM {CITATION_RAW_TABLE}
        GROUP BY citedcorpusid
    """)
    
    print("  创建索引...")
    cursor.execute("DROP INDEX IF EXISTS idx_temp_citations_corpusid")
    cursor.execute("CREATE INDEX idx_temp_citations_corpusid ON temp_citations (corpusid)")
    
    cursor.execute("""
        SELECT reltuples::bigint 
        FROM pg_class 
        WHERE relname = 'temp_citations'
    """)
    count = cursor.fetchone()[0]
    
    conn.commit()
    
    elapsed = time.time() - start_time
    log_performance("阶段4-citations构造", records=f"{count:,}", time_sec=f"{elapsed:.2f}")


def update_temp_import(cursor, conn):
    """阶段5：智能填充temp_import表（自动选择INSERT或UPDATE策略）"""
    print("\n【阶段5】填充temp_import表...")
    
    start_time = time.time()
    
    # 确保前置缓存存在
    for table_name in ("temp_references", "temp_citations"):
        cursor.execute("SELECT to_regclass(%s)", (table_name,))
        if not cursor.fetchone()[0]:
            raise RuntimeError(
                f"{table_name} 不存在。请先执行阶段{3 if table_name == 'temp_references' else 4} "
                "（或使用 --force-stage3/--force-stage4 重建）再执行阶段5。"
            )
    
    # 检查temp_import表是否为空
    cursor.execute(f"SELECT COUNT(*) FROM {TEMP_TABLE}")
    existing_count = cursor.fetchone()[0]
    
    cursor.execute("SET work_mem = '8GB'")
    cursor.execute(f"ALTER TABLE {TEMP_TABLE} SET (autovacuum_enabled = false)")
    
    # 步骤1: 创建title映射缓存
    print("  步骤1: 创建title映射缓存...")
    step1_start = time.time()
    cursor.execute(f"""
        CREATE TEMP TABLE temp_title_cache AS
        SELECT 
            corpusid,
            COALESCE(title, '') as title
        FROM corpusid_mapping_title
        WHERE corpusid IN (
            SELECT DISTINCT citedcorpusid FROM {CITATION_RAW_TABLE}
            UNION
            SELECT DISTINCT citingcorpusid FROM {CITATION_RAW_TABLE}
        )
    """)
    cursor.execute("CREATE INDEX ON temp_title_cache (corpusid)")
    print(f"    ✓ 完成（{time.time() - step1_start:.1f}秒）")
    
    if existing_count == 0:
        # 表为空：使用分批INSERT批量填充
        print(f"\n  ⓘ temp_import表为空，使用分批INSERT模式（超大规模安全策略）")
        
        # 步骤2: 分批聚合并插入citations/references数据
        print("  步骤2: 分批聚合并插入citations/references数据...")
        step2_start = time.time()
        
        # 优化数据库配置
        cursor.execute("SET synchronous_commit = OFF")
        cursor.execute("SET work_mem = '8GB'")
        
        # 构建全量corpusid集合
        print("    2.1: 准备corpusid集合（去重）...")
        cursor.execute(f"""
            CREATE TEMP TABLE temp_all_ids AS
            SELECT DISTINCT citingcorpusid AS corpusid FROM {CITATION_RAW_TABLE}
            UNION
            SELECT DISTINCT citedcorpusid AS corpusid FROM {CITATION_RAW_TABLE}
        """)
        cursor.execute("CREATE INDEX ON temp_all_ids (corpusid)")
        cursor.execute("SELECT COUNT(*) FROM temp_all_ids")
        total_ids = cursor.fetchone()[0]
        print(f"        ✓ corpusid 总数: {total_ids:,}")
        
        batch_size = 50000
        batch_cursor = conn.cursor(name="temp_all_ids_cursor")
        batch_cursor.itersize = batch_size
        batch_cursor.execute("SELECT corpusid FROM temp_all_ids ORDER BY corpusid")
        
        total_inserted = 0
        batch_num = 0
        while True:
            rows = batch_cursor.fetchmany(batch_size)
            if not rows:
                break
            corpus_ids = [row[0] for row in rows]
            batch_num += 1
            batch_start = time.time()
            cursor.execute(f"""
                INSERT INTO {TEMP_TABLE} (corpusid, "references", "citations", is_done)
                SELECT 
                    ids.corpusid,
                    COALESCE(
                        (SELECT json_agg(
                            json_build_object(
                                'corpusid', ref_id,
                                'title', COALESCE(tc.title, '')
                            )
                        )::TEXT
                        FROM unnest(tr.ref_ids) AS ref_id
                        LEFT JOIN temp_title_cache tc ON tc.corpusid = ref_id),
                        '[]'
                    ) AS references,
                    COALESCE(
                        (SELECT json_agg(
                            json_build_object(
                                'corpusid', cite_id,
                                'title', COALESCE(tc2.title, '')
                            )
                        )::TEXT
                        FROM unnest(tcite.cite_ids) AS cite_id
                        LEFT JOIN temp_title_cache tc2 ON tc2.corpusid = cite_id),
                        '[]'
                    ) AS citations,
                    FALSE AS is_done
                FROM unnest(%s::bigint[]) AS ids(corpusid)
                LEFT JOIN temp_references tr ON tr.corpusid = ids.corpusid
                LEFT JOIN temp_citations tcite ON tcite.corpusid = ids.corpusid
            """, (corpus_ids,))
            inserted = len(corpus_ids)
            total_inserted += inserted
            batch_time = time.time() - batch_start
            speed = inserted / batch_time if batch_time > 0 else 0
            print(f"        批次{batch_num:>4}: 插入{inserted:,}条 | 累计{total_inserted:,}条 | {batch_time:.1f}秒 | {speed:,.0f}条/秒")
        
        batch_cursor.close()
        cursor.execute("DROP TABLE temp_all_ids")
        print(f"    ✓ 步骤2完成（{time.time() - step2_start:.1f}秒，插入{total_inserted:,}条）")
        
        # 步骤3: 一次性构建corpusid索引
        print("  步骤3: 构建corpusid索引...")
        step3_start = time.time()
        cursor.execute("""
            SELECT COUNT(*) FROM pg_indexes 
            WHERE tablename = %s AND indexname = 'idx_temp_import_corpusid'
        """, (TEMP_TABLE,))
        
        if cursor.fetchone()[0] == 0:
            cursor.execute("SET maintenance_work_mem = '8GB'")
            cursor.execute("SET max_parallel_maintenance_workers = 8")
            cursor.execute(f"CREATE INDEX idx_temp_import_corpusid ON {TEMP_TABLE} (corpusid)")
            print(f"    ✓ 索引创建完成（{time.time() - step3_start:.1f}秒）")
        else:
            print("    ✓ 索引已存在，跳过创建")
        
        log_performance(
            "阶段5-分批插入完成",
            mode="BATCH_INSERT",
            batches=f"{batch_num}",
            total_inserted=f"{total_inserted:,}",
            time_sec=f"{time.time() - start_time:.2f}"
        )
    else:
        # 表有数据：使用UPDATE更新模式
        print(f"\n  ⓘ temp_import表已有{existing_count:,}条数据，使用UPDATE更新模式")
        
        # 步骤2: 更新references字段
        print("  步骤2: 更新references字段（array→JSON+title）...")
        step2_start = time.time()
        cursor.execute(f"""
            UPDATE {TEMP_TABLE} ti
            SET "references" = (
                SELECT json_agg(
                    json_build_object(
                        'corpusid', ref_id,
                        'title', COALESCE(tc.title, '')
                    )
                )::TEXT
                FROM unnest(tr.ref_ids) AS ref_id
                LEFT JOIN temp_title_cache tc ON tc.corpusid = ref_id
            )
            FROM temp_references tr
            WHERE ti.corpusid = tr.corpusid
        """)
        ref_count = cursor.rowcount
        print(f"    ✓ 完成（{time.time() - step2_start:.1f}秒，更新{ref_count:,}条）")
        
        # 步骤3: 更新citations字段
        print("  步骤3: 更新citations字段（array→JSON+title）...")
        step3_start = time.time()
        cursor.execute(f"""
            UPDATE {TEMP_TABLE} ti
            SET "citations" = (
                SELECT json_agg(
                    json_build_object(
                        'corpusid', cite_id,
                        'title', COALESCE(tc.title, '')
                    )
                )::TEXT
                FROM unnest(tcite.cite_ids) AS cite_id
                LEFT JOIN temp_title_cache tc ON tc.corpusid = cite_id
            )
            FROM temp_citations tcite
            WHERE ti.corpusid = tcite.corpusid
        """)
        cite_count = cursor.rowcount
        print(f"    ✓ 完成（{time.time() - step3_start:.1f}秒，更新{cite_count:,}条）")
        
        # 填充空值
        print("  填充空值...")
        cursor.execute(f"""
            UPDATE {TEMP_TABLE}
            SET 
                "references" = COALESCE("references", '[]'),
                "citations" = COALESCE("citations", '[]')
            WHERE "references" IS NULL OR "citations" IS NULL
        """)
        
        log_performance(
            "阶段5-更新完成",
            mode="UPDATE",
            references_updated=f"{ref_count:,}",
            citations_updated=f"{cite_count:,}",
            time_sec=f"{time.time() - start_time:.2f}"
        )
    
    # 清理title缓存
    cursor.execute("DROP TABLE temp_title_cache")
    
    cursor.execute(f"ALTER TABLE {TEMP_TABLE} SET (autovacuum_enabled = true)")
    
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"\n  总耗时: {elapsed:.2f}秒")


def cleanup(cursor, conn, keep_citation_raw=True):
    """阶段6：清理提示（不自动删除任何表，由用户手动清理）"""
    print("\n【阶段6】清理提示...")
    
    # 检查各表是否存在及记录数
    print("\n  当前表状态：")
    
    # 检查temp_references
    try:
        cursor.execute("SELECT COUNT(*) FROM temp_references")
        count = cursor.fetchone()[0]
        print(f"  - temp_references: {count:,} 条（缓存表，可复用）")
    except:
        print(f"  - temp_references: 不存在")
    
    # 检查temp_citations
    try:
        cursor.execute("SELECT COUNT(*) FROM temp_citations")
        count = cursor.fetchone()[0]
        print(f"  - temp_citations: {count:,} 条（缓存表，可复用）")
    except:
        print(f"  - temp_citations: 不存在")
    
    # 检查citation_raw
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {CITATION_RAW_TABLE}")
        count = cursor.fetchone()[0]
        print(f"  - {CITATION_RAW_TABLE}: {count:,} 条（重要数据）")
    except:
        print(f"  - {CITATION_RAW_TABLE}: 不存在")
    
    print("\n  ⓘ 所有表已保留，如需手动清理，请执行：")
    print(f"     -- 清理缓存表（需要时手动执行）")
    print(f"     DROP TABLE IF EXISTS temp_references;")
    print(f"     DROP TABLE IF EXISTS temp_citations;")
    print(f"     DROP TABLE IF EXISTS temp_title_cache;")
    print(f"")
    print(f"     -- 清理citation_raw（谨慎！14亿+条数据，需1-2小时重建）")
    print(f"     DROP TABLE IF EXISTS {CITATION_RAW_TABLE};")
    
    conn.commit()


def run_full_pipeline(
    gz_directory,
    machine_id='machine0',
    keep_citation_raw=True,
    truncate=False,
    skip_import=False,
    skip_index=False,
    skip_stage3=False,
    skip_stage4=False,
    skip_stage5=False,
    only_import=False,
    only_index=False,
    only_stage3=False,
    only_stage4=False,
    only_stage5=False,
    force_stage3=False,
    force_stage4=False,
    no_count=False,
    skip_cleanup=False,
):
    """执行完整的citations处理流程
    
    参数:
        gz_directory: citations gz文件目录
        machine_id: 目标数据库 (默认: machine0)
        keep_citation_raw: 保留citation_raw表 (默认: True，防止误删)
        truncate: 清空citation_raw重新导入 (默认: False)
        skip_import: 跳过导入阶段 (默认: False)
        skip_index: 跳过建索引阶段 (默认: False)
        skip_stage3: 跳过阶段3缓存构建 (默认: False)
        skip_stage4: 跳过阶段4缓存构建 (默认: False)
        skip_stage5: 跳过阶段5更新 (默认: False)
        only_import: 仅执行阶段0-1（导入），然后停止 (默认: False)
        only_index: 仅执行阶段2（建索引），然后停止 (默认: False)
        only_stage3: 仅执行阶段3（构造references缓存） (默认: False)
        only_stage4: 仅执行阶段4（构造citations缓存） (默认: False)
        only_stage5: 仅执行阶段5（填充temp_import表） (默认: False)
        force_stage3: 强制重建阶段3缓存（忽略已有缓存） (默认: False)
        force_stage4: 强制重建阶段4缓存（忽略已有缓存） (默认: False)
        no_count: 跳过表计数 (默认: False)
        skip_cleanup: 跳过阶段6清理提示 (默认: False)
    """
    print("=" * 80)
    print("Citations数据高性能处理流程")
    print("=" * 80)
    print(f"  数据目录: {gz_directory}")
    print(f"  目标机器: {machine_id}")
    print(f"  保留citation_raw: {'是' if keep_citation_raw else '否（谨慎！）'}")
    print("=" * 80)
    
    overall_start = time.time()
    
    # 连接数据库
    db_config = get_db_config(machine_id)
    print(f"\n连接到数据库 [{machine_id}: {db_config['database']}:{db_config['port']}]...")
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("  ✓ 连接成功")
    
    try:
        only_flags = [only_import, only_index, only_stage3, only_stage4, only_stage5]
        if sum(1 for flag in only_flags if flag) > 1:
            raise ValueError("仅执行模式参数（--only-*）只能指定一个")
        
        # 如果仅执行单独阶段，优先处理
        if only_stage3:
            print("\n【仅阶段3模式】执行references缓存构建...")
            build_references(cursor, conn, force=force_stage3)
            conn.commit()
            print("\n" + "=" * 80)
            print("【仅阶段3模式】完成")
            print("=" * 80)
            return
        
        if only_stage4:
            print("\n【仅阶段4模式】执行citations缓存构建...")
            build_citations(cursor, conn, force=force_stage4)
            conn.commit()
            print("\n" + "=" * 80)
            print("【仅阶段4模式】完成")
            print("=" * 80)
            return
        
        if only_stage5:
            print("\n【仅阶段5模式】直接更新temp_import表...")
            update_temp_import(cursor, conn)
            conn.commit()
            if not skip_cleanup:
                cleanup(cursor, conn, keep_citation_raw)
            print("\n" + "=" * 80)
            print("【仅阶段5模式】完成")
            print("=" * 80)
            return
        
        # 如果只建索引，直接跳到阶段2
        if only_index:
            print("\n【仅建索引模式】跳过阶段0和1，直接执行阶段2...")
            create_indexes(cursor, conn)
            print("\n" + "=" * 80)
            print("【仅建索引模式】阶段2完成，停止执行")
            print("=" * 80)
            print("  下一步请执行：")
            print("  python batch_update/import_citations.py <目录> --skip-import --skip-index")
            print("=" * 80)
            return
        
        # 如果跳过导入和索引，直接跳到阶段3
        if skip_import and skip_index:
            print("\n【聚合更新模式】跳过阶段0、1、2，直接执行阶段3-5...")
        else:
            # 阶段0：创建表
            create_citation_raw_table(cursor, truncate, no_count)
            conn.commit()
            
            # 阶段1：导入数据（可跳过）
            if skip_import:
                print("\n【阶段1】导入citations数据...（已跳过，按 --skip-import 指定）")
            else:
                import_citations_gz(gz_directory, cursor, conn)
            
            # 如果只导入，到此结束
            if only_import:
                print("\n" + "=" * 80)
                print("【仅导入模式】阶段1完成，停止执行")
                print("=" * 80)
                print("  下一步请执行：")
                print("  python batch_update/import_citations.py <目录> --only-index")
                print("=" * 80)
                return
            
            # 阶段2：创建索引（可跳过）
            if skip_index:
                print("\n【阶段2】创建索引...（已跳过，按 --skip-index 指定）")
            else:
                create_indexes(cursor, conn)
        
        # 阶段3：构造references
        if skip_stage3:
            print("\n【阶段3】构造references数据...（已跳过，按 --skip-stage3 指定）")
        else:
            build_references(cursor, conn, force=force_stage3)
        
        # 阶段4：构造citations
        if skip_stage4:
            print("\n【阶段4】构造citations数据...（已跳过，按 --skip-stage4 指定）")
        else:
            build_citations(cursor, conn, force=force_stage4)
        
        # 阶段5：更新temp_import
        if skip_stage5:
            print("\n【阶段5】填充temp_import表...（已跳过，按 --skip-stage5 指定）")
        else:
            update_temp_import(cursor, conn)
        
        # 阶段6：清理
        if not skip_cleanup:
            cleanup(cursor, conn, keep_citation_raw)
        else:
            print("\n【阶段6】清理提示...（已跳过，按 --skip-cleanup 指定）")
        
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
        description="高性能导入citations数据并更新temp_import表（默认保留citation_raw，防止误删）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 完整流程（默认machine0，自动保留citation_raw）
  python batch_update/import_citations.py D:\\gz_temp\\citations
  
  # 分步执行（推荐）：
  # 步骤1：仅导入数据
  python batch_update/import_citations.py D:\\gz_temp\\citations --only-import
  
  # 步骤2：仅建索引
  python batch_update/import_citations.py D:\\gz_temp\\citations --only-index
  
  # 步骤3：构造缓存
  python batch_update/import_citations.py D:\\gz_temp\\citations --only-stage3
  python batch_update/import_citations.py D:\\gz_temp\\citations --only-stage4
  
  # 步骤4：执行聚合和更新（阶段3-5，可跳过已完成的阶段）
  python batch_update/import_citations.py D:\\gz_temp\\citations --skip-import --skip-index --skip-stage3 --skip-stage4
  
  # 跳过导入，直接更新（索引已存在时使用）
  python batch_update/import_citations.py D:\\gz_temp\\citations --skip-import --no-count
  
  # 指定其他机器
  python batch_update/import_citations.py D:\\gz_temp\\citations --machine machine2
  
  # 清空表重新导入
  python batch_update/import_citations.py D:\\gz_temp\\citations --truncate

注意：
  - 默认使用machine0数据库，请用--machine指定正确的数据库
  - citation_raw表默认保留（防止误删），不再支持自动删除
  - 整个流程预计耗时3-4小时（240GB数据）
  - 确保corpusid_mapping_title表已存在
  - 确保temp_import表有数据（否则更新0条）
  - 建议分步执行：先导入、再建索引、构建缓存，最后聚合更新
        """
    )
    
    parser.add_argument("gz_directory", help="包含citations gz文件的目录")
    parser.add_argument("--machine", default="machine0", help="目标机器 (默认: machine0)")
    parser.add_argument("--truncate", action="store_true", help="清空citation_raw表重新导入")
    parser.add_argument("--skip-import", action="store_true", help="跳过阶段1导入")
    parser.add_argument("--skip-index", action="store_true", help="跳过阶段2建索引")
    parser.add_argument("--skip-stage3", action="store_true", help="跳过阶段3（构造references缓存）")
    parser.add_argument("--skip-stage4", action="store_true", help="跳过阶段4（构造citations缓存）")
    parser.add_argument("--skip-stage5", action="store_true", help="跳过阶段5（填充temp_import）")
    parser.add_argument("--only-import", action="store_true", help="仅执行阶段0-1（导入），然后停止")
    parser.add_argument("--only-index", action="store_true", help="仅执行阶段2（建索引），然后停止")
    parser.add_argument("--only-stage3", action="store_true", help="仅执行阶段3（构造references缓存）")
    parser.add_argument("--only-stage4", action="store_true", help="仅执行阶段4（构造citations缓存）")
    parser.add_argument("--only-stage5", action="store_true", help="仅执行阶段5（填充temp_import表）")
    parser.add_argument("--force-stage3", action="store_true", help="强制重建阶段3缓存（忽略已有缓存）")
    parser.add_argument("--force-stage4", action="store_true", help="强制重建阶段4缓存（忽略已有缓存）")
    parser.add_argument("--no-count", action="store_true", help="阶段0跳过COUNT(*)，避免在超大表上卡住")
    parser.add_argument("--skip-cleanup", action="store_true", help="跳过阶段6清理提示输出")
    parser.add_argument("--keep-raw", action="store_true", help="（已废弃：现在默认保留citation_raw）")
    
    args = parser.parse_args()
    
    gz_dir = Path(args.gz_directory)
    if not gz_dir.is_dir():
        print(f"错误: {args.gz_directory} 不是有效的目录")
        sys.exit(1)
    
    run_full_pipeline(
        args.gz_directory,
        args.machine,
        args.keep_raw,
        args.truncate,
        args.skip_import,
        args.skip_index,
        args.skip_stage3,
        args.skip_stage4,
        args.skip_stage5,
        args.only_import,
        args.only_index,
        args.only_stage3,
        args.only_stage4,
        args.only_stage5,
        args.force_stage3,
        args.force_stage4,
        args.no_count,
        args.skip_cleanup,
    )

