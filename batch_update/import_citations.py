"""
高性能导入citations数据并更新temp_import表

核心流程（6个独立阶段）：
【阶段0】创建citation_raw表
【阶段1】导入gz文件到citation_raw表（citingcorpusid, citedcorpusid）
【阶段2】创建索引（idx_citation_citing, idx_citation_cited）
【阶段3】构造references缓存（temp_references: corpusid -> array[citedcorpusid]）
【阶段4】构造citations缓存（temp_citations: corpusid -> array[citingcorpusid]）
【阶段5】填充temp_import表（从缓存表读取数据，更新citations/references字段）
【阶段6】清理提示

每个阶段都可独立执行，通过命令行参数控制：
- --only-import:  仅执行阶段0-1
- --only-index:   仅执行阶段2
- --only-stage3:  仅执行阶段3
- --only-stage4:  仅执行阶段4
- --only-stage5:  仅执行阶段5
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
    """阶段3：构造references缓存（citingcorpusid -> array[citedcorpusid]）"""
    print("\n【阶段3】构造references缓存...")
    
    # 检查是否已存在
    cursor.execute("SELECT to_regclass('temp_references')")
    exists = cursor.fetchone()[0]
    if exists and not force:
        cursor.execute("SELECT COUNT(*) FROM temp_references")
        count = cursor.fetchone()[0]
        print(f"  ⓘ temp_references 已存在（{count:,}条），跳过重建")
        print(f"     使用 --force-stage3 可强制重建")
        return
    
    start_time = time.time()
    
    # 优化数据库配置
    print("  优化数据库配置...")
    cursor.execute("SET work_mem = '8GB'")
    cursor.execute("SET temp_buffers = '2GB'")
    cursor.execute("SET hash_mem_multiplier = 2.0")
    
    # 构建缓存表（使用array_agg，比json_agg快3-5倍）
    print("  聚合数据（citingcorpusid -> array[citedcorpusid]）...")
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
    print("  创建索引...")
    cursor.execute("SET maintenance_work_mem = '4GB'")
    cursor.execute("SET max_parallel_maintenance_workers = 8")
    cursor.execute("CREATE INDEX idx_temp_references_corpusid ON temp_references (corpusid)")
    
    # 统计结果
    cursor.execute("SELECT COUNT(*) FROM temp_references")
    count = cursor.fetchone()[0]
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"\n  ✓ 完成：{count:,} 条记录，耗时 {elapsed:.2f}秒")
    log_performance("阶段3-references构造", records=f"{count:,}", time_sec=f"{elapsed:.2f}")


def build_citations(cursor, conn, force=False):
    """阶段4：构造citations缓存（citedcorpusid -> array[citingcorpusid]）"""
    print("\n【阶段4】构造citations缓存...")
    
    # 检查是否已存在
    cursor.execute("SELECT to_regclass('temp_citations')")
    exists = cursor.fetchone()[0]
    if exists and not force:
        cursor.execute("SELECT COUNT(*) FROM temp_citations")
        count = cursor.fetchone()[0]
        print(f"  ⓘ temp_citations 已存在（{count:,}条），跳过重建")
        print(f"     使用 --force-stage4 可强制重建")
        return
    
    start_time = time.time()
    
    # 优化数据库配置
    print("  优化数据库配置...")
    cursor.execute("SET work_mem = '8GB'")
    cursor.execute("SET temp_buffers = '2GB'")
    cursor.execute("SET hash_mem_multiplier = 2.0")
    
    # 构建缓存表
    print("  聚合数据（citedcorpusid -> array[citingcorpusid]）...")
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
    print("  创建索引...")
    cursor.execute("SET maintenance_work_mem = '4GB'")
    cursor.execute("SET max_parallel_maintenance_workers = 8")
    cursor.execute("CREATE INDEX idx_temp_citations_corpusid ON temp_citations (corpusid)")
    
    # 统计结果
    cursor.execute("SELECT COUNT(*) FROM temp_citations")
    count = cursor.fetchone()[0]
    conn.commit()
    
    elapsed = time.time() - start_time
    print(f"\n  ✓ 完成：{count:,} 条记录，耗时 {elapsed:.2f}秒")
    log_performance("阶段4-citations构造", records=f"{count:,}", time_sec=f"{elapsed:.2f}")


def update_temp_import(cursor, conn):
    """阶段5：填充temp_import表（临时表聚合后分批INSERT）
    
    策略：
    1. 在临时表中聚合所有数据（带title的JSON格式，SQL层面完成）
    2. 分批INSERT到temp_import表（实时显示进度）
    
    优点：
    - 极速：SQL层面聚合，避免Python循环
    - 进度可见：分批插入，实时显示进度
    - 简洁：不做去重检查，直接插入
    """
    print("\n【阶段5】填充temp_import表...")
    start_time = time.time()
    
    # 步骤1: 检查前置条件
    print("  步骤1: 检查前置条件...")
    
    # 检查缓存表是否存在
    for table_name in ("temp_references", "temp_citations"):
        cursor.execute("SELECT to_regclass(%s)", (table_name,))
        if not cursor.fetchone()[0]:
            raise RuntimeError(
                f"❌ {table_name} 不存在。\n"
                f"   请先执行: python batch_update/import_citations.py <目录> "
                f"--only-stage{3 if table_name == 'temp_references' else 4}"
            )
    
    # 使用 pg_class 的统计信息估算行数，避免在超大表上执行 COUNT(*)
    cursor.execute(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = 'temp_references'::regclass"
    )
    ref_estimate = cursor.fetchone()[0]
    cursor.execute(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = 'temp_citations'::regclass"
    )
    cite_estimate = cursor.fetchone()[0]
    
    ref_msg = f"约 {int(ref_estimate):,} 条" if ref_estimate is not None else "未知"
    cite_msg = f"约 {int(cite_estimate):,} 条" if cite_estimate is not None else "未知"
    
    print(f"    ✓ temp_references: {ref_msg}（估算值）")
    print(f"    ✓ temp_citations: {cite_msg}（估算值）")
    
    # 步骤2: 优化数据库配置（充分利用32GB内存和8核CPU）
    print("\n  步骤2: 优化数据库配置（充分利用32GB内存和8核CPU）...")
    cursor.execute("SET work_mem = '2GB'")  # 单个操作2GB（大规模UNION去重需要）
    cursor.execute("SET temp_buffers = '2GB'")  # 临时缓冲区2GB
    cursor.execute("SET maintenance_work_mem = '4GB'")  # 维护操作4GB
    cursor.execute("SET synchronous_commit = OFF")  # 异步提交，提升10倍写入速度
    cursor.execute("SET statement_timeout = 0")  # 禁用超时（大数据UNION操作需要长时间）
    cursor.execute("SET max_parallel_workers_per_gather = 4")  # 并行查询：4个worker
    cursor.execute("SET parallel_tuple_cost = 0.01")  # 降低并行成本估算，鼓励并行
    cursor.execute("SET enable_hashagg = ON")  # 启用HashAgg
    cursor.execute("SET hash_mem_multiplier = 3.0")  # 哈希聚合可用3倍work_mem（6GB）
    cursor.execute("SET effective_cache_size = '16GB'")  # 告诉优化器可用缓存大小
    print("    ✓ 完成（高性能模式：2GB work_mem + 6GB hash_mem + 禁用超时）")
    
    # 步骤3: 创建title映射缓存（直接JOIN，无需预先提取）
    print("\n  步骤3: 创建title映射缓存...")
    step3_start = time.time()
    
    # 直接从corpusid_mapping_title复制所有数据（无过滤，极速）
    cursor.execute("DROP TABLE IF EXISTS temp_title_cache")
    cursor.execute("""
        CREATE TEMP TABLE temp_title_cache AS
        SELECT corpusid, COALESCE(title, '') as title
        FROM corpusid_mapping_title
    """)
    
    # 创建索引用于后续JOIN
    cursor.execute("CREATE INDEX ON temp_title_cache (corpusid)")
    
    cursor.execute("SELECT reltuples::bigint FROM pg_class WHERE oid = 'temp_title_cache'::regclass")
    cache_count = cursor.fetchone()[0]
    print(f"    ✓ 缓存约{int(cache_count):,}条title（{time.time() - step3_start:.1f}秒）")
    
    # 步骤4: 创建去重的corpusid列表（使用UNION自动去重，比UNION ALL+GROUP BY快）
    print("\n  步骤4: 创建corpusid去重列表（使用UNION自动去重）...")
    step4_start = time.time()
    
    cursor.execute("DROP TABLE IF EXISTS temp_unique_corpusids")
    cursor.execute("""
        CREATE UNLOGGED TABLE temp_unique_corpusids AS
        SELECT corpusid FROM temp_references
        UNION
        SELECT corpusid FROM temp_citations
    """)
    
    # 创建索引加速后续批量读取
    cursor.execute("CREATE INDEX idx_unique_corpusids ON temp_unique_corpusids (corpusid)")
    cursor.execute("ANALYZE temp_unique_corpusids")
    
    cursor.execute("SELECT COUNT(*) FROM temp_unique_corpusids")
    unique_count = cursor.fetchone()[0]
    print(f"    ✓ 去重完成: {unique_count:,} 个不重复corpusid（{time.time() - step4_start:.1f}秒）")
    
    # 步骤5: 临时表JOIN + COPY批量插入（极速优化，充分利用32GB内存）
    print("\n  步骤5: 分批COPY到temp_import表（临时表JOIN优化）...")
    step5_start = time.time()
    
    batch_size = 50000  # 大批次：5万条/批（充分利用内存）
    total_inserted = 0
    batch_num = 0
    commit_interval = 5  # 每5批提交一次
    failed_batches = []
    
    # 一次性读取所有corpusid到内存（6700万*8字节=536MB，内存充足）
    print("    加载corpusid列表到内存...")
    load_start = time.time()
    cursor.execute("SELECT corpusid FROM temp_unique_corpusids ORDER BY corpusid")
    all_corpusids = cursor.fetchall()
    print(f"    ✓ 已加载 {len(all_corpusids):,} 条corpusid（{time.time() - load_start:.1f}秒）")
    
    print(f"\n    配置: {batch_size:,}条/批 | 每{commit_interval}批提交 | IN查询+分批title | 内存安全")
    
    # 准备COPY命令
    copy_sql = f"COPY {TEMP_TABLE} (corpusid, \"references\", \"citations\", is_done) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')"
    
    # 分批处理
    for i in range(0, len(all_corpusids), batch_size):
        corpusids = all_corpusids[i:i+batch_size]
        batch_num += 1
        batch_start = time.time()
        
        try:
            query_start = time.time()
            
            # 1. 构造IN子句（对5万条ID，IN比临时表JOIN更快）
            id_list = ','.join(str(cid) for cid, in corpusids)
            
            # 2. 查询references（使用IN，避免临时表overhead）
            cursor.execute(f"""
                SELECT corpusid, ref_ids
                FROM temp_references
                WHERE corpusid IN ({id_list})
            """)
            ref_batch = {row[0]: row[1] for row in cursor.fetchall()}
            
            # 3. 查询citations
            cursor.execute(f"""
                SELECT corpusid, cite_ids
                FROM temp_citations
                WHERE corpusid IN ({id_list})
            """)
            cite_batch = {row[0]: row[1] for row in cursor.fetchall()}
            
            # 4. 收集所有需要查询title的corpusid
            all_ids = set()
            for ref_ids in ref_batch.values():
                all_ids.update(ref_ids)
            for cite_ids in cite_batch.values():
                all_ids.update(cite_ids)
            
            # 5. 批量查询title（使用IN）
            if all_ids:
                # 限制title查询批次大小（避免IN子句过大）
                title_map = {}
                title_ids_list = list(all_ids)
                title_batch_size = 100000  # 每次查询10万个title
                
                for j in range(0, len(title_ids_list), title_batch_size):
                    title_ids_batch = title_ids_list[j:j+title_batch_size]
                    title_id_str = ','.join(str(tid) for tid in title_ids_batch)
                    cursor.execute(f"""
                        SELECT corpusid, title
                        FROM temp_title_cache
                        WHERE corpusid IN ({title_id_str})
                    """)
                    title_map.update({row[0]: row[1] for row in cursor.fetchall()})
            else:
                title_map = {}
            
            query_time = time.time() - query_start
            
            # 6. Python构造JSON并COPY插入
            def data_stream():
                for cid, in corpusids:
                    # 构造references JSON
                    ref_ids = ref_batch.get(cid, [])
                    if ref_ids:
                        ref_list = [{"corpusid": rid, "title": title_map.get(rid, "")} for rid in ref_ids]
                        ref_json = orjson.dumps(ref_list).decode('utf-8')
                    else:
                        ref_json = '[]'
                    
                    # 构造citations JSON
                    cite_ids = cite_batch.get(cid, [])
                    if cite_ids:
                        cite_list = [{"corpusid": cid2, "title": title_map.get(cid2, "")} for cid2 in cite_ids]
                        cite_json = orjson.dumps(cite_list).decode('utf-8')
                    else:
                        cite_json = '[]'
                    
                    yield f"{cid}\t{ref_json}\t{cite_json}\tf\n".encode('utf-8')
            
            copy_start = time.time()
            cursor.copy_expert(copy_sql, CopyStream(data_stream()))
            copy_time = time.time() - copy_start
            
            # 7. 清理批次数据，释放内存
            ref_batch.clear()
            cite_batch.clear()
            title_map.clear()
            all_ids.clear()
            
            inserted = len(corpusids)
            total_inserted += inserted
            batch_time = time.time() - batch_start
            rate = inserted / batch_time if batch_time > 0 else 0
            
            # 批量提交
            commit_start = time.time()
            if batch_num % commit_interval == 0 or i + batch_size >= len(all_corpusids):
                conn.commit()
                commit_marker = "✓"
            else:
                commit_marker = " "
            commit_time = time.time() - commit_start
            
            # 计算预估剩余时间
            elapsed_step5 = time.time() - step5_start
            progress = (total_inserted / unique_count * 100) if unique_count > 0 else 0
            remaining_count = unique_count - total_inserted
            avg_rate = total_inserted / elapsed_step5 if elapsed_step5 > 0 else 0
            eta_seconds = remaining_count / avg_rate if avg_rate > 0 else 0
            
            # 格式化ETA
            if eta_seconds > 3600:
                eta_str = f"{eta_seconds/3600:.1f}小时"
            elif eta_seconds > 60:
                eta_str = f"{eta_seconds/60:.1f}分钟"
            else:
                eta_str = f"{eta_seconds:.0f}秒"
            
            # 每批显示进度
            if batch_num % 5 == 0 or batch_num == 1:
                print(f"    批次 #{batch_num}{commit_marker}: {inserted:,}条 | {batch_time:.2f}秒 | {rate:.0f}条/秒 | 进度{progress:.1f}% | ETA: {eta_str}")
                print(f"        详细: IN查询({query_time:.2f}s) + Python+COPY({copy_time:.2f}s) + 提交({commit_time:.3f}s)")
                
                # 记录到日志文件
                log_performance(
                    "阶段5-批次进度",
                    batch=batch_num,
                    inserted=f"{total_inserted:,}/{unique_count:,}",
                    progress=f"{progress:.1f}%",
                    rate=f"{avg_rate:.0f}条/秒",
                    eta=eta_str
                )
        
        except Exception as e:
            error_msg = str(e)[:200]
            print(f"    ✗ 批次 #{batch_num} 失败: {error_msg}")
            failed_batches.append((batch_num, error_msg))
            conn.rollback()
            
            if len(failed_batches) >= 3:
                recent_failures = [b for b in failed_batches if b[0] > batch_num - 5]
                if len(recent_failures) >= 3:
                    print(f"\n    ❌ 连续失败超过3批，终止处理")
                    break
            continue
    
    conn.commit()
    
    print(f"\n    ✓ 总计插入: {total_inserted:,} 条（{time.time() - step5_start:.1f}秒）")
    
    # 显示失败批次汇总（如果有）
    if failed_batches:
        print(f"    ⚠️  失败批次: {len(failed_batches)}/{batch_num} ({len(failed_batches)/batch_num*100:.1f}%)")
        for batch_id, error in failed_batches[:5]:  # 仅显示前5个
            print(f"      #{batch_id}: {error}")
    
    # 步骤6: 清理临时表
    print("\n  步骤6: 清理临时表...")
    cursor.execute("DROP TABLE IF EXISTS temp_title_cache")
    cursor.execute("DROP TABLE IF EXISTS temp_unique_corpusids")
    cursor.execute(f"ALTER TABLE {TEMP_TABLE} SET (autovacuum_enabled = true)")
    conn.commit()
    print("    ✓ 完成")
    
    # 统计结果
    elapsed = time.time() - start_time
    avg_speed = total_inserted/elapsed if elapsed > 0 else 0
    print(f"\n  【阶段5完成】{unique_count:,}条 | {batch_num}批 | {avg_speed:.0f}条/秒 | {elapsed:.1f}秒")
    
    log_performance(
        "阶段5-INSERT完成",
        unique_corpusids=f"{unique_count:,}",
        inserted=f"{total_inserted:,}",
        batches=batch_num,
        failed=len(failed_batches),
        success_rate=f"{(batch_num-len(failed_batches))/batch_num*100:.1f}%" if batch_num > 0 else "100%",
        time_sec=f"{elapsed:.2f}",
        avg_speed=f"{total_inserted/elapsed:.0f}" if elapsed > 0 else "0"
    )


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
        
        # 如果使用 --force-stage3 或 --force-stage4，自动跳过阶段0、1、2
        # 因为这些参数意味着用户想直接利用已有的 citation_raw 表
        if force_stage3 or force_stage4:
            print("\n【快速缓存重建模式】检测到 --force-stage3/4，自动跳过阶段0、1、2...")
            print("  假设 citation_raw 表已存在并包含数据")
        
        # 如果跳过导入和索引，直接跳到阶段3
        elif skip_import and skip_index:
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
🔥 推荐分步执行（每个阶段独立运行，可中断恢复）：

  阶段1：导入gz文件到citation_raw表（14亿+条记录，约1-2小时）
  python batch_update/import_citations.py D:\\gz_temp\\citations --only-import
  
  阶段2：创建索引（约30-60分钟）
  python batch_update/import_citations.py D:\\gz_temp\\citations --only-index
  
  阶段3：构造references缓存（约20-30分钟）
  python batch_update/import_citations.py D:\\gz_temp\\citations --only-stage3
  
  阶段4：构造citations缓存（约20-30分钟）
  python batch_update/import_citations.py D:\\gz_temp\\citations --only-stage4
  
  阶段5：更新temp_import表（约10-20分钟，需要temp_import表已有数据）
  python batch_update/import_citations.py D:\\gz_temp\\citations --only-stage5

📋 一次性执行（不推荐，中断后需重头开始）：
  python batch_update/import_citations.py D:\\gz_temp\\citations

⚙️ 其他常用选项：
  # 清空citation_raw表重新导入
  python batch_update/import_citations.py D:\\gz_temp\\citations --truncate --only-import
  
  # 强制重建缓存（已有缓存时，自动跳过阶段0-2）
  python batch_update/import_citations.py D:\\gz_temp\\citations --force-stage3
  python batch_update/import_citations.py D:\\gz_temp\\citations --force-stage4
  
  # 指定其他机器
  python batch_update/import_citations.py D:\\gz_temp\\citations --machine machine2

⚠️ 重要提示：
  1. 阶段5：临时表聚合（SQL层面）+ 分批INSERT（显示进度）
  2. 阶段5会插入约6700万条记录（每批10万条，实时显示进度）
  3. 不做去重：重复执行会产生重复数据（建议执行前清空temp_import）
  4. citation_raw表默认保留（14亿+记录，重建需1-2小时）
  5. 缓存表temp_references和temp_citations可复用，除非数据变化
  6. 确保corpusid_mapping_title表已存在（用于填充title字段）
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
    parser.add_argument("--force-stage3", action="store_true", help="强制重建阶段3缓存（忽略已有缓存，自动跳过阶段0-2）")
    parser.add_argument("--force-stage4", action="store_true", help="强制重建阶段4缓存（忽略已有缓存，自动跳过阶段0-2）")
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

