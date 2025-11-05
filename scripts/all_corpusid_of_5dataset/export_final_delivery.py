#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 final_delivery 表导出数据到 JSONL（多进程并行版本）
- 从 final_delivery 表读取 corpusid（按 id 顺序）
- 仅查询本地数据库（machine0）
- 输出到 E:\final_delivery
- 文件名：随机8位UUID.jsonl
"""

import sys
import os
import json
import time
import uuid
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from datetime import datetime
from multiprocessing import Process, Queue, Manager, Value, Lock
from queue import Empty

import psycopg2
from psycopg2.extras import DictCursor

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from db_config import get_db_config

# =============================================================================
# 配置参数
# =============================================================================

BATCH_SIZE = 50000  # 每批50000个corpusid
NUM_WORKERS = 6  # 6个worker进程
TARGET_TOTAL = 146_430_272  # final_delivery 表的总记录数
OUTPUT_DIR = r"E:\final_delivery"  # 输出目录

# 队列大小限制
TASK_QUEUE_SIZE = 8
RESULT_QUEUE_SIZE = 4

# 日志配置
ENABLE_FILE_LOGGING = False
LOG_FILE = 'export_final_delivery.log'

# =============================================================================
# JSON清理工具
# =============================================================================

_CONTROL_CHARS = dict.fromkeys(range(32))
_CONTROL_CHARS.update(dict.fromkeys(range(127, 160)))
_TRANSLATION_TABLE = str.maketrans(_CONTROL_CHARS)

def safe_json_loads(json_str: str) -> dict:
    """安全解析JSON字符串"""
    if not json_str:
        return {}
    cleaned = json_str.translate(_TRANSLATION_TABLE)
    return json.loads(cleaned)

# =============================================================================
# 日志配置
# =============================================================================

def setup_logger(name: str, log_file: str = None, console_output: bool = True, enable_file: bool = True):
    """配置日志"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    if log_file and enable_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

logger = setup_logger('main', LOG_FILE, console_output=True, enable_file=ENABLE_FILE_LOGGING)

# =============================================================================
# 进度管理
# =============================================================================

def create_progress_table(conn):
    """创建导出进度表"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS final_delivery_progress (
                id BIGSERIAL PRIMARY KEY,
                start_id BIGINT NOT NULL,
                batch_size INT NOT NULL,
                batch_filename TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_final_delivery_progress_start_id
            ON final_delivery_progress(start_id)
        """)
        conn.commit()
    finally:
        cursor.close()


def get_last_progress(conn) -> Optional[Dict]:
    """获取最后一次成功的进度"""
    cursor = conn.cursor(cursor_factory=DictCursor)
    try:
        cursor.execute("""
            SELECT start_id, batch_size 
            FROM final_delivery_progress 
            ORDER BY id DESC 
            LIMIT 1
        """)
        result = cursor.fetchone()
        
        if result:
            return dict(result)
        return None
    finally:
        cursor.close()


def record_progress(conn, start_id: int, batch_size: int, filename: str):
    """记录进度"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO final_delivery_progress (start_id, batch_size, batch_filename)
            VALUES (%s, %s, %s)
        """, (start_id, batch_size, filename))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()

# =============================================================================
# 数据查询函数
# =============================================================================

def get_corpus_ids_batch(conn, start_id: int, batch_size: int) -> List[int]:
    """从 final_delivery 表获取一批 corpusid（按 id 顺序）"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT corpusid 
            FROM final_delivery 
            WHERE id >= %s AND id < %s
            ORDER BY id
        """, (start_id, start_id + batch_size))
        
        results = cursor.fetchall()
        return [row[0] for row in results]
    finally:
        cursor.close()


def batch_query_table(conn, table_name: str, corpus_ids: List[int], logger=None, batch_id=None) -> Dict[int, str]:
    """批量查询表数据（范围查询优化版本）"""
    if not corpus_ids:
        return {}
    
    func_start = time.time()
    
    # 获取ID范围
    min_id = min(corpus_ids)
    max_id = max(corpus_ids)
    
    cursor = conn.cursor()
    try:
        execute_start = time.time()
        cursor.execute(f"""
            SELECT corpusid, data 
            FROM {table_name} 
            WHERE corpusid BETWEEN %s AND %s
        """, (min_id, max_id))
        execute_elapsed = time.time() - execute_start
        
        fetchall_start = time.time()
        results = cursor.fetchall()
        fetchall_elapsed = time.time() - fetchall_start
        
        build_dict_start = time.time()
        result_dict = {row[0]: row[1] for row in results}
        build_dict_elapsed = time.time() - build_dict_start
        
        if logger:
            func_elapsed = time.time() - func_start
            hit_rate = (len(results) / len(corpus_ids) * 100) if corpus_ids else 0
            logger.info(f"  [Query-{table_name}] batch={batch_id}, range=[{min_id}, {max_id}], "
                       f"query_ids={len(corpus_ids)}, result_count={len(results)}, hit_rate={hit_rate:.1f}%, "
                       f"execute={execute_elapsed:.3f}s, fetch={fetchall_elapsed:.3f}s, build={build_dict_elapsed:.3f}s, "
                       f"total={func_elapsed:.3f}s")
        
        return result_dict
    finally:
        cursor.close()


def batch_query_authors(conn, author_ids: List[str], logger=None, batch_id=None) -> Dict[str, dict]:
    """批量查询 authors 表"""
    if not author_ids:
        return {}
    
    func_start = time.time()
    
    author_ids_int = []
    for aid in author_ids:
        if aid:
            try:
                author_ids_int.append(int(aid))
            except (ValueError, TypeError):
                continue
    
    author_ids_int.sort()
    
    if not author_ids_int:
        return {}
    
    cursor = conn.cursor()
    try:
        execute_start = time.time()
        cursor.execute("""
            SELECT authorid, data 
            FROM authors 
            WHERE authorid = ANY(%s)
        """, (author_ids_int,))
        execute_elapsed = time.time() - execute_start
        
        results = cursor.fetchall()
        
        result_dict = {}
        for row in results:
            try:
                result_dict[str(row[0])] = safe_json_loads(row[1])
            except (json.JSONDecodeError, ValueError) as e:
                result_dict[str(row[0])] = None
        
        func_elapsed = time.time() - func_start
        if logger and batch_id:
            logger.info(f"  [Query-authors] batch={batch_id}, input={len(author_ids_int)}, result={len(result_dict)}, time={func_elapsed:.3f}s")
        
        return result_dict
    finally:
        cursor.close()


def batch_query_venues(conn, venue_ids: List[str]) -> Dict[str, dict]:
    """批量查询 publication_venues 表"""
    if not venue_ids:
        return {}
    
    venue_ids_sorted = sorted(venue_ids)
    
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT publicationvenueid, data 
            FROM publication_venues 
            WHERE publicationvenueid = ANY(%s)
        """, (venue_ids_sorted,))
        
        results = cursor.fetchall()
        
        result_dict = {}
        for row in results:
            try:
                result_dict[row[0]] = safe_json_loads(row[1])
            except (json.JSONDecodeError, ValueError) as e:
                result_dict[row[0]] = None
        
        return result_dict
    finally:
        cursor.close()

# =============================================================================
# 数据合并处理
# =============================================================================

def create_empty_base_structure(corpusid: int) -> dict:
    """创建空的基础结构"""
    return {
        "corpusid": corpusid,
        "externalids": {},
        "externalIds": {},
        "url": None,
        "title": None,
        "authors": [],
        "venue": None,
        "year": None,
        "referenceCount": 0,
        "citationCount": 0,
        "influentialCitationCount": 0,
        "isOpenAccess": False,
        "s2FieldsOfStudy": [],
        "publicationTypes": [],
        "publicationDate": None,
        "journal": None
    }


def normalize_field_names(data: dict) -> dict:
    """规范化字段名"""
    field_mapping = {
        'referencecount': 'referenceCount',
        'citationcount': 'citationCount',
        'influentialcitationcount': 'influentialCitationCount',
        'isopenaccess': 'isOpenAccess',
        'publicationdate': 'publicationDate',
        'publicationtypes': 'publicationTypes',
        's2fieldsofstudy': 's2FieldsOfStudy',
    }
    
    for old_name, new_name in field_mapping.items():
        if old_name in data:
            data[new_name] = data.pop(old_name)
    
    if 'externalids' in data:
        data['externalIds'] = data['externalids']
    
    return data


def merge_base_data(
    corpus_ids: List[int],
    papers_dict: Dict[int, str],
    abstracts_dict: Dict[int, str],
    tldrs_dict: Dict[int, str]
) -> Tuple[Dict[int, dict], int, List[int]]:
    """合并基础数据"""
    merged_results = {}
    corrupted_count = 0
    failed_corpus_ids = []
    
    for corpusid in corpus_ids:
        if corpusid in papers_dict:
            try:
                base = safe_json_loads(papers_dict[corpusid])
                base = normalize_field_names(base)
            except (json.JSONDecodeError, ValueError) as e:
                base = create_empty_base_structure(corpusid)
                corrupted_count += 1
                failed_corpus_ids.append(corpusid)
        else:
            base = create_empty_base_structure(corpusid)
        
        # 添加 abstracts
        if corpusid in abstracts_dict:
            try:
                abs_data = safe_json_loads(abstracts_dict[corpusid])
                base['abstract'] = abs_data.get('abstract') or ""
                base['openAccessPdf'] = abs_data.get('openaccessinfo') or {}
            except (json.JSONDecodeError, ValueError):
                base['abstract'] = ""
                base['openAccessPdf'] = {}
        else:
            base['abstract'] = ""
            base['openAccessPdf'] = {}
        
        # 添加 tldrs
        if corpusid in tldrs_dict:
            try:
                tldr_data = safe_json_loads(tldrs_dict[corpusid])
                base['tldr'] = {
                    'model': tldr_data.get('model'),
                    'text': tldr_data.get('text')
                }
            except (json.JSONDecodeError, ValueError):
                base['tldr'] = None
        else:
            base['tldr'] = None
        
        # 添加预留字段
        base.setdefault('paperId', "")
        base.setdefault('externalIds', {})
        base.setdefault('fieldsOfStudy', [])
        base.setdefault('citations', [])
        base.setdefault('references', [])
        base.setdefault('citationStyles', {})
        base.setdefault('content', {})
        base.setdefault('embedding', {})
        base.setdefault('detailsOfReference', {})
        base.setdefault('detailsOfCitations', {})
        base.setdefault('authors', [])
        
        base['corpusId'] = corpusid
        
        # 确保数值类型
        if base.get('referenceCount') is None:
            base['referenceCount'] = 0
        if base.get('citationCount') is None:
            base['citationCount'] = 0
        if base.get('influentialCitationCount') is None:
            base['influentialCitationCount'] = 0
        if base.get('isOpenAccess') is None:
            base['isOpenAccess'] = False
        
        # 确保数组类型
        if base.get('s2FieldsOfStudy') is None:
            base['s2FieldsOfStudy'] = []
        if base.get('publicationTypes') is None:
            base['publicationTypes'] = []
        
        # 确保 externalids 和 externalIds 同时存在
        if 'externalids' in base and 'externalIds' not in base:
            base['externalIds'] = base['externalids']
        elif 'externalIds' in base and 'externalids' not in base:
            base['externalids'] = base['externalIds']
        elif 'externalids' not in base and 'externalIds' not in base:
            base['externalids'] = {}
            base['externalIds'] = {}
        
        base['fieldsOfStudy'] = base.get('s2FieldsOfStudy', [])
        
        merged_results[corpusid] = base
    
    return merged_results, corrupted_count, failed_corpus_ids


def collect_author_ids(merged_results: Dict[int, dict], logger=None, batch_id=None) -> Set[str]:
    """收集所有 authorId"""
    author_ids = set()
    
    for base in merged_results.values():
        authors_list = base.get('authors')
        if authors_list and isinstance(authors_list, list):
            for author_obj in authors_list:
                if isinstance(author_obj, dict):
                    author_id = author_obj.get('authorId')
                    if author_id:
                        author_ids.add(str(author_id))
    
    return author_ids


def collect_venue_ids(merged_results: Dict[int, dict]) -> Set[str]:
    """收集所有 publicationvenueid"""
    venue_ids = set()
    for base in merged_results.values():
        venue_id = base.get('publicationvenueid')
        if venue_id:
            venue_ids.add(str(venue_id))
    return venue_ids


def add_related_data(
    merged_results: Dict[int, dict],
    authors_dict: Dict[str, dict],
    venues_dict: Dict[str, dict]
):
    """添加关联数据"""
    for base in merged_results.values():
        # 添加 detailsOfAuthors
        authors_list = base.get('authors')
        details_of_authors = {}
        
        if authors_list and isinstance(authors_list, list):
            for author_obj in authors_list:
                if isinstance(author_obj, dict):
                    author_id = str(author_obj.get('authorId', ''))
                    if author_id and author_id in authors_dict:
                        details_of_authors[author_id] = authors_dict[author_id]
        
        base['detailsOfAuthors'] = details_of_authors
        
        # 添加 publicationVenue
        venue_id = base.get('publicationvenueid')
        if venue_id and str(venue_id) in venues_dict:
            base['publicationVenue'] = venues_dict[str(venue_id)]
        else:
            base['publicationVenue'] = {}
        
        # 删除不需要的字段
        if 'publicationvenueid' in base:
            del base['publicationvenueid']

# =============================================================================
# Worker进程（数据处理）
# =============================================================================

def worker_process(
    worker_id: int,
    task_queue: Queue,
    result_queue: Queue,
    stats: dict,
    stats_lock: Lock
):
    """Worker进程：处理数据查询和合并"""
    worker_logger = setup_logger(f'Worker-{worker_id}', LOG_FILE, console_output=False, enable_file=ENABLE_FILE_LOGGING)
    worker_logger.info(f"Worker-{worker_id} started")
    
    # 只连接本地数据库
    local_conn = None
    try:
        local_config = get_db_config('machine0')
        local_conn = psycopg2.connect(**local_config)
        local_cursor = local_conn.cursor()
        local_cursor.execute("SET work_mem = '256MB'")
        local_cursor.close()
        
        worker_logger.info(f"Worker-{worker_id} connected to local database")
    except Exception as e:
        worker_logger.error(f"Worker-{worker_id} failed to connect to database: {e}")
        if local_conn:
            local_conn.close()
        return
    
    # 处理任务
    while True:
        start_id = None
        try:
            task = task_queue.get(timeout=5)
            
            if task is None:
                worker_logger.info(f"Worker-{worker_id} received stop signal")
                break
            
            start_id, batch_size = task
            batch_start_time = time.time()
            worker_logger.info(f"Worker-{worker_id} ========== START Batch {start_id} ==========")
            
            # 1. 获取 corpus_ids
            step_start = time.time()
            corpus_ids = get_corpus_ids_batch(local_conn, start_id, batch_size)
            step_elapsed = time.time() - step_start
            worker_logger.info(f"Worker-{worker_id} [Step1-GetCorpusIDs] batch={start_id}, count={len(corpus_ids) if corpus_ids else 0}, time={step_elapsed:.3f}s")
            
            if not corpus_ids:
                worker_logger.warning(f"Worker-{worker_id} no data for start_id={start_id}")
                result_queue.put((start_id, None, None))
                continue
            
            # 2. 批量查询基础表（全部从本地）
            step2_start = time.time()
            
            query_start = time.time()
            papers_dict = batch_query_table(local_conn, "papers", corpus_ids, worker_logger, start_id)
            worker_logger.info(f"Worker-{worker_id} [Step2.1-QueryPapers] batch={start_id}, count={len(papers_dict)}, time={time.time()-query_start:.3f}s")
            
            query_start = time.time()
            abstracts_dict = batch_query_table(local_conn, "abstracts_sorted", corpus_ids, worker_logger, start_id)
            worker_logger.info(f"Worker-{worker_id} [Step2.2-QueryAbstracts] batch={start_id}, count={len(abstracts_dict)}, time={time.time()-query_start:.3f}s")
            
            query_start = time.time()
            tldrs_dict = batch_query_table(local_conn, "tldrs", corpus_ids, worker_logger, start_id)
            worker_logger.info(f"Worker-{worker_id} [Step2.3-QueryTldrs] batch={start_id}, count={len(tldrs_dict)}, time={time.time()-query_start:.3f}s")
            
            step2_elapsed = time.time() - step2_start
            worker_logger.info(f"Worker-{worker_id} [Step2-QueryBaseTables-TOTAL] batch={start_id}, time={step2_elapsed:.3f}s")
            
            # 3. 合并基础数据
            step_start = time.time()
            merged_results, corrupted_count, failed_corpus_ids = merge_base_data(
                corpus_ids, papers_dict, abstracts_dict, tldrs_dict
            )
            step_elapsed = time.time() - step_start
            worker_logger.info(f"Worker-{worker_id} [Step3-MergeBaseData] batch={start_id}, corrupted={corrupted_count}, time={step_elapsed:.3f}s")
            
            # 4. 收集关联ID
            step_start = time.time()
            author_ids = collect_author_ids(merged_results, worker_logger, start_id)
            venue_ids = collect_venue_ids(merged_results)
            step_elapsed = time.time() - step_start
            worker_logger.info(f"Worker-{worker_id} [Step4-CollectRelatedIDs] batch={start_id}, authors={len(author_ids)}, venues={len(venue_ids)}, time={step_elapsed:.3f}s")
            
            # 5. 批量查询关联表（全部从本地）
            step5_start = time.time()
            
            query_start = time.time()
            authors_dict = batch_query_authors(local_conn, list(author_ids), worker_logger, start_id)
            worker_logger.info(f"Worker-{worker_id} [Step5.1-QueryAuthors] batch={start_id}, result_count={len(authors_dict)}, time={time.time()-query_start:.3f}s")
            
            query_start = time.time()
            venues_dict = batch_query_venues(local_conn, list(venue_ids))
            worker_logger.info(f"Worker-{worker_id} [Step5.2-QueryVenues] batch={start_id}, count={len(venues_dict)}, time={time.time()-query_start:.3f}s")
            
            step5_elapsed = time.time() - step5_start
            worker_logger.info(f"Worker-{worker_id} [Step5-QueryRelatedTables-TOTAL] batch={start_id}, time={step5_elapsed:.3f}s")
            
            # 6. 添加关联数据
            step_start = time.time()
            add_related_data(merged_results, authors_dict, venues_dict)
            step_elapsed = time.time() - step_start
            worker_logger.info(f"Worker-{worker_id} [Step6-AddRelatedData] batch={start_id}, time={step_elapsed:.3f}s")
            
            # 7. 发送到结果队列
            step_start = time.time()
            result_queue.put((start_id, corpus_ids, merged_results))
            step_elapsed = time.time() - step_start
            worker_logger.info(f"Worker-{worker_id} [Step7-PutToResultQueue] batch={start_id}, time={step_elapsed:.3f}s")
            
            # 批次总计
            batch_elapsed = time.time() - batch_start_time
            worker_logger.info(
                f"Worker-{worker_id} ========== COMPLETE Batch {start_id}: "
                f"{len(corpus_ids)} records in {batch_elapsed:.3f}s (avg {len(corpus_ids)/batch_elapsed:.1f} records/s) =========="
            )
            
            # 更新统计
            with stats_lock:
                stats['processed'] += len(corpus_ids)
                stats['batches'] += 1
            
        except Empty:
            continue
        except Exception as e:
            worker_logger.error(f"Worker-{worker_id} error processing batch: {e}")
            import traceback
            traceback.print_exc()
            if start_id is not None:
                try:
                    result_queue.put((start_id, None, None), timeout=5)
                except:
                    pass
    
    local_conn.close()
    worker_logger.info(f"Worker-{worker_id} stopped")

# =============================================================================
# Writer进程（文件写入）
# =============================================================================

def writer_process(
    result_queue: Queue,
    progress_queue: Queue,
    output_dir: str
):
    """Writer进程：串行写入文件"""
    writer_logger = setup_logger('Writer', LOG_FILE, console_output=False, enable_file=ENABLE_FILE_LOGGING)
    writer_logger.info("Writer process started")
    
    os.makedirs(output_dir, exist_ok=True)
    
    while True:
        try:
            result = result_queue.get(timeout=10)
            
            if result is None:
                writer_logger.info("Writer received stop signal")
                break
            
            start_id, corpus_ids, merged_results = result
            writer_logger.info(f"Writer ========== START Writing Batch {start_id} ==========")
            
            if corpus_ids is None or merged_results is None:
                writer_logger.warning(f"Skipping empty result for start_id={start_id}")
                progress_queue.put((start_id, False))
                continue
            
            write_start_time = time.time()
            
            # 生成随机8位UUID文件名
            file_uuid = uuid.uuid4().hex[:8]
            filename = f"{file_uuid}.jsonl"
            filepath = os.path.join(output_dir, filename)
            
            try:
                file_write_start = time.time()
                with open(filepath, 'w', encoding='utf-8', buffering=16777216) as f:  # 16MB缓冲
                    for corpusid in corpus_ids:
                        if corpusid not in merged_results:
                            raise Exception(f"Missing data for corpusid {corpusid}")
                        
                        json_line = json.dumps(merged_results[corpusid], ensure_ascii=False)
                        f.write(json_line + '\n')
                file_write_elapsed = time.time() - file_write_start
                
                file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                write_speed_mbs = file_size_mb / file_write_elapsed if file_write_elapsed > 0 else 0
                
                writer_logger.info(
                    f"Writer [Step1-WriteFile] batch={start_id}, file={filename}, "
                    f"records={len(corpus_ids)}, size={file_size_mb:.2f}MB, "
                    f"time={file_write_elapsed:.3f}s, speed={write_speed_mbs:.2f}MB/s"
                )
                
                notify_start = time.time()
                progress_queue.put((start_id, True, filename))
                notify_elapsed = time.time() - notify_start
                writer_logger.info(f"Writer [Step2-NotifyComplete] batch={start_id}, time={notify_elapsed:.3f}s")
                
                total_elapsed = time.time() - write_start_time
                writer_logger.info(
                    f"Writer ========== COMPLETE Batch {start_id}: "
                    f"{len(corpus_ids)} records in {total_elapsed:.3f}s =========="
                )
                
            except Exception as e:
                writer_logger.error(f"Failed to write {filename}: {e}")
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                        writer_logger.info(f"Removed failed file: {filename}")
                    except:
                        pass
                progress_queue.put((start_id, False))
                raise
        
        except Empty:
            continue
        except Exception as e:
            writer_logger.error(f"Writer error: {e}")
            import traceback
            traceback.print_exc()
    
    writer_logger.info("Writer process stopped")

# =============================================================================
# 主进程（任务分配和进度管理）
# =============================================================================

def print_progress(processed: int, total: int, speed: float, elapsed: float, 
                   task_queue_size: int = 0, result_queue_size: int = 0):
    """打印进度信息"""
    if total <= 0:
        return
    
    remaining = total - processed
    remaining_time = remaining / speed if speed > 0 else 0
    progress_pct = (processed / total) * 100
    
    elapsed_hours = elapsed / 3600
    remaining_hours = remaining_time / 3600
    
    progress_bar_len = 30
    filled = int(progress_bar_len * min(processed, total) / total)
    bar = '█' * filled + '░' * (progress_bar_len - filled)
    
    print(f"\r[{bar}] {progress_pct:.1f}% | "
          f"已处理: {processed:,} | 速度: {speed:.0f}条/秒 | "
          f"已用: {elapsed_hours:.1f}h | 剩余: {remaining_hours:.1f}h | "
          f"队列: T{task_queue_size}/{TASK_QUEUE_SIZE} R{result_queue_size}/{RESULT_QUEUE_SIZE}",
          end='', flush=True)


def main():
    """主函数"""
    print("="*80)
    print("从 final_delivery 表导出数据 - 并行处理模式")
    print("="*80)
    print(f"Worker进程数: {NUM_WORKERS}")
    print(f"批次大小: {BATCH_SIZE:,} 条/批")
    print(f"目标总量: {TARGET_TOTAL:,} 条")
    print(f"输出目录: {OUTPUT_DIR}")
    print(f"数据源: 仅本地数据库 (machine0)")
    print("="*80)
    print()
    
    # 建立数据库连接
    try:
        local_config = get_db_config('machine0')
        local_conn = psycopg2.connect(**local_config)
        print(f"✓ 已连接到数据库: {local_config['database']}")
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}")
        return
    
    # 创建进度表
    create_progress_table(local_conn)
    
    # 进度恢复
    last_progress = get_last_progress(local_conn)
    
    if last_progress:
        start_id = last_progress['start_id'] + last_progress['batch_size']
        already_processed = start_id - 1
        print(f"✓ 断点续传: 从 start_id={start_id:,} 继续")
        print(f"  已完成: {already_processed:,} 条 ({already_processed/TARGET_TOTAL*100:.1f}%)")
    else:
        start_id = 1
        already_processed = 0
        print(f"✓ 首次运行: 从头开始处理")
    print()
    
    # 创建队列
    manager = Manager()
    task_queue = Queue(maxsize=TASK_QUEUE_SIZE)
    result_queue = Queue(maxsize=RESULT_QUEUE_SIZE)
    progress_queue = Queue()
    
    stats = manager.dict()
    stats['processed'] = 0
    stats['batches'] = 0
    stats_lock = Lock()
    
    # 启动Worker进程
    workers = []
    for i in range(NUM_WORKERS):
        p = Process(
            target=worker_process,
            args=(i, task_queue, result_queue, stats, stats_lock),
            name=f"Worker-{i}"
        )
        p.start()
        workers.append(p)
    
    print(f"✓ 已启动 {NUM_WORKERS} 个Worker进程")
    
    # 启动Writer进程
    writer = Process(
        target=writer_process,
        args=(result_queue, progress_queue, OUTPUT_DIR),
        name="Writer"
    )
    writer.start()
    print(f"✓ 已启动Writer进程")
    print()
    print("开始处理数据...")
    print()
    
    # 主循环
    current_id = start_id
    total_processed = already_processed
    overall_start_time = time.time()
    pending_batches = {}
    
    if already_processed > 0:
        print_progress(total_processed, TARGET_TOTAL, 0, 0, 0, 0)
    
    try:
        while total_processed < TARGET_TOTAL:
            # 分配任务
            while len(pending_batches) < TASK_QUEUE_SIZE:
                if current_id >= 1 + TARGET_TOTAL:
                    break
                
                try:
                    task_queue.put((current_id, BATCH_SIZE), timeout=1)
                    pending_batches[current_id] = BATCH_SIZE
                    current_id += BATCH_SIZE
                except Exception as e:
                    break
            
            # 接收进度
            try:
                progress_result = progress_queue.get(timeout=1)
                
                if len(progress_result) == 2:
                    batch_start_id, success = progress_result
                    filename = None
                else:
                    batch_start_id, success, filename = progress_result
                
                if success and filename:
                    record_progress(local_conn, batch_start_id, BATCH_SIZE, filename)
                    
                    batch_size = pending_batches.pop(batch_start_id, BATCH_SIZE)
                    total_processed += batch_size
                    
                    elapsed = time.time() - overall_start_time
                    current_session_processed = total_processed - already_processed
                    speed = current_session_processed / elapsed if elapsed > 0 else 0
                    print_progress(total_processed, TARGET_TOTAL, speed, elapsed,
                                 len(pending_batches), result_queue.qsize())
                    
                else:
                    print(f"\n✗ 批次 {batch_start_id} 处理失败!")
                    pending_batches.pop(batch_start_id, None)
                    raise Exception(f"Batch {batch_start_id} processing failed")
            
            except Empty:
                continue
        
        print("\n\n✓ 所有批次处理完成!")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断处理")
    except Exception as e:
        print(f"\n\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n正在停止所有进程...")
        
        for _ in range(NUM_WORKERS):
            task_queue.put(None)
        
        for p in workers:
            p.join(timeout=10)
            if p.is_alive():
                p.terminate()
        
        result_queue.put(None)
        writer.join(timeout=10)
        if writer.is_alive():
            writer.terminate()
        
        local_conn.close()
        
        total_elapsed = time.time() - overall_start_time
        current_session_processed = total_processed - already_processed
        print("\n" + "="*80)
        print("数据导出完成!")
        print("="*80)
        print(f"总处理量: {total_processed:,} 条 (本次: {current_session_processed:,} 条)")
        print(f"本次耗时: {total_elapsed/3600:.2f} 小时")
        if total_elapsed > 0:
            print(f"本次平均速度: {current_session_processed/total_elapsed:.0f} 条/秒")
        print(f"输出目录: {OUTPUT_DIR}")
        print("="*80)


if __name__ == '__main__':
    main()

