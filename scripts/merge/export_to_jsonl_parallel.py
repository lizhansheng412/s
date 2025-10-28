#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S2ORC数据合并导出 - 多进程并行优化版本
目标：1天内完成1亿条数据处理

架构：
- 主进程：任务分配、进度管理
- 8个Worker进程：并行数据查询和合并
- 1个Writer进程：串行写入文件（避免USB硬盘并发瓶颈）
"""

import sys
import os
import json
import time
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
from database.config.db_config_v2 import get_db_config

# =============================================================================
# 配置参数
# =============================================================================

BATCH_SIZE = 50000  # 每批50000个corpusid，约3GB/文件（减少查询频率，分摊查询开销）
NUM_WORKERS = 4  # 4个worker进程（SSD可以支持并发）
TARGET_TOTAL = 95_000_000  # 目标9500万条
OUTPUT_DIR = r"F:\delivery_data\first_batch"

# 队列大小限制（避免内存爆炸）
TASK_QUEUE_SIZE = 8  # 任务队列：最多8个待处理批次（根据内存大小调整）
RESULT_QUEUE_SIZE = 4  # 结果队列：最多4个待写入批次（根据内存大小调整）

# 日志配置
ENABLE_FILE_LOGGING = False  # 是否启用文件日志（True=写入日志文件，False=仅控制台输出）
LOG_FILE = 'merge_export_parallel.log'  # 日志文件路径

# =============================================================================
# JSON清理工具（最优化版本）
# =============================================================================

# 创建转换表（str.translate是最快的字符串替换方法，C语言实现）
# 将所有控制字符映射为None（删除）
_CONTROL_CHARS = dict.fromkeys(range(32))  # \x00-\x1f
_CONTROL_CHARS.update(dict.fromkeys(range(127, 160)))  # \x7f-\x9f
_TRANSLATION_TABLE = str.maketrans(_CONTROL_CHARS)

def safe_json_loads(json_str: str) -> dict:
    """
    安全解析JSON字符串（最优化版本）
    使用str.translate()进行字符清理，比正则表达式快5-10倍
    
    Args:
        json_str: JSON字符串
    
    Returns:
        解析后的字典
    """
    if not json_str:
        return {}
    
    # 使用预编译的转换表清理控制字符（最快方法）
    cleaned = json_str.translate(_TRANSLATION_TABLE)
    return json.loads(cleaned)

# =============================================================================
# 日志配置
# =============================================================================

def setup_logger(name: str, log_file: str = None, console_output: bool = True, enable_file: bool = True):
    """
    配置日志
    
    Args:
        name: logger名称
        log_file: 日志文件路径
        console_output: 是否输出到控制台
        enable_file: 是否启用文件日志（由全局配置ENABLE_FILE_LOGGING控制）
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    
    # 控制台输出（可选）
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # 文件输出（受全局开关控制）
    if log_file and enable_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# 主进程logger（根据配置决定是否写入文件）
logger = setup_logger('main', LOG_FILE, console_output=True, enable_file=ENABLE_FILE_LOGGING)

# =============================================================================
# 进度管理
# =============================================================================

def create_progress_table(conn):
    """创建导出进度表"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS export_progress (
                id BIGSERIAL PRIMARY KEY,
                start_id BIGINT NOT NULL,
                batch_size INT NOT NULL,
                batch_filename TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_export_progress_start_id
            ON export_progress(start_id)
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
            FROM export_progress 
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
            INSERT INTO export_progress (start_id, batch_size, batch_filename)
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
    """从paper_ids表获取一批corpusid（优化版本）"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT corpusid 
            FROM paper_ids 
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
    
    # 获取ID范围（corpus_ids是连续的）
    min_id = min(corpus_ids)
    max_id = max(corpus_ids)
    
    cursor = conn.cursor()
    try:
        # 对abstracts表使用范围查询（解决低命中率+大量空查找问题）
        if table_name == "abstracts":
            # 使用BETWEEN范围查询（1次范围扫描，避免50000次单独索引查找）
            execute_start = time.time()
            cursor.execute(f"""
                SELECT corpusid, data 
                FROM {table_name} 
                WHERE corpusid BETWEEN %s AND %s
            """, (min_id, max_id))
            execute_elapsed = time.time() - execute_start
            
            # 获取结果
            fetchall_start = time.time()
            results = cursor.fetchall()
            fetchall_elapsed = time.time() - fetchall_start
            
            # 构建字典
            build_dict_start = time.time()
            result_dict = {row[0]: row[1] for row in results}
            build_dict_elapsed = time.time() - build_dict_start
            
            # 记录详细日志
            if logger:
                func_elapsed = time.time() - func_start
                hit_rate = (len(results) / len(corpus_ids) * 100) if corpus_ids else 0
                logger.info(f"  [QueryAbstracts-Detail] batch={batch_id}, range=[{min_id}, {max_id}], "
                           f"query_ids={len(corpus_ids)}, result_count={len(results)}, hit_rate={hit_rate:.1f}%, "
                           f"execute={execute_elapsed:.3f}s, fetch={fetchall_elapsed:.3f}s, build={build_dict_elapsed:.3f}s, "
                           f"total={func_elapsed:.3f}s")
        
        else:
            # 其他表也使用范围查询（因为corpus_ids是连续的，范围查询效率更高）
            execute_start = time.time()
            cursor.execute(f"""
                SELECT corpusid, data 
                FROM {table_name} 
                WHERE corpusid BETWEEN %s AND %s
            """, (min_id, max_id))
            execute_elapsed = time.time() - execute_start
            
            # 获取结果
            fetchall_start = time.time()
            results = cursor.fetchall()
            fetchall_elapsed = time.time() - fetchall_start
            
            # 构建字典
            build_dict_start = time.time()
            result_dict = {row[0]: row[1] for row in results}
            build_dict_elapsed = time.time() - build_dict_start
        
        return result_dict
    finally:
        cursor.close()


def batch_query_authors(conn, author_ids: List[str], logger=None, batch_id=None) -> Dict[str, dict]:
    """批量查询authors表（性能监控版本）"""
    if not author_ids:
        return {}
    
    func_start = time.time()
    
    # 步骤1: 转换authorId为int并排序
    convert_start = time.time()
    author_ids_int = []
    for aid in author_ids:
        if aid:
            try:
                author_ids_int.append(int(aid))
            except (ValueError, TypeError):
                continue
    
    # 排序authorId以优化磁盘顺序访问
    author_ids_int.sort()
    convert_elapsed = time.time() - convert_start
    
    if logger and batch_id:
        logger.info(f"  [QueryAuthors-Step1-ConvertAndSort] batch={batch_id}, count={len(author_ids_int)}, time={convert_elapsed:.3f}s")
    
    if not author_ids_int:
        return {}
    
    cursor = conn.cursor()
    try:
        # 步骤2: 执行SQL查询
        execute_start = time.time()
        cursor.execute("""
            SELECT authorid, data 
            FROM authors 
            WHERE authorid = ANY(%s)
        """, (author_ids_int,))
        execute_elapsed = time.time() - execute_start
        
        if logger and batch_id:
            logger.info(f"  [QueryAuthors-Step2-ExecuteSQL] batch={batch_id}, time={execute_elapsed:.3f}s")
        
        # 步骤3: 获取结果
        fetchall_start = time.time()
        results = cursor.fetchall()
        fetchall_elapsed = time.time() - fetchall_start
        
        if logger and batch_id:
            logger.info(f"  [QueryAuthors-Step3-FetchAll] batch={batch_id}, rows={len(results)}, time={fetchall_elapsed:.3f}s")
        
        # 步骤4: 构建结果字典（JSON解析）
        build_dict_start = time.time()
        result_dict = {}
        json_parse_time = 0
        json_parse_count = 0
        
        for row in results:
            try:
                parse_start = time.time()
                result_dict[str(row[0])] = safe_json_loads(row[1])
                json_parse_time += time.time() - parse_start
                json_parse_count += 1
            except (json.JSONDecodeError, ValueError) as e:
                result_dict[str(row[0])] = None
        
        build_dict_elapsed = time.time() - build_dict_start
        avg_json_parse = (json_parse_time / json_parse_count) if json_parse_count > 0 else 0
        
        if logger and batch_id:
            logger.info(f"  [QueryAuthors-Step4-BuildDict] batch={batch_id}, parsed={json_parse_count}, time={build_dict_elapsed:.3f}s, avg_json={avg_json_parse*1000:.3f}ms")
        
        # 总计
        func_elapsed = time.time() - func_start
        if logger and batch_id:
            logger.info(f"  [QueryAuthors-TOTAL] batch={batch_id}, time={func_elapsed:.3f}s (execute={execute_elapsed:.1f}s, fetch={fetchall_elapsed:.1f}s, parse={build_dict_elapsed:.1f}s)")
        
        return result_dict
    finally:
        cursor.close()


def batch_query_venues(conn, venue_ids: List[str]) -> Dict[str, dict]:
    """批量查询publication_venues表（优化版本）"""
    if not venue_ids:
        return {}
    
    # 排序venue_ids以优化磁盘顺序访问
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
    """创建空的基础结构（如果papers表中有数据但解析失败时使用）"""
    return {
        "corpusid": corpusid,
        "externalids": {},  # 小写版本（原始字段）
        "externalIds": {},  # 驼峰版本（预留字段）
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
    """
    规范化字段名：将数据库中的小写字段名转换为驼峰命名
    特殊处理：externalids 保留原字段，同时添加 externalIds（两个字段同时存在，值相同）
    """
    # 字段名映射表（旧名 -> 新名）
    field_mapping = {
        'referencecount': 'referenceCount',
        'citationcount': 'citationCount',
        'influentialcitationcount': 'influentialCitationCount',
        'isopenaccess': 'isOpenAccess',
        'publicationdate': 'publicationDate',
        'publicationtypes': 'publicationTypes',
        's2fieldsofstudy': 's2FieldsOfStudy',
    }
    
    # 执行字段名转换（删除旧字段）
    for old_name, new_name in field_mapping.items():
        if old_name in data:
            data[new_name] = data.pop(old_name)
    
    # 特殊处理：externalids 保留原字段，同时添加驼峰版本（不删除原字段）
    if 'externalids' in data:
        data['externalIds'] = data['externalids']  # 复制值，保留 externalids
    
    return data


def merge_base_data(
    corpus_ids: List[int],
    papers_dict: Dict[int, str],
    abstracts_dict: Dict[int, str],
    tldrs_dict: Dict[int, str]
) -> Tuple[Dict[int, dict], int, List[int]]:
    """
    合并基础数据（容错版本）
    
    Returns:
        (merged_results, corrupted_count, failed_corpus_ids): 合并结果、损坏的记录数和失败的corpusid列表
    """
    merged_results = {}
    corrupted_count = 0
    failed_corpus_ids = []
    
    for corpusid in corpus_ids:
        # 1. 获取base结构（容错处理）
        if corpusid in papers_dict:
            try:
                base = safe_json_loads(papers_dict[corpusid])
                # 规范化字段名
                base = normalize_field_names(base)
            except (json.JSONDecodeError, ValueError) as e:
                # 容错：使用空结构，记录损坏但继续处理
                base = create_empty_base_structure(corpusid)
                corrupted_count += 1
                failed_corpus_ids.append(corpusid)
        else:
            base = create_empty_base_structure(corpusid)
        
        # 2. 添加abstracts字段
        if corpusid in abstracts_dict:
            try:
                abs_data = safe_json_loads(abstracts_dict[corpusid])
                base['abstract'] = abs_data.get('abstract') or ""  # 默认为空字符串而非None
                base['openAccessPdf'] = abs_data.get('openaccessinfo') or {}
            except (json.JSONDecodeError, ValueError):
                base['abstract'] = ""  # 默认为空字符串
                base['openAccessPdf'] = {}
        else:
            base['abstract'] = ""  # 默认为空字符串
            base['openAccessPdf'] = {}
        
        # 3. 添加tldrs字段
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
        
        # 4. 添加预留空字段（按照S2ORC标准格式，只在字段不存在时设置默认值）
        base.setdefault('paperId', "")  # 空字符串而非None
        base.setdefault('externalIds', {})
        base.setdefault('fieldsOfStudy', [])
        base.setdefault('citations', [])
        base.setdefault('references', [])
        base.setdefault('citationStyles', {})
        base.setdefault('content', {})
        base.setdefault('embedding', {})
        base.setdefault('detailsOfReference', {})  # dict类型
        base.setdefault('detailsOfCitations', {})  # dict类型
        base.setdefault('authors', [])  # 空数组而非null
        
        # corpusId 必须与 corpusid 保持一致（强制设置）
        base['corpusId'] = corpusid
        
        # 5. 确保数值类型字段有正确的默认值
        if base.get('referenceCount') is None:
            base['referenceCount'] = 0
        if base.get('citationCount') is None:
            base['citationCount'] = 0
        if base.get('influentialCitationCount') is None:
            base['influentialCitationCount'] = 0
        if base.get('isOpenAccess') is None:
            base['isOpenAccess'] = False
        
        # 6. 确保数组类型字段有正确的默认值
        if base.get('s2FieldsOfStudy') is None:
            base['s2FieldsOfStudy'] = []
        if base.get('publicationTypes') is None:
            base['publicationTypes'] = []
        
        # 7. 确保 externalids 和 externalIds 同时存在且值相同
        if 'externalids' in base and 'externalIds' not in base:
            # 只有小写存在，复制到驼峰
            base['externalIds'] = base['externalids']
        elif 'externalIds' in base and 'externalids' not in base:
            # 只有驼峰存在，复制到小写
            base['externalids'] = base['externalIds']
        elif 'externalids' not in base and 'externalIds' not in base:
            # 两个都不存在，都设为空对象
            base['externalids'] = {}
            base['externalIds'] = {}
        # 如果两个都存在，保持原值（normalize_field_names 已经复制过了）
        
        # 8. fieldsOfStudy 与 s2FieldsOfStudy 保持一致
        base['fieldsOfStudy'] = base.get('s2FieldsOfStudy', [])
        
        merged_results[corpusid] = base
    
    return merged_results, corrupted_count, failed_corpus_ids


def collect_author_ids(merged_results: Dict[int, dict], logger=None, batch_id=None) -> Set[str]:
    """收集所有authorId（性能监控版本）"""
    func_start = time.time()
    
    author_ids = set()
    total_papers = len(merged_results)
    total_authors_found = 0
    
    for base in merged_results.values():
        authors_list = base.get('authors')
        if authors_list and isinstance(authors_list, list):
            for author_obj in authors_list:
                if isinstance(author_obj, dict):
                    author_id = author_obj.get('authorId')
                    if author_id:
                        author_ids.add(str(author_id))
                        total_authors_found += 1
    
    func_elapsed = time.time() - func_start
    
    if logger and batch_id:
        avg_authors_per_paper = total_authors_found / total_papers if total_papers > 0 else 0
        logger.info(f"  [CollectAuthorIDs] batch={batch_id}, papers={total_papers}, unique_authors={len(author_ids)}, total_authors={total_authors_found}, avg={avg_authors_per_paper:.1f}, time={func_elapsed:.3f}s")
    
    return author_ids


def collect_venue_ids(merged_results: Dict[int, dict]) -> Set[str]:
    """收集所有publicationvenueid"""
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
        # 1. 添加detailsOfAuthors（注意是details复数，类型为dict）
        authors_list = base.get('authors')
        details_of_authors = {}
        
        if authors_list and isinstance(authors_list, list):
            for author_obj in authors_list:
                if isinstance(author_obj, dict):
                    author_id = str(author_obj.get('authorId', ''))
                    if author_id and author_id in authors_dict:
                        details_of_authors[author_id] = authors_dict[author_id]
        
        base['detailsOfAuthors'] = details_of_authors  # 默认为{}而不是None
        
        # 2. 添加publicationVenue
        venue_id = base.get('publicationvenueid')
        if venue_id and str(venue_id) in venues_dict:
            base['publicationVenue'] = venues_dict[str(venue_id)]
        else:
            base['publicationVenue'] = {}  # 空字典而非None
        
        # 3. 删除不需要的publicationvenueid字段
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
    """
    Worker进程：处理数据查询和合并
    
    Args:
        worker_id: Worker ID
        task_queue: 任务队列（接收start_id）
        result_queue: 结果队列（发送合并后的数据）
        stats: 共享统计数据
        stats_lock: 统计数据锁
    """
    # Worker日志（根据配置决定是否写入文件）
    worker_logger = setup_logger(f'Worker-{worker_id}', LOG_FILE, console_output=False, enable_file=ENABLE_FILE_LOGGING)
    worker_logger.info(f"Worker-{worker_id} started")
    
    # 建立数据库连接（优化版本）
    local_conn = None
    machine3_conn = None
    try:
        local_config = get_db_config('machine0')
        local_conn = psycopg2.connect(**local_config)
        # 优化本地连接
        local_cursor = local_conn.cursor()
        local_cursor.execute("SET work_mem = '128MB'")
        local_cursor.close()
        
        machine3_config = get_db_config('machine3')
        # 添加连接选项以优化网络传输
        machine3_config['options'] = '-c work_mem=256MB -c random_page_cost=1.1 -c effective_cache_size=2GB'
        machine3_conn = psycopg2.connect(**machine3_config)
        # 设置自动提交模式（减少事务开销）
        machine3_conn.set_session(autocommit=True, readonly=True)
        
        worker_logger.info(f"Worker-{worker_id} connected to databases")
    except Exception as e:
        worker_logger.error(f"Worker-{worker_id} failed to connect to database: {e}")
        # 清理已建立的连接
        if local_conn:
            local_conn.close()
        if machine3_conn:
            machine3_conn.close()
        return
    
    # 处理任务
    while True:
        start_id = None  # 初始化，避免异常时未定义
        try:
            # 从任务队列获取任务（超时5秒）
            task = task_queue.get(timeout=5)
            
            if task is None:  # 结束信号
                worker_logger.info(f"Worker-{worker_id} received stop signal")
                break
            
            start_id, batch_size = task
            batch_start_time = time.time()
            worker_logger.info(f"Worker-{worker_id} ========== START Batch {start_id} ==========")
            
            # 1. 获取corpus_ids
            step_start = time.time()
            corpus_ids = get_corpus_ids_batch(local_conn, start_id, batch_size)
            step_elapsed = time.time() - step_start
            worker_logger.info(f"Worker-{worker_id} [Step1-GetCorpusIDs] batch={start_id}, count={len(corpus_ids) if corpus_ids else 0}, time={step_elapsed:.3f}s")
            
            if not corpus_ids:
                worker_logger.warning(f"Worker-{worker_id} no data for start_id={start_id}")
                result_queue.put((start_id, None, None))
                continue
            
            # 2. 批量查询基础表 (3次查询)
            step2_start = time.time()
            
            # 2.1 查询papers表
            query_start = time.time()
            papers_dict = batch_query_table(machine3_conn, "papers", corpus_ids, worker_logger, start_id)
            query_elapsed = time.time() - query_start
            worker_logger.info(f"Worker-{worker_id} [Step2.1-QueryPapers] batch={start_id}, count={len(papers_dict)}, time={query_elapsed:.3f}s")
            
            # 2.2 查询abstracts表（从本地SSD查询，已迁移）
            query_start = time.time()
            worker_logger.info(f"Worker-{worker_id} [Step2.2-QueryAbstracts] ===== START ===== batch={start_id}, corpus_ids={len(corpus_ids)}")
            abstracts_dict = batch_query_table(local_conn, "abstracts", corpus_ids, worker_logger, start_id)
            query_elapsed = time.time() - query_start
            worker_logger.info(f"Worker-{worker_id} [Step2.2-QueryAbstracts] ===== END ===== batch={start_id}, count={len(abstracts_dict)}, time={query_elapsed:.3f}s")
            
            # 2.3 查询tldrs表
            query_start = time.time()
            tldrs_dict = batch_query_table(machine3_conn, "tldrs", corpus_ids, worker_logger, start_id)
            query_elapsed = time.time() - query_start
            worker_logger.info(f"Worker-{worker_id} [Step2.3-QueryTldrs] batch={start_id}, count={len(tldrs_dict)}, time={query_elapsed:.3f}s")
            
            step2_elapsed = time.time() - step2_start
            worker_logger.info(f"Worker-{worker_id} [Step2-QueryBaseTables-TOTAL] batch={start_id}, time={step2_elapsed:.3f}s")
            
            # 3. 合并基础数据（容错处理）
            step_start = time.time()
            merged_results, corrupted_count, failed_corpus_ids = merge_base_data(
                corpus_ids, papers_dict, abstracts_dict, tldrs_dict
            )
            step_elapsed = time.time() - step_start
            worker_logger.info(f"Worker-{worker_id} [Step3-MergeBaseData] batch={start_id}, corrupted={corrupted_count}, time={step_elapsed:.3f}s")
            
            # 记录失败的corpusid
            if failed_corpus_ids:
                failed_file = os.path.join(os.path.dirname(__file__), 'failed_corposid')
                with open(failed_file, 'a', encoding='utf-8') as f:
                    for corpusid in failed_corpus_ids:
                        f.write(f"{corpusid}\n")
            
            # 4. 收集关联ID
            step_start = time.time()
            worker_logger.info(f"Worker-{worker_id} [Step4-CollectRelatedIDs] ===== START =====")
            author_ids = collect_author_ids(merged_results, worker_logger, start_id)
            venue_ids = collect_venue_ids(merged_results)
            step_elapsed = time.time() - step_start
            worker_logger.info(f"Worker-{worker_id} [Step4-CollectRelatedIDs] batch={start_id}, authors={len(author_ids)}, venues={len(venue_ids)}, time={step_elapsed:.3f}s")
            
            # 5. 批量查询关联表 (2次查询)
            step5_start = time.time()
            
            # 5.1 查询authors表（从本地SSD查询，已迁移）
            query_start = time.time()
            worker_logger.info(f"Worker-{worker_id} [Step5.1-QueryAuthors] ===== START ===== batch={start_id}, author_count={len(author_ids)}")
            authors_dict = batch_query_authors(local_conn, list(author_ids), worker_logger, start_id)
            query_elapsed = time.time() - query_start
            worker_logger.info(f"Worker-{worker_id} [Step5.1-QueryAuthors] ===== END ===== batch={start_id}, result_count={len(authors_dict)}, time={query_elapsed:.3f}s")
            
            # 5.2 查询venues表
            query_start = time.time()
            venues_dict = batch_query_venues(machine3_conn, list(venue_ids))
            query_elapsed = time.time() - query_start
            worker_logger.info(f"Worker-{worker_id} [Step5.2-QueryVenues] batch={start_id}, count={len(venues_dict)}, time={query_elapsed:.3f}s")
            
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
            # 只在start_id已定义时发送失败信号
            if start_id is not None:
                try:
                    result_queue.put((start_id, None, None), timeout=5)
                except:
                    pass
    
    # 关闭连接
    local_conn.close()
    machine3_conn.close()
    worker_logger.info(f"Worker-{worker_id} stopped")

# =============================================================================
# Writer进程（文件写入）
# =============================================================================

def writer_process(
    result_queue: Queue,
    progress_queue: Queue,
    output_dir: str
):
    """
    Writer进程：串行写入文件（避免USB硬盘并发瓶颈）
    
    Args:
        result_queue: 结果队列（接收合并后的数据）
        progress_queue: 进度队列（发送写入完成的信号）
        output_dir: 输出目录
    """
    # Writer日志（根据配置决定是否写入文件）
    writer_logger = setup_logger('Writer', LOG_FILE, console_output=False, enable_file=ENABLE_FILE_LOGGING)
    writer_logger.info("Writer process started")
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    while True:
        try:
            # 从结果队列获取数据
            result = result_queue.get(timeout=10)
            
            if result is None:  # 结束信号
                writer_logger.info("Writer received stop signal")
                break
            
            start_id, corpus_ids, merged_results = result
            writer_logger.info(f"Writer ========== START Writing Batch {start_id} ==========")
            
            if corpus_ids is None or merged_results is None:
                writer_logger.warning(f"Skipping empty result for start_id={start_id}")
                progress_queue.put((start_id, False))
                continue
            
            # Writer步骤1: 写入JSONL文件
            write_start_time = time.time()
            
            end_id = start_id + len(corpus_ids) - 1
            filename = f"batch_{start_id}_{end_id}.jsonl"
            filepath = os.path.join(output_dir, filename)
            
            try:
                # 文件写入计时
                file_write_start = time.time()
                with open(filepath, 'w', encoding='utf-8', buffering=16777216) as f:  # 16MB缓冲
                    for corpusid in corpus_ids:
                        if corpusid not in merged_results:
                            raise Exception(f"Missing data for corpusid {corpusid}")
                        
                        json_line = json.dumps(merged_results[corpusid], ensure_ascii=False)
                        f.write(json_line + '\n')
                file_write_elapsed = time.time() - file_write_start
                
                # 检查文件大小
                file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                write_speed_mbs = file_size_mb / file_write_elapsed if file_write_elapsed > 0 else 0
                
                writer_logger.info(
                    f"Writer [Step1-WriteFile] batch={start_id}, file={filename}, "
                    f"records={len(corpus_ids)}, size={file_size_mb:.2f}MB, "
                    f"time={file_write_elapsed:.3f}s, speed={write_speed_mbs:.2f}MB/s"
                )
                
                # Writer步骤2: 通知完成
                notify_start = time.time()
                progress_queue.put((start_id, True, filename))
                notify_elapsed = time.time() - notify_start
                writer_logger.info(f"Writer [Step2-NotifyComplete] batch={start_id}, time={notify_elapsed:.3f}s")
                
                # 总计
                total_elapsed = time.time() - write_start_time
                writer_logger.info(
                    f"Writer ========== COMPLETE Batch {start_id}: "
                    f"{len(corpus_ids)} records in {total_elapsed:.3f}s =========="
                )
                
            except Exception as e:
                writer_logger.error(f"Failed to write {filename}: {e}")
                # 删除失败的文件
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
    """打印进度信息（单行刷新）"""
    # 防御性检查
    if total <= 0:
        return
    
    remaining = total - processed
    remaining_time = remaining / speed if speed > 0 else 0
    progress_pct = (processed / total) * 100
    
    # 格式化时间
    elapsed_hours = elapsed / 3600
    remaining_hours = remaining_time / 3600
    
    # 单行输出，使用\r覆盖
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
    print("S2ORC数据合并导出 - 并行处理模式")
    print("="*80)
    print(f"Worker进程数: {NUM_WORKERS}")
    print(f"批次大小: {BATCH_SIZE:,} 条/批 (~5.5GB/文件)")
    print(f"目标总量: {TARGET_TOTAL:,} 条")
    print(f"输出目录: {OUTPUT_DIR}")
    print("="*80)
    print()
    
    # 初始化阶段
    init_start = time.time()
    logger.info("========== [INIT] 初始化阶段开始 ==========")
    
    # 建立主进程的数据库连接
    try:
        db_conn_start = time.time()
        local_config = get_db_config('machine0')
        local_conn = psycopg2.connect(**local_config)
        db_conn_elapsed = time.time() - db_conn_start
        print(f"✓ 已连接到数据库: {local_config['database']}")
        logger.info(f"[INIT-DBConnect] time={db_conn_elapsed:.3f}s")
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}")
        return
    
    # 创建进度表
    create_table_start = time.time()
    create_progress_table(local_conn)
    create_table_elapsed = time.time() - create_table_start
    logger.info(f"[INIT-CreateProgressTable] time={create_table_elapsed:.3f}s")
    
    init_elapsed = time.time() - init_start
    logger.info(f"========== [INIT] 初始化阶段完成: {init_elapsed:.3f}s ==========")
    
    # 进度恢复阶段
    progress_restore_start = time.time()
    logger.info("========== [PROGRESS-RESTORE] 进度恢复阶段开始 ==========")
    
    last_progress = get_last_progress(local_conn)
    
    if last_progress:
        start_id = last_progress['start_id'] + last_progress['batch_size']
        # 计算已经处理的数量（从1开始到start_id之前）
        already_processed = start_id - 1
        print(f"✓ 断点续传: 从 start_id={start_id:,} 继续")
        print(f"  已完成: {already_processed:,} 条 ({already_processed/TARGET_TOTAL*100:.1f}%)")
        logger.info(f"[PROGRESS-RESTORE] 断点续传: start_id={start_id}, already_processed={already_processed}")
    else:
        start_id = 1
        already_processed = 0
        print(f"✓ 首次运行: 从头开始处理")
        logger.info(f"[PROGRESS-RESTORE] 首次运行")
    
    progress_restore_elapsed = time.time() - progress_restore_start
    logger.info(f"========== [PROGRESS-RESTORE] 进度恢复完成: {progress_restore_elapsed:.3f}s ==========")
    print()
    
    # 创建队列阶段
    queue_create_start = time.time()
    logger.info("========== [QUEUE-CREATE] 创建队列阶段开始 ==========")
    
    manager = Manager()
    task_queue = Queue(maxsize=TASK_QUEUE_SIZE)
    result_queue = Queue(maxsize=RESULT_QUEUE_SIZE)
    progress_queue = Queue()
    
    # 共享统计数据
    stats = manager.dict()
    stats['processed'] = 0
    stats['batches'] = 0
    stats_lock = Lock()
    
    queue_create_elapsed = time.time() - queue_create_start
    logger.info(f"[QUEUE-CREATE] task_queue_size={TASK_QUEUE_SIZE}, result_queue_size={RESULT_QUEUE_SIZE}")
    logger.info(f"========== [QUEUE-CREATE] 创建队列完成: {queue_create_elapsed:.3f}s ==========")
    
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
    
    # 主循环：分配任务和管理进度
    current_id = start_id
    total_processed = already_processed  # 从已处理的数量开始，而不是从0开始
    overall_start_time = time.time()
    pending_batches = {}  # {start_id: batch_size}
    last_update_time = time.time()
    
    # 如果是断点续传，显示初始进度
    if already_processed > 0:
        print_progress(total_processed, TARGET_TOTAL, 0, 0, 0, 0)
    
    try:
        while total_processed < TARGET_TOTAL:
            # 1. 分配任务到Worker（避免超过目标）
            dispatch_start = time.time()
            tasks_dispatched = 0
            while len(pending_batches) < TASK_QUEUE_SIZE:
                # 检查是否会超过目标（从1开始到TARGET_TOTAL）
                if current_id >= 1 + TARGET_TOTAL:
                    break
                
                try:
                    task_queue.put((current_id, BATCH_SIZE), timeout=1)
                    pending_batches[current_id] = BATCH_SIZE
                    current_id += BATCH_SIZE
                    tasks_dispatched += 1
                except Exception as e:
                    break
            
            if tasks_dispatched > 0:
                dispatch_elapsed = time.time() - dispatch_start
                logger.info(f"[MainLoop-DispatchTasks] dispatched={tasks_dispatched}, pending={len(pending_batches)}, time={dispatch_elapsed:.3f}s")
            
            # 2. 接收Writer完成的进度
            try:
                receive_start = time.time()
                progress_result = progress_queue.get(timeout=1)
                receive_elapsed = time.time() - receive_start
                
                if len(progress_result) == 2:
                    batch_start_id, success = progress_result
                    filename = None
                else:
                    batch_start_id, success, filename = progress_result
                
                if success and filename:
                    # 记录进度到数据库
                    record_start = time.time()
                    record_progress(local_conn, batch_start_id, BATCH_SIZE, filename)
                    record_elapsed = time.time() - record_start
                    
                    # 更新统计
                    batch_size = pending_batches.pop(batch_start_id, BATCH_SIZE)
                    total_processed += batch_size
                    
                    # 实时显示进度（单行刷新）
                    elapsed = time.time() - overall_start_time
                    # 计算本次运行的速度（不包含之前已处理的部分）
                    current_session_processed = total_processed - already_processed
                    speed = current_session_processed / elapsed if elapsed > 0 else 0
                    print_progress(total_processed, TARGET_TOTAL, speed, elapsed,
                                 len(pending_batches), result_queue.qsize())
                    
                    logger.info(
                        f"[MainLoop-ReceiveProgress] batch={batch_start_id}, "
                        f"receive_time={receive_elapsed:.3f}s, record_time={record_elapsed:.3f}s, "
                        f"total_processed={total_processed}, speed={speed:.1f}rec/s"
                    )
                    
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
        # 停止所有进程
        print("\n正在停止所有进程...")
        
        # 发送停止信号给Workers
        for _ in range(NUM_WORKERS):
            task_queue.put(None)
        
        # 等待Workers结束
        for p in workers:
            p.join(timeout=10)
            if p.is_alive():
                p.terminate()
        
        # 停止Writer
        result_queue.put(None)
        writer.join(timeout=10)
        if writer.is_alive():
            writer.terminate()
        
        # 关闭数据库连接
        local_conn.close()
        
        # 最终统计
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
        if ENABLE_FILE_LOGGING:
            print(f"详细日志: {LOG_FILE}")
        else:
            print(f"日志记录: 已禁用（仅控制台输出）")
        print("="*80)


if __name__ == '__main__':
    main()

