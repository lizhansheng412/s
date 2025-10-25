#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
极致优化版 - GZ文件流式批量插入
性能优化：
  ✅ 生产者-消费者模式：解压和插入完全分离并行
  ✅ 正则快速提取主键字段：避免完整JSON解析
  ✅ 更大批次：50000条/批次，减少数据库往返
  ✅ 队列缓冲：解压快时不等待，插入快时不空闲
  ✅ 断点续传：支持中断恢复
  ✅ 灵活主键配置：不同表使用不同主键字段
     - authors表: authorid (JSON字符串→DB BIGINT)
     - citations表: id（自增），额外字段citingcorpusid（允许重复）
     - publication_venues表: publicationvenueid (JSON字符串→DB TEXT，值从id提取)
     - 其他表: corpusid (JSON数字→DB BIGINT)
  
目标：10倍性能提升（3000-5000条/秒）
"""

import gzip
import re
import sys
import time
import logging
from pathlib import Path
from typing import Optional, Set
from multiprocessing import Process, Queue, Manager, cpu_count
from queue import Empty

import psycopg2

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.config import db_config_v2
from database.config.db_config_v2 import FIELD_TABLES

# =============================================================================
# 根据数据大小动态配置（关键优化！）
# =============================================================================

# 针对TEXT类型优化配置（彻底避免缓冲区问题：不使用临时表，使用VALUES）
TABLE_CONFIGS = {
    # 超大数据 (60-120KB/条): s2orc系列 
    's2orc': {'batch_size': 1000, 'commit_batches': 3, 'extractors': 1},
    's2orc_v2': {'batch_size': 1000, 'commit_batches': 3, 'extractors': 1},
    # 1000条×100KB×3批=300MB/批次
        
    # 中等数据 (16KB/条): embeddings系列 
    'embeddings_specter_v1': {'batch_size': 6250, 'commit_batches': 3, 'extractors': 1},
    'embeddings_specter_v2': {'batch_size': 6250, 'commit_batches': 3, 'extractors': 1},
    # 6250条×16KB×3批=300MB/批次
    
    # 小数据 (1-3KB/条): 其他表
    'papers': {'batch_size': 50000, 'commit_batches': 3, 'extractors': 2},
    'abstracts': {'batch_size': 50000, 'commit_batches': 3, 'extractors': 2},
    'authors': {'batch_size': 50000, 'commit_batches': 3, 'extractors': 2},
    'citations': {'batch_size': 50000, 'commit_batches': 3, 'extractors': 1},
    'publication_venues': {'batch_size': 50000, 'commit_batches': 3, 'extractors': 2},
    'tldrs': {'batch_size': 50000, 'commit_batches': 3, 'extractors': 2},
    # 50,000条×2KB×3批=300MB/批次
    
    # 极速模式：paper_ids（只有一个BIGINT字段，8字节/条）
    # 大数据量优化：批次增大以分摊索引维护开销
    'paper_ids': {'batch_size': 1000000, 'commit_batches': 1, 'extractors': 3},
    # 100万条×8字节×1批=8MB/批次（极小，极速，减少commit次数）
}

DEFAULT_CONFIG = {'batch_size': 20000, 'commit_batches': 1, 'extractors': 1}
NUM_EXTRACTORS = 1
QUEUE_SIZE = 20
# 日志文件路径（将根据表名动态生成）
PROGRESS_DIR = 'logs/progress'
FAILED_DIR = 'logs/failed'
ALWAYS_FAILED_DIR = 'logs/always_failed'

# 不同表使用不同的主键字段（数据库字段名）
TABLE_PRIMARY_KEY_MAP = {
    'authors': 'authorid',
    'publication_venues': 'publicationvenueid',
    # citations表使用自增主键，无需在此配置
    # 其他表默认使用corpusid
}

# JSON中主键字段是字符串格式的表（需要用引号匹配正则表达式）
JSON_STRING_KEY_TABLES = {
    'authors',  # JSON: "authorid":"5232161" (带引号)，DB: BIGINT (需转换)
    'publication_venues'  # JSON: "id":"uuid-..." (带引号)，DB: TEXT (保持字符串)
}

# 只需插入主键的表（无data字段）
NO_DATA_TABLES = {
    'paper_ids'  # 只插入corpusid，其他字段使用默认值
}

# 数据库中主键是TEXT类型的表（插入时不需要转换）
DB_TEXT_KEY_TABLES = {
    'publication_venues'  # publicationvenueid是TEXT类型（UUID字符串）
}

# JSON字段名到数据库字段名的映射（用于正则提取）
JSON_FIELD_MAP = {
    'citations': 'citingcorpusid',  # 从JSON的citingcorpusid字段提取值
    'publication_venues': 'id',  # JSON中的id字段对应数据库的publicationvenueid
    # 其他表的JSON字段名和数据库字段名一致，使用数据库字段名即可
}

# 正则表达式：快速提取主键字段（比完整JSON解析快10倍）
def get_key_pattern(field_name: str, is_uuid: bool = False):
    """
    根据字段名生成正则表达式
    
    Args:
        field_name: JSON字段名
        is_uuid: 是否是UUID类型（字符串），否则是数字类型
    """
    if is_uuid:
        # UUID格式: "id": "0b0cbb6c-54d7-4989-9265-abb19476957d"
        return re.compile(rf'"{field_name}"\s*:\s*"([0-9a-f\-]+)"', re.IGNORECASE)
    else:
        # 数字格式: "corpusid": 12345
        return re.compile(rf'"{field_name}"\s*:\s*(\d+)', re.IGNORECASE)

def get_log_files(table_name: str):
    """
    根据表名获取专属的日志文件路径
    
    Args:
        table_name: 表名
    
    Returns:
        (progress_file, failed_file, always_failed_file) 元组
    """
    progress_dir = Path(PROGRESS_DIR)
    failed_dir = Path(FAILED_DIR)
    always_failed_dir = Path(ALWAYS_FAILED_DIR)
    
    # 创建目录
    progress_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)
    always_failed_dir.mkdir(parents=True, exist_ok=True)
    
    progress_file = progress_dir / f"{table_name}_progress.txt"
    failed_file = failed_dir / f"{table_name}_failed.txt"
    always_failed_file = always_failed_dir / f"{table_name}_failed.txt"
    
    return str(progress_file), str(failed_file), str(always_failed_file)

# 设置日志级别为ERROR，只显示错误信息
logging.basicConfig(
    level=logging.ERROR,
    format='%(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # 主进程可以INFO


# =============================================================================
# 断点续传
# =============================================================================

class ProgressTracker:
    """进度跟踪器"""
    
    def __init__(self, progress_file: str):
        self.progress_file = Path(progress_file)
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
    
    def load_completed(self) -> Set[str]:
        if not self.progress_file.exists():
            return set()
        
        completed = set()
        with open(self.progress_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    completed.add(line)
        
        return completed
    
    def mark_completed(self, file_name: str):
        with open(self.progress_file, 'a', encoding='utf-8') as f:
            f.write(f"{file_name}\n")
            f.flush()
    
    def reset(self):
        if self.progress_file.exists():
            self.progress_file.unlink()


class FailedFilesLogger:
    """失败文件记录器（支持 failed 和 always_failed）"""
    
    def __init__(self, failed_file: str, always_failed_file: str = None, is_retry: bool = False):
        self.failed_file = Path(failed_file)
        self.always_failed_file = Path(always_failed_file) if always_failed_file else None
        self.is_retry = is_retry
        self.failed_file.parent.mkdir(parents=True, exist_ok=True)
        if self.always_failed_file:
            self.always_failed_file.parent.mkdir(parents=True, exist_ok=True)
    
    def load_failed(self) -> Set[str]:
        """加载已知失败的文件列表"""
        if not self.failed_file.exists():
            return set()
        
        failed = set()
        with open(self.failed_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split('|')
                    if len(parts) >= 2:
                        failed.add(parts[1].strip())
        
        return failed
    
    def log_failed(self, file_name: str, error: str):
        """记录失败文件"""
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # 重试模式：失败记录到 always_failed
            if self.is_retry and self.always_failed_file:
                with open(self.always_failed_file, 'a', encoding='utf-8') as f:
                    f.write(f"{timestamp} | {file_name} | {error}\n")
                    f.flush()
            else:
                # 首次处理：记录到 failed
                with open(self.failed_file, 'a', encoding='utf-8') as f:
                    f.write(f"{timestamp} | {file_name} | {error}\n")
                    f.flush()
        except Exception as e:
            logger.error(f"记录失败文件日志时出错: {e}")
    
    def remove_from_failed(self, file_name: str):
        """从 failed 中删除记录（重试成功时调用）"""
        if not self.failed_file.exists():
            return
        
        try:
            lines = []
            with open(self.failed_file, 'r', encoding='utf-8') as f:
                for line in f:
                    # 精确匹配文件名（格式：时间戳 | 文件名 | 错误信息）
                    parts = line.split('|')
                    if len(parts) >= 2 and parts[1].strip() != file_name:
                        lines.append(line)
            
            # 写回文件
            with open(self.failed_file, 'w', encoding='utf-8') as f:
                f.writelines(lines)
        except Exception as e:
            # 删除失败不影响主流程，记录日志即可
            logger.warning(f"从 failed 日志中删除记录失败: {e}")
    
    def reset(self):
        if self.failed_file.exists():
            self.failed_file.unlink()


# =============================================================================
# 生产者：解压进程（多个并行）
# =============================================================================

def extractor_worker(
    file_queue: Queue,
    data_queue: Queue,
    stats_dict: dict,
    table_name: str,
    batch_size: int = 10000
):
    """
    解压工作进程（生产者）
    从file_queue取文件，解压后放入data_queue
    """
    # 完全禁用此进程的日志输出
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    
    # 根据表名确定主键字段（数据库字段名）
    primary_key_field = TABLE_PRIMARY_KEY_MAP.get(table_name, 'corpusid')
    # 确定JSON中对应的字段名（用于正则提取）
    json_field_name = JSON_FIELD_MAP.get(table_name, primary_key_field)
    # 判断JSON中主键是否是字符串格式（用于正则匹配）
    is_json_string = table_name in JSON_STRING_KEY_TABLES
    # 判断数据库中主键是否是TEXT类型（用于决定是否转换）
    is_db_text = table_name in DB_TEXT_KEY_TABLES
    key_pattern = get_key_pattern(json_field_name, is_uuid=is_json_string)
    
    worker_name = f"Extractor-{id(file_queue) % 1000}"
    
    while True:
        try:
            # 获取文件任务
            task = file_queue.get(timeout=1)
            if task is None:  # 结束信号
                break
            
            gz_file_path, file_name = task
            start_time = time.time()
            
            try:
                batch = []
                line_count = 0
                valid_count = 0
                
                # 流式解压 - 遇到损坏直接跳过，不重试
                # USB盘优化：使用二进制模式+更大缓冲区，减少随机I/O
                try:
                    import io
                    # 使用二进制模式打开，设置8MB缓冲区
                    with gzip.open(gz_file_path, 'rb') as f_binary:
                        # 包装为带大缓冲区的文本流
                        f = io.TextIOWrapper(io.BufferedReader(f_binary, buffer_size=8*1024*1024), 
                                            encoding='utf-8', errors='ignore')
                        for line in f:
                            line_count += 1
                            line = line.strip()
                            if not line:
                                continue
                            
                            # 正则快速提取主键字段（避免完整JSON解析）
                            match = key_pattern.search(line)
                            if not match:
                                continue
                            
                            # 提取主键值
                            # - 如果DB是TEXT类型，保持字符串（如publication_venues）
                            # - 否则转换为int（如authors的authorid虽然JSON是字符串，但DB是BIGINT）
                            key_value = match.group(1) if is_db_text else int(match.group(1))
                            valid_count += 1
                            
                            # 优化：直接使用原始行，避免不必要的转义检查
                            # PostgreSQL COPY可以处理大多数JSON字符
                            batch.append((key_value, line))
                            
                            # 批次满了，发送到队列
                            if len(batch) >= batch_size:
                                data_queue.put(('data', file_name, batch))
                                batch = []
                    
                    # 发送剩余数据
                    if batch:
                        data_queue.put(('data', file_name, batch))
                    
                    # 发送文件完成信号
                    data_queue.put(('done', file_name, valid_count))
                    
                    # 更新统计
                    stats_dict['extracted'] = stats_dict.get('extracted', 0) + valid_count
                    
                except (OSError, EOFError, ValueError) as gz_error:
                    # GZIP文件损坏，直接跳过，不重试
                    data_queue.put(('error', file_name, f"Corrupted"))
                    continue
                
            except Exception as e:
                data_queue.put(('error', file_name, str(e)))
        
        except Empty:
            continue
        except Exception:
            break


# =============================================================================
# 消费者：插入进程（单个，高效批量插入）
# =============================================================================

def inserter_worker(
    data_queue: Queue,
    table_name: str,
    stats_dict: dict,
    tracker: ProgressTracker,
    failed_logger: FailedFilesLogger,
    use_upsert: bool = False,
    commit_batches: int = 3,
    total_files: int = 0,
    primary_key: str = 'corpusid',
    is_retry: bool = False,
    initial_completed: int = 0,
    db_config: dict = None
):
    """
    插入工作进程（消费者）
    持续从data_queue取数据并批量插入
    
    Args:
        primary_key: 主键字段名（默认corpusid）
        db_config: 数据库配置字典
    """
    try:
        # 创建数据库连接
        if db_config is None:
            db_config = db_config_v2.DB_CONFIG
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        cursor = conn.cursor()
        
        # 性能优化配置（分离读写路径后的优化配置，充分利用8核32GB）
        try:
            cursor.execute("SET synchronous_commit = OFF")  # 异步提交（关键优化）
            cursor.execute("SET commit_delay = 100000")  # 延迟提交100ms
            cursor.execute("SET maintenance_work_mem = '2GB'")  # 维护内存（适中，避免脏页积压）
            cursor.execute("SET work_mem = '512MB'")  # 工作内存（适中）
            cursor.execute("SET temp_buffers = '4GB'")  # 临时缓冲区
            cursor.execute("SET effective_cache_size = '16GB'")  # 缓存大小
            cursor.execute("SET max_parallel_workers_per_gather = 0")  # 关闭并行
            # 临时表空间使用D盘（前提：已创建d1_temp表空间）
            try:
                cursor.execute("SET temp_tablespaces = 'd1_temp'")
            except:
                pass  # 如果表空间不存在，忽略
        except Exception as e:
            conn.rollback()
            logger.warning(f"部分性能配置失败（可忽略）: {e}")
        
        # 这些参数需要在postgresql.conf中设置，不能在会话级别修改，注释掉避免警告
        # bgwriter_delay = 1000ms  # 后台写入延迟（减少随机写）
        # checkpoint_timeout = 30min  # 检查点间隔（减少刷盘）
        # checkpoint_completion_target = 0.9  # 检查点完成目标
        
        # WAL设置（可能失败，单独处理）
        try:
            cursor.execute("SET wal_writer_delay = '1000ms'")
        except Exception:
            conn.rollback()  # 回滚并继续
        
        # 禁用触发器（INSERT模式）
        if not use_upsert:
            try:
                cursor.execute(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL")
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.warning(f"禁用触发器失败（可忽略）: {e}")
        
        total_inserted = 0
        file_stats = {}  # {file_name: inserted_count}
        completed_files = 0  # 当前批次完成的文件数
        last_log_time = time.time()
        start_time = time.time()
        batch_count = 0  # 批次计数器
        
        while True:
            try:
                # 非阻塞获取，避免长时间等待
                item = data_queue.get(timeout=5)
                
                item_type = item[0]
                
                if item_type == 'stop':
                    break
                
                elif item_type == 'data':
                    _, file_name, batch = item
                    
                    try:
                        # 批量插入（传入主键字段）
                        inserted = batch_insert_copy(cursor, table_name, batch, use_upsert, primary_key)
                        batch_count += 1
                        
                        # 每N个批次commit一次，减少commit开销
                        if batch_count >= commit_batches:
                            conn.commit()
                            batch_count = 0
                        
                        total_inserted += inserted
                        file_stats[file_name] = file_stats.get(file_name, 0) + inserted
                    
                    except Exception as insert_error:
                        # 插入失败，回滚当前事务
                        conn.rollback()
                        batch_count = 0  # 重置批次计数
                        logger.error(f"批量插入失败（已回滚）: {insert_error}")
                        # 跳过这个批次，继续处理下一个
                        continue
                    
                    # 定期输出进度（每3秒）
                    current_time = time.time()
                    if current_time - last_log_time >= 3:
                        elapsed = current_time - start_time
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        
                        # 总进度（包含初始已完成的文件）
                        total_completed_now = initial_completed + completed_files
                        total_all_files = initial_completed + total_files
                        overall_progress = (total_completed_now / total_all_files * 100) if total_all_files > 0 else 0
                        
                        # 估算剩余时间
                        if completed_files > 0:
                            avg_time_per_file = elapsed / completed_files
                            remaining_files = total_files - completed_files
                            eta_seconds = remaining_files * avg_time_per_file
                            eta_hours = int(eta_seconds / 3600)
                            eta_mins = int((eta_seconds % 3600) / 60)
                            eta_str = f"{eta_hours}h{eta_mins}m" if eta_hours > 0 else f"{eta_mins}m"
                        else:
                            eta_str = "calculating..."
                        
                        # 显示：总进度（包含初始已完成的）
                        print(f"\r[{total_completed_now}/{total_all_files}] {overall_progress:.1f}% | "
                              f"{total_inserted:,} rows | {rate:.0f}/s | "
                              f"ETA: {eta_str}    ", end='', flush=True)
                        last_log_time = current_time
                
                elif item_type == 'done':
                    _, file_name, _ = item
                    # 标记文件完成
                    tracker.mark_completed(file_name)
                    # 重试成功：从 failed 中删除
                    if is_retry:
                        failed_logger.remove_from_failed(file_name)
                    completed_files += 1
                    inserted = file_stats.get(file_name, 0)
                
                elif item_type == 'error':
                    _, file_name, error = item
                    # 记录失败文件
                    failed_logger.log_failed(file_name, error)
                    completed_files += 1
            
            except Empty:
                # 队列空，继续等待
                continue
            except Exception as e:
                logger.error(f"[Inserter] 处理数据异常: {e}")
                conn.rollback()
                continue
        
        # 提交剩余未commit的数据
        if batch_count > 0:
            conn.commit()
        
        # 恢复表状态
        if not use_upsert:
            # 启用触发器
            cursor.execute(f"ALTER TABLE {table_name} ENABLE TRIGGER ALL")
            conn.commit()
        
        cursor.close()
        conn.close()
        
        stats_dict['inserted'] = total_inserted
        
    except Exception as e:
        logger.error(f"[Inserter] 严重错误: {e}")
        import traceback
        traceback.print_exc()


def clean_utf8(text: str) -> str:
    """清理字符串中的无效 UTF-8 字符"""
    if not text:
        return text
    # 编码为 UTF-8，替换无效字符，再解码回来
    return text.encode('utf-8', errors='replace').decode('utf-8', errors='replace')


def batch_insert_copy(cursor, table_name: str, batch: list, use_upsert: bool = False, primary_key: str = 'corpusid') -> int:
    """
    使用COPY批量插入（最快方法）
    
    数据以TEXT格式存储（不验证不解析，极速）
    
    注意：
    - paper_ids表：只插入corpusid字段，无data字段（极速模式）
    - authors表：主键列名是authorid (BIGINT)
    - citations表：主键是id (BIGSERIAL自增)，citingcorpusid为普通字段
    - publication_venues表：主键列名是publicationvenueid (TEXT)，值从id提取（UUID字符串）
    - 其他表：主键列名是corpusid (BIGINT)
    
    Args:
        cursor: 数据库游标
        table_name: 表名
        batch: 数据批次 [(key_value, json_line), ...] - key_value已从JSON正确字段提取
        use_upsert: 是否使用UPSERT模式
        primary_key: 主键列名（authorid/publicationvenueid/corpusid，citations表无需）
    """
    if not batch:
        return 0
    
    from io import StringIO
    import psycopg2.errors
    
    # 检查数据库主键是否是TEXT类型（用于VALUES语句格式化）
    is_string_key = table_name in DB_TEXT_KEY_TABLES
    
    try:
        # paper_ids表特殊处理：只插入corpusid，无其他字段（极速模式）
        if table_name == 'paper_ids':
            # 批内去重（只保留唯一corpusid）
            unique_ids = list(set(key_val for key_val, _ in batch))
            
            buffer = StringIO()
            # 只插入corpusid，无时间戳字段（极速优化）
            buffer.write(''.join(f"{cid}\n" for cid in unique_ids))
            buffer.seek(0)
            
            try:
                # 只插入corpusid字段，其他字段使用默认值
                cursor.copy_expert(
                    f"COPY {table_name} (corpusid) FROM STDIN",
                    buffer
                )
                return len(unique_ids)
            except psycopg2.errors.UniqueViolation:
                # 有重复key，预查询过滤（断点续传场景）
                cursor.connection.rollback()
                
                # 批量查询已存在的ID（分批查询，避免占用过多内存）
                chunk_size = 100000
                existing_ids = set()
                
                for i in range(0, len(unique_ids), chunk_size):
                    chunk = unique_ids[i:i + chunk_size]
                    placeholders = ','.join(['%s'] * len(chunk))
                    cursor.execute(
                        f"SELECT corpusid FROM {table_name} WHERE corpusid IN ({placeholders})",
                        chunk
                    )
                    existing_ids.update(row[0] for row in cursor.fetchall())
                
                # 过滤并插入新数据
                new_ids = [cid for cid in unique_ids if cid not in existing_ids]
                if not new_ids:
                    return 0
                
                buffer = StringIO()
                buffer.write(''.join(f"{cid}\n" for cid in new_ids))
                buffer.seek(0)
                
                try:
                    cursor.copy_expert(
                        f"COPY {table_name} (corpusid) FROM STDIN",
                        buffer
                    )
                    return len(new_ids)
                except Exception:
                    cursor.connection.rollback()
                    return 0
        
        # 以下是其他表的原有逻辑
        elif use_upsert:
            # UPSERT模式：批内去重 + 分批插入
            seen = {key_value: clean_utf8(data) for key_value, data in batch}
            chunk_size = 1000
            total_processed = 0
            
            for i in range(0, len(seen), chunk_size):
                chunk = list(seen.items())[i:i + chunk_size]
                
                # 构建VALUES子句（字符串主键需加引号）
                values_list = []
                for k, v in chunk:
                    escaped_data = v.replace("'", "''")
                    if is_string_key:
                        escaped_key = str(k).replace("'", "''")
                        values_list.append(f"('{escaped_key}', '{escaped_data}', NOW(), NOW())")
                    else:
                        values_list.append(f"({k}, '{escaped_data}', NOW(), NOW())")
                values = ','.join(values_list)
                
                cursor.execute(f"""
                    INSERT INTO {table_name} ({primary_key}, data, insert_time, update_time)
                    VALUES {values}
                    ON CONFLICT ({primary_key}) DO UPDATE SET
                        data = EXCLUDED.data, update_time = EXCLUDED.update_time
                """)
                total_processed += len(chunk)
            
            return total_processed
        else:
            # INSERT模式：极速COPY（TEXT类型，不验证不解析，最快）
            
            # citations表：自增主键，直接插入citingcorpusid字段
            if table_name == 'citations':
                buffer = StringIO()
                buffer.write(''.join(f"{key_val}\t{clean_utf8(data)}\t\\N\t\\N\n" for key_val, data in batch))
                buffer.seek(0)
                
                cursor.copy_expert(
                    f"COPY {table_name} (citingcorpusid, data, insert_time, update_time) "
                    f"FROM STDIN WITH (FORMAT TEXT, NULL '\\N', DELIMITER E'\\t')",
                    buffer
                )
                return len(batch)
            
            # 其他表：主键唯一，处理冲突
            buffer = StringIO()
            buffer.write(''.join(f"{key_val}\t{clean_utf8(data)}\t\\N\t\\N\n" for key_val, data in batch))
            buffer.seek(0)
            
            try:
                cursor.copy_expert(
                    f"COPY {table_name} ({primary_key}, data, insert_time, update_time) "
                    f"FROM STDIN WITH (FORMAT TEXT, NULL '\\N', DELIMITER E'\\t')",
                    buffer
                )
                return len(batch)
            except psycopg2.errors.UniqueViolation:
                # 有重复key，使用预查询过滤法
                cursor.connection.rollback()
                
                batch_ids = [key_val for key_val, _ in batch]
                if not batch_ids:
                    return 0
                
                # 批量查询已存在的ID
                placeholders = ','.join(['%s'] * len(batch_ids))
                cursor.execute(
                    f"SELECT {primary_key} FROM {table_name} WHERE {primary_key} IN ({placeholders})",
                    batch_ids
                )
                existing_ids = set(row[0] for row in cursor.fetchall())
                
                # 过滤并插入新数据
                new_batch = [(k, v) for k, v in batch if k not in existing_ids]
                if not new_batch:
                    return 0
                
                buffer = StringIO()
                buffer.write(''.join(f"{key_val}\t{clean_utf8(data)}\t\\N\t\\N\n" for key_val, data in new_batch))
                buffer.seek(0)
                
                try:
                    cursor.copy_expert(
                        f"COPY {table_name} ({primary_key}, data, insert_time, update_time) "
                        f"FROM STDIN WITH (FORMAT TEXT, NULL '\\N', DELIMITER E'\\t')",
                        buffer
                    )
                    return len(new_batch)
                except Exception:
                    cursor.connection.rollback()
                    return 0
        
    except Exception as e:
        import traceback
        logger.error(f"批量插入失败: {e}")
        logger.error(f"详细信息: {traceback.format_exc()}")
        raise


# =============================================================================
# 主协调器
# =============================================================================

def process_gz_folder_pipeline(
    folder_path: str,
    table_name: str,
    use_upsert: bool = False,
    num_extractors: int = NUM_EXTRACTORS,
    resume: bool = True,
    reset_progress: bool = False,
    retry_failed: bool = False,
    is_retry: bool = False
):
    """
    流水线并行处理：多个解压进程 + 单个插入进程
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise ValueError(f"文件夹不存在: {folder_path}")
    
    # 根据表名获取专属的日志文件路径
    progress_file, failed_file, always_failed_file = get_log_files(table_name)
    
    # 初始化进度跟踪（每个表独立的日志文件）
    tracker = ProgressTracker(progress_file)
    failed_logger = FailedFilesLogger(failed_file, always_failed_file, is_retry)
    
    if reset_progress:
        tracker.reset()
        failed_logger.reset()
    
    # 加载已完成和失败的文件
    completed_files = tracker.load_completed() if resume else set()
    failed_files = failed_logger.load_failed() if (resume and not retry_failed) else set()
    
    # 扫描GZ文件
    gz_files = sorted(folder.glob("*.gz"))
    if not gz_files:
        logger.warning(f"未找到.gz文件: {folder_path}")
        return
    
    total_gz_count = len(gz_files)
    
    # 过滤：排除已完成的文件，以及已知失败的文件（除非retry_failed=True）
    excluded_files = completed_files | failed_files
    pending_files = [(str(f), f.name) for f in gz_files if f.name not in excluded_files]
    
    # 统计信息
    completed_count = len(completed_files)
    failed_count = len(failed_files)
    
    # 获取该表使用的主键字段
    primary_key_field = TABLE_PRIMARY_KEY_MAP.get(table_name, 'corpusid')
    
    # 根据表名获取优化配置
    config = TABLE_CONFIGS.get(table_name, DEFAULT_CONFIG)
    batch_size = config['batch_size']
    commit_batches = config['commit_batches']
    # 如果用户没指定extractors，使用配置中的值
    if num_extractors == NUM_EXTRACTORS:  # 默认值
        num_extractors = config['extractors']
    
    # 精简输出：一行显示核心信息
    failed_info = f", failed: {failed_count}" if failed_files else ""
    logger.info(f"\n[{table_name}] Total: {total_gz_count}, Completed: {completed_count}{failed_info}, Pending: {len(pending_files)}")
    
    if not pending_files:
        logger.info("All files processed!\n")
        return
    
    overall_start = time.time()
    
    try:
        # 创建队列
        file_queue = Queue()
        data_queue = Queue(maxsize=QUEUE_SIZE)
        
        # 创建共享统计字典
        manager = Manager()
        stats_dict = manager.dict()
        
        # 添加文件任务
        for task in pending_files:
            file_queue.put(task)
        
        # 添加结束信号（每个解压进程一个）
        for _ in range(num_extractors):
            file_queue.put(None)
        
        # 启动插入进程（消费者）
        # 传入总文件数和已完成数，用于正确显示进度
        # 传入当前的 DB_CONFIG 确保子进程使用正确的数据库配置
        inserter = Process(
            target=inserter_worker,
            args=(data_queue, table_name, stats_dict, tracker, failed_logger, use_upsert, commit_batches, len(pending_files), primary_key_field, is_retry, completed_count, db_config_v2.DB_CONFIG.copy()),
            name='Inserter'
        )
        inserter.start()
        
        # 启动解压进程（生产者）
        extractors = []
        for i in range(num_extractors):
            p = Process(
                target=extractor_worker,
                args=(file_queue, data_queue, stats_dict, table_name, batch_size),
                name=f'Extractor-{i+1}'
            )
            p.start()
            extractors.append(p)
        
        # 等待所有解压进程完成
        for p in extractors:
            p.join()
        
        # 发送停止信号给插入进程
        data_queue.put(('stop', None, None))
        
        # 等待插入进程完成
        inserter.join()
        
        elapsed = time.time() - overall_start
        total_inserted = stats_dict.get('inserted', 0)
        avg_rate = total_inserted / elapsed if elapsed > 0 else 0
        
        logger.info(f"[{table_name}] DONE: {len(pending_files)} files, {total_inserted:,} rows, {elapsed/60:.1f} min, {avg_rate:.0f} rows/s\n")
        
    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user (progress saved)")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nProcessing failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# =============================================================================
# 主函数
# =============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='极致优化版 - GZ文件流式批量插入（目标：3000-5000条/秒）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
性能优化要点：
  🚀 生产者-消费者模式：解压和插入完全并行
  🚀 正则快速提取：避免完整JSON解析（提速10倍）
  🚀 更大批次：50000条/批次，减少数据库往返
  🚀 队列缓冲：持续供应数据，无空闲等待
  🚀 多进程解压：充分利用多核CPU
  🚀 灵活主键配置：根据表名自动使用正确的主键字段
     - authors表使用authorid
     - citations表使用自增id，citingcorpusid为普通字段（允许重复，极速插入）
     - publication_venues表使用publicationvenueid（值从id提取，UUID字符串）
     - 其他表使用corpusid

示例：
  # 处理papers文件夹（使用corpusid作为主键）
  python scripts/stream_gz_to_db_optimized.py --dir "E:\\machine_win01\\2025-09-30\\papers" --table papers
  
  # 处理authors文件夹（自动使用authorid作为主键）
  python scripts/stream_gz_to_db_optimized.py --dir "E:\\machine_win01\\2025-09-30\\authors" --table authors
  
  # 处理citations文件夹（自增主键，极速插入，无需去重）
  python scripts/stream_gz_to_db_optimized.py --dir "E:\\machine_win01\\2025-09-30\\citations" --table citations
  
  # 自定义解压进程数（根据CPU核心数）
  python scripts/stream_gz_to_db_optimized.py --dir "E:\\path\\to\\s2orc" --table s2orc --extractors 8
  
  # 中断后继续
  python scripts/stream_gz_to_db_optimized.py --dir "E:\\path\\to\\papers" --table papers --resume
        """
    )
    
    parser.add_argument('--dir', type=str, required=True,
                       help='GZ文件所在文件夹路径')
    parser.add_argument('--table', type=str, required=True,
                       choices=FIELD_TABLES,
                       help='目标数据库表名')
    parser.add_argument('--upsert', action='store_true',
                       help='使用UPSERT模式')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS,
                       help=f'解压进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--resume', action='store_true',
                       help='启用断点续传')
    parser.add_argument('--reset', action='store_true',
                       help='重置进度')
    parser.add_argument('--retry-failed', action='store_true',
                       help='重新处理失败的文件（重新下载后使用）')
    
    parser.set_defaults(resume=True)
    
    args = parser.parse_args()
    
    # 执行处理
    process_gz_folder_pipeline(
        folder_path=args.dir,
        table_name=args.table,
        use_upsert=args.upsert,
        num_extractors=args.extractors,
        resume=args.resume,
        reset_progress=args.reset,
        retry_failed=args.retry_failed
    )


if __name__ == '__main__':
    main()

