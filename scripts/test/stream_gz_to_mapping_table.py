#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 量级映射表插入脚本
只提取 corpusid 到 corpus_filename_mapping 表
"""

import gzip
import sys
import time
import logging
from pathlib import Path
from typing import Set
from multiprocessing import Process, Queue, Manager
from queue import Empty
from io import StringIO, BufferedReader, TextIOWrapper

import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.config import get_db_config

# 配置参数（平衡性能和稳定性）
BATCH_SIZE = 500000   # 50万条/批次（corpusid只有8字节）
COMMIT_BATCHES = 6    # 每6批次提交（300万条/事务，约24MB）
NUM_EXTRACTORS = 4
QUEUE_SIZE = 30

MAPPING_TABLE = 'corpus_bigdataset'
SUPPORTED_TABLES = {'embeddings_specter_v1', 'embeddings_specter_v2', 's2orc', 's2orc_v2'}

PROGRESS_DIR = 'logs/progress_mapping'
FAILED_DIR = 'logs/failed_mapping'

logging.basicConfig(level=logging.ERROR, format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ProgressTracker:
    """进度跟踪"""
    
    def __init__(self, progress_file: str):
        self.progress_file = Path(progress_file)
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
    
    def load_completed(self) -> Set[str]:
        if not self.progress_file.exists():
            return set()
        with open(self.progress_file, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    
    def mark_completed(self, file_name: str):
        with open(self.progress_file, 'a', encoding='utf-8') as f:
            f.write(f"{file_name}\n")
            f.flush()
    
    def reset(self):
        if self.progress_file.exists():
            self.progress_file.unlink()


class FailedFilesLogger:
    """失败文件记录"""
    
    def __init__(self, failed_file: str):
        self.failed_file = Path(failed_file)
        self.failed_file.parent.mkdir(parents=True, exist_ok=True)
    
    def load_failed(self) -> Set[str]:
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
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.failed_file, 'a', encoding='utf-8') as f:
            f.write(f"{timestamp} | {file_name} | {error}\n")
            f.flush()


def get_log_files(dataset_type: str):
    """获取日志文件路径"""
    progress_dir = Path(PROGRESS_DIR)
    failed_dir = Path(FAILED_DIR)
    progress_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)
    
    progress_file = progress_dir / f"{dataset_type}_mapping_progress.txt"
    failed_file = failed_dir / f"{dataset_type}_mapping_failed.txt"
    
    return str(progress_file), str(failed_file)


def fast_extract_corpusid(line: str) -> int:
    """快速提取 corpusid（比正则快3-5倍）"""
    try:
        # 查找 "corpusid" 字段（不区分大小写）
        idx = line.lower().find('"corpusid"')
        if idx == -1:
            return None
        
        # 查找冒号后的数字
        idx = line.find(':', idx)
        if idx == -1:
            return None
        
        # 跳过空白和引号
        idx += 1
        while idx < len(line) and line[idx] in ' \t':
            idx += 1
        
        # 提取数字
        start = idx
        while idx < len(line) and line[idx].isdigit():
            idx += 1
        
        if idx > start:
            return int(line[start:idx])
        return None
    except (ValueError, IndexError):
        return None


def extractor_worker(file_queue: Queue, data_queue: Queue, stats_dict: dict, batch_size: int = BATCH_SIZE):
    """生产者：解压并提取 corpusid（已优化）"""
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    
    while True:
        try:
            task = file_queue.get(timeout=1)
            if task is None:
                break
            
            gz_file_path, file_name = task
            
            try:
                # 使用 set 在 extractor 侧去重，减少传输量
                batch_set = set()
                valid_count = 0
                
                try:
                    with gzip.open(gz_file_path, 'rb') as f_binary:
                        # 优化：32MB 缓冲区，提升读取速度
                        f = TextIOWrapper(BufferedReader(f_binary, buffer_size=32*1024*1024), 
                                        encoding='utf-8', errors='ignore')
                        
                        for line in f:
                            line = line.strip()
                            if not line or len(line) < 15:  # "corpusid":1 最短15字符
                                continue
                            
                            # 快速提取（比正则快3-5倍）
                            corpusid = fast_extract_corpusid(line)
                            if corpusid is None:
                                continue
                            
                            valid_count += 1
                            batch_set.add(corpusid)
                            
                            # 批次达到上限，转为list发送（set内存更紧凑）
                            if len(batch_set) >= batch_size:
                                data_queue.put(('data', file_name, list(batch_set)))
                                batch_set.clear()
                    
                    # 发送剩余数据
                    if batch_set:
                        data_queue.put(('data', file_name, list(batch_set)))
                    
                    data_queue.put(('done', file_name, valid_count))
                    stats_dict['extracted'] = stats_dict.get('extracted', 0) + valid_count
                    
                except (OSError, EOFError, ValueError, gzip.BadGzipFile):
                    data_queue.put(('error', file_name, "Corrupted file"))
                    continue
                except MemoryError:
                    import gc
                    gc.collect()
                    data_queue.put(('error', file_name, "Memory error"))
                    continue
                
            except Exception as e:
                data_queue.put(('error', file_name, str(e)))
        
        except Empty:
            continue
        except Exception:
            break


def inserter_worker(data_queue: Queue, dataset_type: str, stats_dict: dict, 
                   tracker: ProgressTracker, failed_logger: FailedFilesLogger, 
                   commit_batches: int = COMMIT_BATCHES, total_files: int = 0):
    """消费者：批量插入 corpusid（已优化）"""
    conn = None
    cursor = None
    buffer_pool = StringIO()  # 复用 StringIO 对象，减少 GC 压力
    
    try:
        # 始终连接本机数据库（machine1 的 5431 端口）
        db_config = get_db_config('machine1')
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        cursor = conn.cursor()
        
        # 数据库性能优化（会话级别，保守配置）
        try:
            cursor.execute("SET synchronous_commit = OFF")
            cursor.execute("SET work_mem = '1GB'")
            cursor.execute("SET maintenance_work_mem = '2GB'")
        except Exception as e:
            pass  # 忽略配置失败
        
        total_inserted = 0
        completed_files = 0
        last_log_time = time.time()
        start_time = time.time()
        batch_count = 0
        pending_done = []  # 批量写入进度文件
        
        while True:
            try:
                item = data_queue.get(timeout=5)
                item_type = item[0]
                
                if item_type == 'stop':
                    break
                
                elif item_type == 'data':
                    _, file_name, batch = item
                    
                    # 重试机制（最多3次）
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            # 使用复用的 StringIO
                            inserted = batch_insert_corpusids(cursor, batch, buffer_pool)
                            batch_count += 1
                            
                            # 超大事务提交策略（USB硬盘优化）
                            if batch_count >= commit_batches:
                                conn.commit()
                                batch_count = 0
                                
                                # 批量写入进度（减少文件 I/O）
                                if pending_done:
                                    for fname in pending_done:
                                        tracker.mark_completed(fname)
                                    pending_done.clear()
                            
                            total_inserted += inserted
                            break  # 成功，跳出重试循环
                        
                        except psycopg2.DatabaseError as e:
                            logger.warning(f"插入错误 (尝试 {attempt+1}/{max_retries}): {e}")
                            conn.rollback()
                            batch_count = 0
                            
                            if attempt < max_retries - 1:
                                # 重连数据库
                                try:
                                    if cursor:
                                        cursor.close()
                                    if conn:
                                        conn.close()
                                    conn = psycopg2.connect(**db_config)
                                    conn.autocommit = False
                                    cursor = conn.cursor()
                                    cursor.execute("SET synchronous_commit = OFF")
                                    time.sleep(0.5)  # 短暂等待
                                except Exception:
                                    pass
                            else:
                                # 最后一次也失败，记录并跳过
                                logger.error(f"批次插入失败（已重试{max_retries}次），跳过")
                                break
                    
                    # 定期输出进度（每2秒，更频繁的反馈）
                    current_time = time.time()
                    if current_time - last_log_time >= 2:
                        elapsed = current_time - start_time
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        progress_pct = (completed_files / total_files * 100) if total_files > 0 else 0
                        
                        print(f"\r📊 [{completed_files}/{total_files}] {progress_pct:.1f}% | "
                              f"{total_inserted:,}条 | {rate:,.0f}条/秒    ", end='', flush=True)
                        last_log_time = current_time
                
                elif item_type == 'done':
                    _, file_name, _ = item
                    pending_done.append(file_name)
                    completed_files += 1
                
                elif item_type == 'error':
                    _, file_name, error = item
                    failed_logger.log_failed(file_name, error)
                    completed_files += 1
            
            except Empty:
                continue
            except Exception as e:
                logger.error(f"处理队列项错误: {e}")
                if conn:
                    conn.rollback()
                continue
        
        # 最终提交
        if batch_count > 0:
            conn.commit()
        
        # 写入剩余进度
        if pending_done:
            for fname in pending_done:
                tracker.mark_completed(fname)
        
        stats_dict['inserted'] = total_inserted
        
    except Exception as e:
        logger.error(f"插入进程错误: {e}")
    finally:
        # 安全清理
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def batch_insert_corpusids(cursor, batch: list, buffer: StringIO = None) -> int:
    """批量插入 corpusid（COPY 方式，复用 StringIO）"""
    if not batch:
        return 0
    
    try:
        # 复用 StringIO 对象，避免频繁创建销毁
        if buffer is None:
            buffer = StringIO()
        else:
            buffer.seek(0)
            buffer.truncate(0)
        
        # 优化：直接写入，避免 join 的额外内存分配
        for cid in batch:
            buffer.write(str(cid))
            buffer.write('\n')
        
        buffer.seek(0)
        
        # COPY 是 PostgreSQL 最快的批量插入方式
        cursor.copy_expert(
            f"COPY {MAPPING_TABLE} (corpusid) FROM STDIN",
            buffer
        )
        return len(batch)
    
    except Exception as e:
        logger.error(f"批量插入失败: {e}")
        raise


def process_gz_folder_to_mapping(folder_path: str, dataset_type: str, 
                                 num_extractors: int = NUM_EXTRACTORS,
                                 resume: bool = True, reset_progress: bool = False):
    """处理 GZ 文件夹，提取 corpusid"""
    if dataset_type not in SUPPORTED_TABLES:
        raise ValueError(f"不支持的数据集: {dataset_type}")
    
    folder = Path(folder_path)
    if not folder.exists():
        raise ValueError(f"文件夹不存在: {folder_path}")
    
    progress_file, failed_file = get_log_files(dataset_type)
    tracker = ProgressTracker(progress_file)
    failed_logger = FailedFilesLogger(failed_file)
    
    if reset_progress:
        tracker.reset()
        failed_logger.reset()
    
    completed_files = tracker.load_completed() if resume else set()
    failed_files = failed_logger.load_failed() if resume else set()
    
    gz_files = sorted(folder.glob("*.gz"))
    if not gz_files:
        logger.warning(f"未找到 .gz 文件: {folder_path}")
        return
    
    excluded_files = completed_files | failed_files
    pending_files = [(str(f), f.name) for f in gz_files if f.name not in excluded_files]
    
    logger.info(f"\n▶ [{dataset_type}] 总计: {len(gz_files)}, 已完成: {len(completed_files)}, 待处理: {len(pending_files)}")
    
    if not pending_files:
        logger.info("✅ 所有文件已处理完成\n")
        return
    
    overall_start = time.time()
    
    try:
        file_queue = Queue()
        data_queue = Queue(maxsize=QUEUE_SIZE)
        
        manager = Manager()
        stats_dict = manager.dict()
        
        for task in pending_files:
            file_queue.put(task)
        
        for _ in range(num_extractors):
            file_queue.put(None)
        
        inserter = Process(
            target=inserter_worker,
            args=(data_queue, dataset_type, stats_dict, tracker, failed_logger, COMMIT_BATCHES, len(pending_files)),
            name='Inserter'
        )
        inserter.start()
        
        extractors = []
        for i in range(num_extractors):
            p = Process(
                target=extractor_worker,
                args=(file_queue, data_queue, stats_dict, BATCH_SIZE),
                name=f'Extractor-{i+1}'
            )
            p.start()
            extractors.append(p)
        
        for p in extractors:
            p.join()
        
        data_queue.put(('stop', None, None))
        inserter.join()
        
        elapsed = time.time() - overall_start
        total_inserted = stats_dict.get('inserted', 0)
        avg_rate = total_inserted / elapsed if elapsed > 0 else 0
        
        logger.info(f"\n✅ [{dataset_type}] 完成: {len(pending_files)}文件, {total_inserted:,}条, {elapsed/60:.1f}分钟, {avg_rate:.0f}条/秒\n")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ 错误: {e}")
        sys.exit(1)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='提取 corpusid 到大数据集表（写入本机 Machine1 数据库）')
    parser.add_argument('--dir', type=str, required=True, help='GZ 文件夹路径')
    parser.add_argument('--dataset', type=str, required=True, choices=list(SUPPORTED_TABLES), help='数据集类型')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS, help=f'解压进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--resume', action='store_true', help='启用断点续传')
    parser.add_argument('--reset', action='store_true', help='重置进度')
    parser.set_defaults(resume=True)
    
    args = parser.parse_args()
    
    process_gz_folder_to_mapping(
        folder_path=args.dir,
        dataset_type=args.dataset,
        num_extractors=args.extractors,
        resume=args.resume,
        reset_progress=args.reset
    )


if __name__ == '__main__':
    main()
