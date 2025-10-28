#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
corpusid 到 gz 文件名映射脚本 - 极速 COPY 插入模式
使用独立日志：logs/corpusid_mapping_progress/ 和 logs/corpusid_mapping_failed/
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

# 性能参数
BATCH_SIZE = 500000
COMMIT_BATCHES = 6
NUM_EXTRACTORS = 1  # 提取进程（USB硬盘瓶颈）
NUM_INSERTERS = 4  # 插入进程（利用SSD性能）
QUEUE_SIZE = 100  # 较大队列，缓冲速度差异

TABLE_NAME = 'corpus_new_bigdataset'

PROGRESS_DIR = 'logs/corpusid_mapping_progress'
FAILED_DIR = 'logs/corpusid_mapping_failed'

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
    
    def reset(self):
        if self.failed_file.exists():
            self.failed_file.unlink()


def get_log_files(field_name: str):
    """获取日志文件路径"""
    progress_dir = Path(PROGRESS_DIR)
    failed_dir = Path(FAILED_DIR)
    progress_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)
    
    progress_file = progress_dir / f"{field_name}_progress.txt"
    failed_file = failed_dir / f"{field_name}_failed.txt"
    
    return str(progress_file), str(failed_file)


def fast_extract_corpusid(line: str) -> int:
    """快速提取 corpusid"""
    try:
        idx = line.lower().find('"corpusid"')
        if idx == -1:
            return None
        
        idx = line.find(':', idx)
        if idx == -1:
            return None
        
        idx += 1
        while idx < len(line) and line[idx] in ' \t':
            idx += 1
        
        start = idx
        while idx < len(line) and line[idx].isdigit():
            idx += 1
        
        if idx > start:
            return int(line[start:idx])
        return None
    except (ValueError, IndexError):
        return None


def extractor_worker(file_queue: Queue, data_queue: Queue, progress_queue: Queue,
                    stats_dict: dict, field_name: str, batch_size: int = BATCH_SIZE):
    """生产者：解压并提取 corpusid"""
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    
    while True:
        try:
            task = file_queue.get(timeout=1)
            if task is None:
                break
            
            gz_file_path, file_name = task
            
            try:
                batch_set = set()
                valid_count = 0
                
                try:
                    with gzip.open(gz_file_path, 'rb') as f_binary:
                        f = TextIOWrapper(BufferedReader(f_binary, buffer_size=32*1024*1024), 
                                        encoding='utf-8', errors='ignore')
                        
                        for line in f:
                            line = line.strip()
                            if not line or len(line) < 15:
                                continue
                            
                            corpusid = fast_extract_corpusid(line)
                            if corpusid is None:
                                continue
                            
                            valid_count += 1
                            batch_set.add(corpusid)
                            
                            if len(batch_set) >= batch_size:
                                data_queue.put(('data', field_name, file_name, list(batch_set)))
                                batch_set.clear()
                    
                    if batch_set:
                        data_queue.put(('data', field_name, file_name, list(batch_set)))
                    
                    progress_queue.put(('done', file_name, valid_count))
                    stats_dict['extracted'] = stats_dict.get('extracted', 0) + valid_count
                    
                except (OSError, EOFError, ValueError, gzip.BadGzipFile):
                    progress_queue.put(('error', file_name, "Corrupted file"))
                    continue
                except MemoryError:
                    import gc
                    gc.collect()
                    progress_queue.put(('error', file_name, "Memory error"))
                    continue
                
            except Exception as e:
                progress_queue.put(('error', file_name, str(e)))
        
        except Empty:
            continue
        except Exception:
            break


def inserter_worker(worker_id: int, data_queue: Queue, stats_dict: dict, 
                   commit_batches: int = COMMIT_BATCHES):
    """消费者：批量COPY插入"""
    conn = None
    cursor = None
    buffer_pool = StringIO()
    
    try:
        db_config = get_db_config('machine1')
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        cursor = conn.cursor()
        
        try:
            cursor.execute("SET synchronous_commit = OFF")
            cursor.execute("SET work_mem = '1GB'")
        except Exception:
            pass
        
        total_inserted = 0
        batch_count = 0
        
        while True:
            try:
                item = data_queue.get(timeout=5)
                item_type = item[0]
                
                if item_type == 'stop':
                    break
                
                elif item_type == 'data':
                    _, field_name, gz_filename, corpusids = item
                    
                    try:
                        inserted = batch_copy_insert(cursor, field_name, gz_filename, 
                                                     corpusids, buffer_pool)
                        batch_count += 1
                        
                        if batch_count >= commit_batches:
                            conn.commit()
                            batch_count = 0
                        
                        total_inserted += inserted
                    
                    except psycopg2.DatabaseError as e:
                        logger.warning(f"[Inserter-{worker_id}] 插入错误: {e}")
                        conn.rollback()
                        batch_count = 0
                        try:
                            if cursor:
                                cursor.close()
                            if conn:
                                conn.close()
                            conn = psycopg2.connect(**db_config)
                            conn.autocommit = False
                            cursor = conn.cursor()
                            cursor.execute("SET synchronous_commit = OFF")
                        except Exception:
                            pass
            
            except Empty:
                continue
            except Exception as e:
                logger.error(f"[Inserter-{worker_id}] 错误: {e}")
                if conn:
                    conn.rollback()
                continue
        
        if batch_count > 0:
            conn.commit()
        
        stats_dict[f'inserted_{worker_id}'] = total_inserted
        
    except Exception as e:
        logger.error(f"[Inserter-{worker_id}] 进程错误: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def batch_copy_insert(cursor, field_name: str, gz_filename: str, 
                     corpusids: list, buffer: StringIO = None) -> int:
    """批量COPY插入（极速模式）"""
    if not corpusids:
        return 0
    
    try:
        if buffer is None:
            buffer = StringIO()
        else:
            buffer.seek(0)
            buffer.truncate(0)
        
        # 构造 TSV 数据：corpusid \t gz_filename
        for cid in corpusids:
            buffer.write(str(cid))
            buffer.write('\t')
            buffer.write(gz_filename)
            buffer.write('\n')
        
        buffer.seek(0)
        
        # COPY 插入（只插入 corpusid 和对应字段）
        cursor.copy_expert(
            f"COPY {TABLE_NAME} (corpusid, {field_name}) FROM STDIN",
            buffer
        )
        return len(corpusids)
    
    except Exception as e:
        logger.error(f"批量插入失败: {e}")
        raise


def process_gz_folder_to_mapping(folder_path: str, field_name: str, 
                                 num_extractors: int = NUM_EXTRACTORS,
                                 num_inserters: int = NUM_INSERTERS,
                                 resume: bool = True, reset_progress: bool = False):
    """处理 GZ 文件夹，映射 corpusid 到 gz 文件名"""
    folder = Path(folder_path)
    if not folder.exists():
        raise ValueError(f"文件夹不存在: {folder_path}")
    
    progress_file, failed_file = get_log_files(field_name)
    tracker = ProgressTracker(progress_file)
    failed_logger = FailedFilesLogger(failed_file)
    
    if reset_progress:
        tracker.reset()
        failed_logger.reset()
    
    completed_files = tracker.load_completed() if resume else set()
    failed_files = failed_logger.load_failed() if resume else set()
    
    gz_files = list(folder.glob("*.gz"))
    if not gz_files:
        logger.warning(f"未找到 .gz 文件: {folder_path}")
        return
    
    excluded_files = completed_files | failed_files
    pending_files = [(str(f), f.name) for f in gz_files if f.name not in excluded_files]
    
    logger.info(f"\n▶ [{field_name}] 提取:{num_extractors}进程 插入:{num_inserters}进程")
    logger.info(f"   总计:{len(gz_files)} 已完成:{len(completed_files)} 待处理:{len(pending_files)}")
    
    if not pending_files:
        logger.info("✅ 所有文件已处理完成\n")
        return
    
    overall_start = time.time()
    
    try:
        file_queue = Queue()
        data_queue = Queue(maxsize=QUEUE_SIZE)
        progress_queue = Queue()
        
        manager = Manager()
        stats_dict = manager.dict()
        
        for task in pending_files:
            file_queue.put(task)
        
        for _ in range(num_extractors):
            file_queue.put(None)
        
        # 启动多个插入进程
        inserters = []
        for i in range(num_inserters):
            p = Process(
                target=inserter_worker,
                args=(i+1, data_queue, stats_dict, COMMIT_BATCHES),
                name=f'Inserter-{i+1}'
            )
            p.start()
            inserters.append(p)
        
        # 启动提取进程
        extractors = []
        for i in range(num_extractors):
            p = Process(
                target=extractor_worker,
                args=(file_queue, data_queue, progress_queue, stats_dict, field_name, BATCH_SIZE),
                name=f'Extractor-{i+1}'
            )
            p.start()
            extractors.append(p)
        
        # 监控进度
        completed_count = 0
        failed_count = 0
        last_log_time = time.time()
        
        while completed_count + failed_count < len(pending_files):
            try:
                item = progress_queue.get(timeout=2)
                item_type = item[0]
                
                if item_type == 'done':
                    _, file_name, _ = item
                    tracker.mark_completed(file_name)
                    completed_count += 1
                
                elif item_type == 'error':
                    _, file_name, error = item
                    failed_logger.log_failed(file_name, error)
                    failed_count += 1
                
                # 定期输出进度
                current_time = time.time()
                if current_time - last_log_time >= 3:
                    progress_pct = ((completed_count + failed_count) / len(pending_files) * 100) if pending_files else 0
                    print(f"\r📊 进度: {completed_count + failed_count}/{len(pending_files)} ({progress_pct:.1f}%)    ", 
                          end='', flush=True)
                    last_log_time = current_time
            
            except Empty:
                continue
        
        # 等待提取进程完成
        for p in extractors:
            p.join()
        
        # 停止插入进程
        for _ in range(num_inserters):
            data_queue.put(('stop', None, None, None))
        
        for p in inserters:
            p.join()
        
        elapsed = time.time() - overall_start
        total_inserted = sum(stats_dict.get(f'inserted_{i}', 0) for i in range(1, num_inserters+1))
        avg_rate = total_inserted / elapsed if elapsed > 0 else 0
        
        logger.info(f"\n✅ [{field_name}] 完成: {completed_count}文件, {total_inserted:,}条, "
                   f"{elapsed/60:.1f}分钟, {avg_rate:.0f}条/秒\n")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ 错误: {e}")
        sys.exit(1)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='提取 corpusid 并映射到 gz 文件名')
    parser.add_argument('--dir', type=str, required=True, help='GZ 文件夹路径')
    parser.add_argument('--field', type=str, required=True, help='表字段名')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS, 
                       help=f'提取进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--inserters', type=int, default=NUM_INSERTERS, 
                       help=f'插入进程数（默认: {NUM_INSERTERS}）')
    parser.add_argument('--resume', action='store_true', help='启用断点续传')
    parser.add_argument('--reset', action='store_true', help='重置进度')
    parser.set_defaults(resume=True)
    
    args = parser.parse_args()
    
    process_gz_folder_to_mapping(
        folder_path=args.dir,
        field_name=args.field,
        num_extractors=args.extractors,
        num_inserters=args.inserters,
        resume=args.resume,
        reset_progress=args.reset
    )


if __name__ == '__main__':
    main()

