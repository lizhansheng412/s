#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
提取所有 gz 文件中的 corpusid 并插入到 final_delivery 表
极速 COPY 插入模式，支持断点续传
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
import db_config

# 性能参数（数据库写入优化 - 小批次快速提交）
BATCH_SIZE = 1000000         # 20万条/批（小批次，避免单次COPY过慢）
COMMIT_BATCHES = 1        # 每3批提交（5万条/事务，快速释放锁）
NUM_EXTRACTORS = 1         # 提取进程（USB硬盘瓶颈，必须为1避免随机访问）
NUM_INSERTERS = 1          # 3个插入进程（平衡并行和锁竞争，可用--inserters调整）
QUEUE_SIZE = 80            # 小队列（快速流转，避免内存堆积）

# USB硬盘优化
USB_BUFFER_SIZE = 512 * 1024 * 1024  # 512MB缓冲（减少内存占用）
SORT_BY_SIZE = True                  # 按文件大小排序
SMALL_FILE_THRESHOLD = 500 * 1024 * 1024  # 500MB阈值（快速读取中小文件）
SKIP_BATCH_DEDUP = True              # 跳过批内去重（数据库层面去重更快）

TABLE_NAME = 'final_delivery'

# 日志目录
PROGRESS_DIR = 'logs/final_delivery_progress'
FAILED_DIR = 'logs/final_delivery_failed'

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
                    stats_dict: dict, batch_size: int = BATCH_SIZE):
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
                # 终极优化：跳过批内去重，直接用list（数据库层面去重更快）
                batch_list = [] if SKIP_BATCH_DEDUP else set()
                valid_count = 0
                
                try:
                    # USB硬盘优化：检查文件大小，选择最优读取方式
                    import os
                    file_size = os.path.getsize(gz_file_path)
                    
                    # 小文件（<1.5GB）：一次性读入内存，避免多次磁盘访问
                    if file_size < SMALL_FILE_THRESHOLD:
                        with gzip.open(gz_file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                            for line in content.splitlines():
                                line = line.strip()
                                if not line or len(line) < 15:
                                    continue
                                
                                corpusid = fast_extract_corpusid(line)
                                if corpusid is None:
                                    continue
                                
                                valid_count += 1
                                if SKIP_BATCH_DEDUP:
                                    batch_list.append(corpusid)
                                    if len(batch_list) >= batch_size:
                                        data_queue.put(('data', batch_list))
                                        batch_list = []
                                else:
                                    batch_list.add(corpusid)
                                    if len(batch_list) >= batch_size:
                                        data_queue.put(('data', list(batch_list)))
                                        batch_list.clear()
                    else:
                        # 大文件：流式读取，使用超大缓冲区（512MB）
                        with gzip.open(gz_file_path, 'rb') as f_binary:
                            f = TextIOWrapper(BufferedReader(f_binary, buffer_size=USB_BUFFER_SIZE), 
                                            encoding='utf-8', errors='ignore')
                            
                            for line in f:
                                line = line.strip()
                                if not line or len(line) < 15:
                                    continue
                                
                                corpusid = fast_extract_corpusid(line)
                                if corpusid is None:
                                    continue
                                
                                valid_count += 1
                                if SKIP_BATCH_DEDUP:
                                    batch_list.append(corpusid)
                                    if len(batch_list) >= batch_size:
                                        data_queue.put(('data', batch_list))
                                        batch_list = []
                                else:
                                    batch_list.add(corpusid)
                                    if len(batch_list) >= batch_size:
                                        data_queue.put(('data', list(batch_list)))
                                        batch_list.clear()
                    
                    if batch_list:
                        if SKIP_BATCH_DEDUP:
                            data_queue.put(('data', batch_list))
                        else:
                            data_queue.put(('data', list(batch_list)))
                    
                    progress_queue.put(('done', file_name, valid_count))
                    stats_dict['extracted'] = stats_dict.get('extracted', 0) + valid_count
                    
                except (OSError, EOFError, ValueError, gzip.BadGzipFile):
                    progress_queue.put(('error', file_name, "Corrupted"))
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
        config = db_config.DB_CONFIG
        conn = psycopg2.connect(**config)
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
                    _, corpusids = item
                    
                    try:
                        inserted = batch_copy_insert(cursor, corpusids, buffer_pool)
                        batch_count += 1
                        
                        if batch_count >= commit_batches:
                            conn.commit()
                            batch_count = 0
                        
                        total_inserted += inserted
                    
                    except psycopg2.DatabaseError as e:
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
            except Exception:
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


def batch_copy_insert(cursor, corpusids: list, buffer: StringIO = None) -> int:
    """批量COPY插入"""
    if not corpusids:
        return 0
    
    try:
        if buffer is None:
            buffer = StringIO()
        else:
            buffer.seek(0)
            buffer.truncate(0)
        
        # 构造数据：每行一个 corpusid
        for cid in corpusids:
            buffer.write(str(cid))
            buffer.write('\n')
        
        buffer.seek(0)
        
        # COPY 插入
        cursor.copy_expert(
            f"COPY {TABLE_NAME} (corpusid) FROM STDIN",
            buffer
        )
        return len(corpusids)
    
    except Exception as e:
        logger.error(f"批量插入失败: {e}")
        raise


def process_gz_folder(folder_path: str, 
                     num_extractors: int = NUM_EXTRACTORS,
                     num_inserters: int = NUM_INSERTERS,
                     resume: bool = True, 
                     reset_progress: bool = False):
    """处理 GZ 文件夹，提取所有 corpusid"""
    folder = Path(folder_path)
    if not folder.exists():
        raise ValueError(f"文件夹不存在: {folder_path}")
    
    # 使用文件夹名作为日志标识
    folder_name = folder.name
    progress_file = Path(PROGRESS_DIR) / f"{folder_name}_progress.txt"
    failed_file = Path(FAILED_DIR) / f"{folder_name}_failed.txt"
    
    tracker = ProgressTracker(str(progress_file))
    failed_logger = FailedFilesLogger(str(failed_file))
    
    if reset_progress:
        tracker.reset()
        failed_logger.reset()
    
    completed_files = tracker.load_completed() if resume else set()
    failed_files = failed_logger.load_failed() if resume else set()
    
    gz_files = list(folder.glob("*.gz"))
    if not gz_files:
        logger.warning(f"未找到 .gz 文件: {folder_path}")
        return
    
    # USB硬盘优化：按文件大小排序，先处理小文件
    # 好处：1) 减少内存压力 2) 快速看到进度 3) 顺序访问磁盘
    if SORT_BY_SIZE:
        gz_files = sorted(gz_files, key=lambda f: f.stat().st_size)
    
    excluded_files = completed_files | failed_files
    pending_files = [(str(f), f.name) for f in gz_files if f.name not in excluded_files]
    
    logger.info(f"\n📂 文件夹: {folder_name}")
    logger.info(f"   总计:{len(gz_files)} 已完成:{len(completed_files)} 待处理:{len(pending_files)}")
    logger.info(f"   提取:{num_extractors}进程 插入:{num_inserters}进程")
    
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
        
        # 启动插入进程
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
                args=(file_queue, data_queue, progress_queue, stats_dict, BATCH_SIZE),
                name=f'Extractor-{i+1}'
            )
            p.start()
            extractors.append(p)
        
        # 监控进度
        completed_count = 0
        failed_count = 0
        last_log_time = time.time()
        start_time = time.time()
        
        from datetime import datetime
        start_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n⏰ 开始时间: {start_datetime}")
        print(f"📊 总文件数: {len(pending_files)}\n")
        
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
                
                # 实时更新进度
                current_time = time.time()
                if current_time - last_log_time >= 1:
                    elapsed = current_time - start_time
                    processed = completed_count + failed_count
                    progress_pct = (processed / len(pending_files) * 100) if pending_files else 0
                    
                    # 预估剩余时间
                    if processed > 0:
                        avg_time_per_file = elapsed / processed
                        remaining_files = len(pending_files) - processed
                        eta_seconds = avg_time_per_file * remaining_files
                        eta_hours = int(eta_seconds // 3600)
                        eta_minutes = int((eta_seconds % 3600) // 60)
                        eta_secs = int(eta_seconds % 60)
                        eta_str = f"{eta_hours:02d}:{eta_minutes:02d}:{eta_secs:02d}"
                    else:
                        eta_str = "--:--:--"
                    
                    elapsed_hours = int(elapsed // 3600)
                    elapsed_minutes = int((elapsed % 3600) // 60)
                    elapsed_secs = int(elapsed % 60)
                    elapsed_str = f"{elapsed_hours:02d}:{elapsed_minutes:02d}:{elapsed_secs:02d}"
                    
                    print(f"\r📊 进度:{processed}/{len(pending_files)} ({progress_pct:.1f}%) | "
                          f"✅成功:{completed_count} ❌失败:{failed_count} | "
                          f"⏱️已用:{elapsed_str} 预计剩余:{eta_str}    ", 
                          end='', flush=True)
                    last_log_time = current_time
            
            except Empty:
                continue
        
        # 等待提取进程完成
        for p in extractors:
            p.join()
        
        # 停止插入进程
        for _ in range(num_inserters):
            data_queue.put(('stop', None))
        
        for p in inserters:
            p.join()
        
        elapsed = time.time() - overall_start
        total_inserted = sum(stats_dict.get(f'inserted_{i}', 0) for i in range(1, num_inserters+1))
        avg_rate = total_inserted / elapsed if elapsed > 0 else 0
        
        total_hours = int(elapsed // 3600)
        total_minutes = int((elapsed % 3600) // 60)
        total_secs = int(elapsed % 60)
        
        end_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print("\n")
        logger.info(f"{'='*70}")
        logger.info(f"✅ [{folder_name}] 处理完成")
        logger.info(f"{'='*70}")
        logger.info(f"⏰ 结束时间: {end_datetime}")
        logger.info(f"📊 处理统计:")
        logger.info(f"   - 成功文件: {completed_count:,}")
        logger.info(f"   - 失败文件: {failed_count:,}")
        logger.info(f"   - 插入记录: {total_inserted:,} 条")
        logger.info(f"⏱️  性能统计:")
        logger.info(f"   - 总耗时: {total_hours:02d}:{total_minutes:02d}:{total_secs:02d}")
        logger.info(f"   - 插入速度: {avg_rate:,.0f} 条/秒")
        if completed_count > 0:
            logger.info(f"   - 平均每文件: {elapsed/completed_count:.1f} 秒")
        logger.info(f"{'='*70}\n")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def batch_process_folders(folders: list, 
                          num_extractors: int = NUM_EXTRACTORS,
                          num_inserters: int = NUM_INSERTERS,
                          resume: bool = True):
    """批量处理多个文件夹"""
    logger.info("="*70)
    logger.info(f"🚀 批量处理启动")
    logger.info(f"   待处理: {len(folders)} 个文件夹")
    for i, folder in enumerate(folders, 1):
        logger.info(f"   [{i}] {folder}")
    logger.info(f"   进程配置: 提取={num_extractors}, 插入={num_inserters}")
    logger.info("="*70)
    
    overall_start = time.time()
    success_count = 0
    failed_folders = []
    
    for i, folder_path in enumerate(folders, 1):
        folder = Path(folder_path)
        
        logger.info("")
        logger.info(f"📁 [{i}/{len(folders)}] {folder.name}")
        logger.info("-"*70)
        
        if not folder.exists():
            logger.warning(f"⚠️  文件夹不存在: {folder_path}")
            failed_folders.append(f"{folder.name} (不存在)")
            continue
        
        try:
            process_gz_folder(
                folder_path=str(folder_path),
                num_extractors=num_extractors,
                num_inserters=num_inserters,
                resume=resume,
                reset_progress=False
            )
            
            success_count += 1
            logger.info(f"✅ {folder.name} 完成\n")
            
        except KeyboardInterrupt:
            logger.warning(f"\n⚠️  用户中断 | 已完成: {success_count}/{len(folders)}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ {folder.name} 失败: {e}")
            failed_folders.append(f"{folder.name} ({str(e)})")
            continue
    
    elapsed = time.time() - overall_start
    
    logger.info("")
    logger.info("="*70)
    logger.info("🏁 批量处理完成")
    logger.info(f"   成功: {success_count}/{len(folders)} | 耗时: {elapsed/3600:.2f}小时")
    
    if failed_folders:
        logger.warning("⚠️  失败列表:")
        for folder in failed_folders:
            logger.warning(f"     {folder}")
    
    logger.info("="*70)
    
    if success_count == len(folders):
        logger.info("✅ 全部成功！")
        logger.info("💡 运行去重和建索引: python scripts/all_corpusid_of_5dataset/init_table.py --finalize")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='提取 gz 文件中的 corpusid 并插入到 final_delivery 表',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例：
  # 单个文件夹
  python scripts/all_corpusid_of_5dataset/extract_corpusid.py \\
    --dir "E:\\data\\s2orc"
  
  # 批量处理多个文件夹
  python scripts/all_corpusid_of_5dataset/extract_corpusid.py \\
    --dirs "E:\\data\\s2orc" "E:\\data\\citations" "E:\\data\\papers"
  
  # 自定义进程数
  python scripts/all_corpusid_of_5dataset/extract_corpusid.py \\
    --dirs "E:\\data\\s2orc" "E:\\data\\citations" \\
    --extractors 2 --inserters 6
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dir', type=str, help='单个GZ文件夹路径')
    group.add_argument('--dirs', nargs='+', type=str, help='多个文件夹路径（空格分隔）')
    
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS, 
                       help=f'提取进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--inserters', type=int, default=NUM_INSERTERS, 
                       help=f'插入进程数（默认: {NUM_INSERTERS}）')
    parser.add_argument('--no-resume', action='store_true', help='禁用断点续传')
    parser.add_argument('--reset', action='store_true', help='重置进度')
    
    args = parser.parse_args()
    
    # 单个文件夹处理
    if args.dir:
        process_gz_folder(
            folder_path=args.dir,
            num_extractors=args.extractors,
            num_inserters=args.inserters,
            resume=not args.no_resume,
            reset_progress=args.reset
        )
    
    # 批量处理
    elif args.dirs:
        batch_process_folders(
            folders=args.dirs,
            num_extractors=args.extractors,
            num_inserters=args.inserters,
            resume=not args.no_resume
        )


if __name__ == '__main__':
    main()
