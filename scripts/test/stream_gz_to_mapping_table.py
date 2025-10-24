#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
轻量级映射表插入脚本（测试用）
只提取 corpusid 和 filename，插入到 corpus_filename_mapping 表

专门针对大数据集：embeddings_specter_v1, embeddings_specter_v2, s2orc, s2orc_v2
不插入真正的数据，只建立 corpusid → filename 的索引映射

性能优化（极致版）：
  ✅ 生产者-消费者模式：4个解压进程 + 1个插入进程并行
  ✅ 正则快速提取corpusid：预编译正则，避免完整JSON解析
  ✅ 超大批次：200000条/批次，200万条/事务
  ✅ 大队列缓冲（40个批次）：持续供应数据，无空闲等待
  ✅ 16MB解压缓冲区：减少磁盘I/O次数
  ✅ 批量字符串构建：减少StringIO写入次数
  ✅ 断点续传：支持中断恢复，自动过滤已插入数据
  
目标：30000-60000条/秒（无额外索引，最大化速度，比完整数据快30倍+）
"""

import gzip
import re
import sys
import time
import logging
from pathlib import Path
from typing import Set
from multiprocessing import Process, Queue, Manager
from queue import Empty

import psycopg2

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from database.config.db_config_v2 import DB_CONFIG

# =============================================================================
# 配置
# =============================================================================

# 轻量级配置：只有两个字段，插入极快（性能优化版）
BATCH_SIZE = 200000  # 20万条/批次（corpusid + filename 很小，极速插入）
COMMIT_BATCHES = 10  # 每10个批次commit一次（200万条/事务，约60MB）
NUM_EXTRACTORS = 4  # 4个解压进程（充分利用多核CPU）
QUEUE_SIZE = 40  # 增大队列缓冲（保证数据持续供应）

# 映射表名
MAPPING_TABLE = 'corpus_filename_mapping'

# 支持的数据集类型（只针对大数据集）
SUPPORTED_TABLES = {'embeddings_specter_v1', 'embeddings_specter_v2', 's2orc', 's2orc_v2'}

# 日志文件路径
PROGRESS_DIR = 'logs/progress_mapping'
FAILED_DIR = 'logs/failed_mapping'

# 正则表达式：快速提取corpusid
CORPUSID_PATTERN = re.compile(r'"corpusid"\s*:\s*(\d+)', re.IGNORECASE)

# 设置日志
logging.basicConfig(
    level=logging.ERROR,
    format='%(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    """失败文件记录器"""
    
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


def get_log_files(dataset_type: str):
    """
    根据数据集类型获取日志文件路径
    
    Args:
        dataset_type: 数据集类型 (embeddings_specter_v1/v2, s2orc/s2orc_v2)
    
    Returns:
        (progress_file, failed_file) 元组
    """
    progress_dir = Path(PROGRESS_DIR)
    failed_dir = Path(FAILED_DIR)
    
    progress_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)
    
    progress_file = progress_dir / f"{dataset_type}_mapping_progress.txt"
    failed_file = failed_dir / f"{dataset_type}_mapping_failed.txt"
    
    return str(progress_file), str(failed_file)


# =============================================================================
# 生产者：解压进程
# =============================================================================

def extractor_worker(
    file_queue: Queue,
    data_queue: Queue,
    stats_dict: dict,
    batch_size: int = BATCH_SIZE
):
    """
    解压工作进程（生产者）
    只提取 corpusid 和 filename
    """
    # 禁用此进程的日志输出
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    
    while True:
        try:
            task = file_queue.get(timeout=1)
            if task is None:  # 结束信号
                break
            
            gz_file_path, file_name = task
            
            try:
                batch = []
                valid_count = 0
                
                # 流式解压（性能优化：更大缓冲区）
                try:
                    import io
                    with gzip.open(gz_file_path, 'rb') as f_binary:
                        # 16MB缓冲区（2倍提升，减少I/O次数）
                        f = io.TextIOWrapper(io.BufferedReader(f_binary, buffer_size=16*1024*1024), 
                                            encoding='utf-8', errors='ignore')
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            
                            # 正则快速提取corpusid（预编译的正则，极速）
                            match = CORPUSID_PATTERN.search(line)
                            if not match:
                                continue
                            
                            corpusid = int(match.group(1))
                            valid_count += 1
                            
                            # 只存储 (corpusid, filename)
                            batch.append((corpusid, file_name))
                            
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
                    
                except (OSError, EOFError, ValueError, gzip.BadGzipFile) as gz_error:
                    # GZIP文件损坏或读取错误，直接跳过
                    data_queue.put(('error', file_name, f"Corrupted or unreadable"))
                    continue
                except MemoryError:
                    # 内存不足，尝试清理并跳过
                    import gc
                    gc.collect()
                    data_queue.put(('error', file_name, f"Memory error"))
                    continue
                
            except Exception as e:
                data_queue.put(('error', file_name, str(e)))
        
        except Empty:
            continue
        except Exception:
            break


# =============================================================================
# 消费者：插入进程
# =============================================================================

def inserter_worker(
    data_queue: Queue,
    dataset_type: str,
    stats_dict: dict,
    tracker: ProgressTracker,
    failed_logger: FailedFilesLogger,
    commit_batches: int = COMMIT_BATCHES,
    total_files: int = 0
):
    """
    插入工作进程（消费者）
    只插入 (corpusid, filename) 到映射表
    """
    try:
        # 创建数据库连接
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()
        
        # 性能优化配置（轻量级索引专用 - 极致优化）
        try:
            cursor.execute("SET synchronous_commit = OFF")  # 异步提交（关键优化）
            cursor.execute("SET commit_delay = 100000")  # 延迟提交100ms
            cursor.execute("SET work_mem = '1GB'")  # 更大的工作内存（加速排序和查询）
            cursor.execute("SET maintenance_work_mem = '2GB'")  # 维护内存
            cursor.execute("SET effective_cache_size = '16GB'")  # 缓存大小
            cursor.execute("SET temp_buffers = '512MB'")  # 临时缓冲区
            cursor.execute("SET max_parallel_workers_per_gather = 0")  # 关闭并行（单进程更快）
            # 临时表空间使用D盘（如果存在）
            try:
                cursor.execute("SET temp_tablespaces = 'd1_temp'")
            except:
                pass
        except Exception as e:
            conn.rollback()
            logger.warning(f"部分性能配置失败（可忽略）: {e}")
        
        total_inserted = 0
        file_stats = {}
        completed_files = 0
        last_log_time = time.time()
        start_time = time.time()
        batch_count = 0
        
        while True:
            try:
                item = data_queue.get(timeout=5)
                item_type = item[0]
                
                if item_type == 'stop':
                    break
                
                elif item_type == 'data':
                    _, file_name, batch = item
                    
                    try:
                        # 批量插入映射表
                        inserted = batch_insert_mapping(cursor, batch)
                        batch_count += 1
                        
                        # 每N个批次commit一次
                        if batch_count >= commit_batches:
                            conn.commit()
                            batch_count = 0
                        
                        total_inserted += inserted
                        file_stats[file_name] = file_stats.get(file_name, 0) + inserted
                    
                    except psycopg2.DatabaseError as db_error:
                        # 数据库错误，回滚并重试
                        conn.rollback()
                        batch_count = 0
                        logger.error(f"数据库错误（已回滚）: {db_error}")
                        # 尝试重连（可能是连接超时）
                        try:
                            conn.close()
                            conn = psycopg2.connect(**DB_CONFIG)
                            conn.autocommit = False
                            cursor = conn.cursor()
                        except:
                            pass
                        continue
                    except Exception as insert_error:
                        conn.rollback()
                        batch_count = 0
                        logger.error(f"批量插入失败（已回滚）: {insert_error}")
                        continue
                    
                    # 定期输出进度（每3秒）
                    current_time = time.time()
                    if current_time - last_log_time >= 3:
                        elapsed = current_time - start_time
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        progress_pct = (completed_files / total_files * 100) if total_files > 0 else 0
                        
                        if completed_files > 0:
                            avg_time_per_file = elapsed / completed_files
                            remaining_files = total_files - completed_files
                            eta_seconds = remaining_files * avg_time_per_file
                            eta_hours = int(eta_seconds / 3600)
                            eta_mins = int((eta_seconds % 3600) / 60)
                            eta_str = f"{eta_hours}小时{eta_mins}分" if eta_hours > 0 else f"{eta_mins}分"
                        else:
                            eta_str = "计算中..."
                        
                        print(f"\r📊 [{completed_files}/{total_files}] {progress_pct:.1f}% | "
                              f"{total_inserted:,}条 | {rate:.0f}条/秒 | "
                              f"剩余: {eta_str}    ", end='', flush=True)
                        last_log_time = current_time
                
                elif item_type == 'done':
                    _, file_name, _ = item
                    tracker.mark_completed(file_name)
                    completed_files += 1
                
                elif item_type == 'error':
                    _, file_name, error = item
                    failed_logger.log_failed(file_name, error)
                    completed_files += 1
            
            except Empty:
                continue
            except Exception as e:
                logger.error(f"[Inserter] 处理数据异常: {e}")
                conn.rollback()
                continue
        
        # 提交剩余数据
        if batch_count > 0:
            conn.commit()
        
        cursor.close()
        conn.close()
        
        stats_dict['inserted'] = total_inserted
        
    except Exception as e:
        logger.error(f"[Inserter] 严重错误: {e}")
        import traceback
        traceback.print_exc()


def batch_insert_mapping(cursor, batch: list) -> int:
    """
    批量插入映射表（只有两个字段：corpusid, filename）
    
    Args:
        cursor: 数据库游标
        batch: 数据批次 [(corpusid, filename), ...]
    
    Returns:
        插入的记录数
    """
    if not batch:
        return 0
    
    from io import StringIO
    import psycopg2.errors
    
    try:
        # 使用COPY批量插入（最快方法 - 优化版：预分配缓冲区）
        buffer = StringIO()
        # 优化：使用列表批量构建，然后一次性写入（减少写入次数）
        lines = []
        for corpusid, filename in batch:
            # corpusid是数字，filename需要转义（gz文件名通常不需要复杂转义）
            escaped_filename = filename.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
            lines.append(f"{corpusid}\t{escaped_filename}\n")
        buffer.write(''.join(lines))
        buffer.seek(0)
        
        try:
            cursor.copy_expert(
                f"""
                COPY {MAPPING_TABLE} (corpusid, filename)
                FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')
                """,
                buffer
            )
            return len(batch)
        except psycopg2.errors.UniqueViolation:
            # 有重复corpusid（断点续传场景）
            cursor.connection.rollback()
            
            # 提取所有corpusid
            batch_ids = [corpusid for corpusid, _ in batch]
            if not batch_ids:
                return 0
            
            placeholders = ','.join(['%s'] * len(batch_ids))
            
            # 查询已存在的corpusid
            cursor.execute(f"""
                SELECT corpusid FROM {MAPPING_TABLE} 
                WHERE corpusid IN ({placeholders})
            """, batch_ids)
            
            existing_ids = set(row[0] for row in cursor.fetchall())
            
            # 过滤掉已存在的记录
            new_batch = [(cid, fn) for cid, fn in batch if cid not in existing_ids]
            
            if not new_batch:
                return 0  # 全部已存在
            
            # COPY插入新数据（优化：批量构建）
            buffer = StringIO()
            lines = []
            for corpusid, filename in new_batch:
                escaped_filename = filename.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
                lines.append(f"{corpusid}\t{escaped_filename}\n")
            buffer.write(''.join(lines))
            buffer.seek(0)
            
            try:
                cursor.copy_expert(
                    f"""
                    COPY {MAPPING_TABLE} (corpusid, filename)
                    FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')
                    """,
                    buffer
                )
                return len(new_batch)
            except Exception:
                cursor.connection.rollback()
                return 0
    
    except Exception as e:
        logger.error(f"批量插入映射表失败: {e}")
        raise


# =============================================================================
# 主协调器
# =============================================================================

def process_gz_folder_to_mapping(
    folder_path: str,
    dataset_type: str,
    num_extractors: int = NUM_EXTRACTORS,
    resume: bool = True,
    reset_progress: bool = False,
    retry_failed: bool = False
):
    """
    处理GZ文件夹，提取corpusid和filename到映射表
    
    Args:
        folder_path: GZ文件夹路径
        dataset_type: 数据集类型 (embeddings_specter_v1/v2, s2orc/s2orc_v2)
        num_extractors: 解压进程数
        resume: 是否启用断点续传
        reset_progress: 是否重置进度
        retry_failed: 是否重试失败的文件
    """
    # 验证数据集类型
    if dataset_type not in SUPPORTED_TABLES:
        raise ValueError(f"不支持的数据集类型: {dataset_type}. 支持的类型: {SUPPORTED_TABLES}")
    
    folder = Path(folder_path)
    if not folder.exists():
        raise ValueError(f"文件夹不存在: {folder_path}")
    
    # 获取日志文件路径
    progress_file, failed_file = get_log_files(dataset_type)
    
    # 初始化进度跟踪
    tracker = ProgressTracker(progress_file)
    failed_logger = FailedFilesLogger(failed_file)
    
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
    
    # 过滤：排除已完成和失败的文件
    excluded_files = completed_files | failed_files
    pending_files = [(str(f), f.name) for f in gz_files if f.name not in excluded_files]
    
    # 输出信息
    failed_info = f", 失败: {len(failed_files)}" if failed_files else ""
    logger.info(f"\n▶ [{dataset_type}] 映射表模式 - 总计: {len(gz_files)}, 已完成: {len(completed_files)}{failed_info}, 待处理: {len(pending_files)}")
    
    if not pending_files:
        logger.info("✅ 所有文件已处理完成！\n")
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
        
        # 添加结束信号
        for _ in range(num_extractors):
            file_queue.put(None)
        
        # 启动插入进程
        inserter = Process(
            target=inserter_worker,
            args=(data_queue, dataset_type, stats_dict, tracker, failed_logger, COMMIT_BATCHES, len(pending_files)),
            name='Inserter'
        )
        inserter.start()
        
        # 启动解压进程
        extractors = []
        for i in range(num_extractors):
            p = Process(
                target=extractor_worker,
                args=(file_queue, data_queue, stats_dict, BATCH_SIZE),
                name=f'Extractor-{i+1}'
            )
            p.start()
            extractors.append(p)
        
        # 等待所有解压进程完成
        for p in extractors:
            p.join()
        
        # 发送停止信号
        data_queue.put(('stop', None, None))
        
        # 等待插入进程完成
        inserter.join()
        
        elapsed = time.time() - overall_start
        total_inserted = stats_dict.get('inserted', 0)
        avg_rate = total_inserted / elapsed if elapsed > 0 else 0
        
        logger.info(f"✅ [{dataset_type}] 完成: {len(pending_files)}个文件, {total_inserted:,}条映射, {elapsed/60:.1f}分钟, {avg_rate:.0f}条/秒\n")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  用户中断（进度已保存）")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ 处理失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# =============================================================================
# 主函数
# =============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='轻量级映射表插入脚本（测试用）- 只提取 corpusid 和 filename',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
说明：
  本脚本专门用于大数据集（embeddings_specter_v1/v2, s2orc/s2orc_v2）
  不插入完整数据，只建立 corpusid → filename 的索引映射
  
  性能优化：
    - 4个解压进程并行（充分利用CPU）
    - 20万条/批次，200万条/事务（极速插入）
    - 16MB解压缓冲区（减少I/O）
    - 无额外索引（只有主键，最大化插入速度）
  
  目标速度：30000-60000条/秒（无索引开销，比完整数据快30倍+）

使用前提：
  必须先执行 init_mapping_table.py 创建映射表

示例：
  # 处理 embeddings-specter_v1 文件夹
  python scripts/test/stream_gz_to_mapping_table.py --dir "E:\\2025-09-30\\embeddings-specter_v1" --dataset embeddings_specter_v1
  
  # 处理 s2orc 文件夹
  python scripts/test/stream_gz_to_mapping_table.py --dir "E:\\2025-09-30\\s2orc" --dataset s2orc
  
  # 中断后继续
  python scripts/test/stream_gz_to_mapping_table.py --dir "E:\\2025-09-30\\s2orc" --dataset s2orc --resume
        """
    )
    
    parser.add_argument('--dir', type=str, required=True,
                       help='GZ文件所在文件夹路径')
    parser.add_argument('--dataset', type=str, required=True,
                       choices=list(SUPPORTED_TABLES),
                       help='数据集类型')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS,
                       help=f'解压进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--resume', action='store_true',
                       help='启用断点续传')
    parser.add_argument('--reset', action='store_true',
                       help='重置进度')
    parser.add_argument('--retry-failed', action='store_true',
                       help='重新处理失败的文件')
    
    parser.set_defaults(resume=True)
    
    args = parser.parse_args()
    
    # 执行处理
    process_gz_folder_to_mapping(
        folder_path=args.dir,
        dataset_type=args.dataset,
        num_extractors=args.extractors,
        resume=args.resume,
        reset_progress=args.reset,
        retry_failed=args.retry_failed
    )


if __name__ == '__main__':
    main()

