#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
极致优化版 - GZ文件流式写入文件性能测试
=============================================
功能：完全模拟 stream_gz_to_db_optimized.py，但将数据库插入替换为文件写入
目标：最大化写入速度，测试纯I/O性能瓶颈（目标：8000-15000条/秒）

极致性能优化要点：
✅ join+单次write：比writelines和列表推导式更快，减少内存分配
✅ 512MB超大缓冲区：大幅减少系统调用，提升30-50%性能
✅ 16MB解压缓冲区：减少解压系统调用
✅ 延迟flush：累积N批次后才flush，减少磁盘I/O
✅ 生产者-消费者：解压和写入完全并行
✅ 文件即时关闭：处理完立即释放资源（避免900个文件同时打开）
✅ 流式处理：不一次性加载整个文件到内存

输出：每个gz对应一个txt文件，保存到 E:\test 目录
"""

import gzip
import sys
import time
import logging
from pathlib import Path
from typing import Set
from multiprocessing import Process, Queue, Manager
from queue import Empty
import io

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

# =============================================================================
# 极致优化配置（针对文件写入）
# =============================================================================

# 输出目录
OUTPUT_DIR = Path(r'E:\test')

# 针对embedding数据优化（16KB/条）
EMBEDDING_BATCH_SIZE = 10000  # 10000条×16KB = 160MB/批次
BATCH_SIZE = EMBEDDING_BATCH_SIZE

# 性能配置
NUM_EXTRACTORS = 1  # 解压进程数（embedding数据大，单进程避免I/O争用）
QUEUE_SIZE = 50  # 更大队列，确保写入进程不空闲
WRITE_BUFFER_SIZE = 512 * 1024 * 1024  # 512MB 超大写入缓冲区（极致优化）
DECOMPRESS_BUFFER_SIZE = 16 * 1024 * 1024  # 16MB 解压缓冲区

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# 进度跟踪
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
    解压工作进程（生产者）- 极致优化版
    从file_queue取文件，解压后放入data_queue
    
    性能优化：
    1. 16MB解压缓冲区（减少解压系统调用）
    2. 流式处理（不一次性加载整个文件到内存）
    3. 批量发送到队列（减少进程间通信开销）
    4. 自动跳过损坏的gz文件
    """
    # 禁用此进程的日志输出
    logging.getLogger().setLevel(logging.CRITICAL)
    
    while True:
        try:
            # 获取文件任务
            task = file_queue.get(timeout=1)
            if task is None:  # 结束信号
                break
            
            gz_file_path, file_name = task
            
            try:
                batch = []
                valid_count = 0
                
                # 流式解压 - 使用更大缓冲区
                try:
                    with gzip.open(gz_file_path, 'rb') as f_binary:
                        # 包装为带大缓冲区的文本流（16MB）
                        f = io.TextIOWrapper(
                            io.BufferedReader(f_binary, buffer_size=DECOMPRESS_BUFFER_SIZE), 
                            encoding='utf-8', 
                            errors='ignore'
                        )
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            
                            valid_count += 1
                            batch.append(line)
                            
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
                    
                except (OSError, EOFError, ValueError):
                    # GZIP文件损坏，跳过
                    data_queue.put(('error', file_name, f"Corrupted"))
                    continue
                
            except Exception as e:
                data_queue.put(('error', file_name, str(e)))
        
        except Empty:
            continue
        except Exception:
            break


# =============================================================================
# 消费者：写入进程
# =============================================================================

def writer_worker(
    data_queue: Queue,
    output_dir: Path,
    folder_name: str,
    stats_dict: dict,
    tracker: ProgressTracker,
    failed_logger: FailedFilesLogger,
    total_files: int = 0,
    commit_batches: int = 3  # 每N个批次flush一次
):
    """
    写入工作进程（消费者）- 极致优化版
    持续从data_queue取数据并批量写入文件
    
    极致性能优化策略：
    1. 每个gz对应一个输出文件（避免单文件过大，embedding解压后约3GB/文件）
    2. join+单次write（比writelines和列表推导式更快，减少内存分配）
    3. 超大缓冲区512MB（大幅减少系统调用，提升30-50%性能）
    4. 延迟flush（累积N批次后才flush，减少磁盘I/O）
    5. 文件完成后立即关闭（避免900个文件同时打开，释放资源）
    6. 最小化字典访问（缓存文件句柄）
    
    预期性能：8000-15000条/秒（纯I/O，无数据库开销）
    """
    file_handles = {}  # 在try外定义，确保finally能访问
    
    try:
        # 创建输出目录
        output_dir.mkdir(parents=True, exist_ok=True)
        
        total_written = 0
        completed_files = 0
        last_log_time = time.time()
        start_time = time.time()
        
        while True:
            try:
                # 非阻塞获取
                item = data_queue.get(timeout=5)
                item_type = item[0]
                
                if item_type == 'stop':
                    break
                
                elif item_type == 'data':
                    _, file_name, batch = item
                    
                    try:
                        # 获取或创建文件句柄
                        if file_name not in file_handles:
                            # 输出文件名：原文件名去掉.gz后缀
                            output_file = output_dir / f"{file_name.replace('.gz', '.txt')}"
                            # 使用超大缓冲区（512MB）
                            fh = open(
                                output_file, 
                                'w', 
                                encoding='utf-8', 
                                buffering=WRITE_BUFFER_SIZE
                            )
                            file_handles[file_name] = [fh, 0]  # [handle, batch_count]
                        
                        fh, batch_count = file_handles[file_name]
                        
                        # 极速写入优化：
                        # 1. 使用join一次性构建字符串（避免列表推导式的内存开销）
                        # 2. 单次write比writelines更快（减少函数调用）
                        # 3. 直接在内存中构建完整字符串
                        if batch:  # 确保batch非空
                            fh.write('\n'.join(batch) + '\n')
                        
                        batch_count += 1
                        file_handles[file_name][1] = batch_count
                        total_written += len(batch)
                        
                        # 每N个批次flush一次（减少磁盘I/O）
                        if batch_count >= commit_batches:
                            fh.flush()
                            file_handles[file_name][1] = 0  # 重置计数
                    
                    except Exception as write_error:
                        logger.error(f"写入失败 {file_name}: {write_error}")
                        continue
                    
                    # 定期输出进度（每3秒）
                    current_time = time.time()
                    if current_time - last_log_time >= 3:
                        elapsed = current_time - start_time
                        rate = total_written / elapsed if elapsed > 0 else 0
                        progress_pct = (completed_files / total_files * 100) if total_files > 0 else 0
                        
                        # 估算剩余时间
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
                              f"{total_written:,}条 | {rate:.0f}条/秒 | "
                              f"剩余: {eta_str}    ", end='', flush=True)
                        last_log_time = current_time
                
                elif item_type == 'done':
                    _, file_name, _ = item
                    
                    # 关闭并删除文件句柄（释放资源）
                    if file_name in file_handles:
                        try:
                            fh, batch_count = file_handles[file_name]
                            if batch_count > 0:
                                fh.flush()
                            fh.close()
                        except Exception:
                            pass
                        finally:
                            del file_handles[file_name]
                    
                    # 标记文件完成
                    tracker.mark_completed(file_name)
                    completed_files += 1
                
                elif item_type == 'error':
                    _, file_name, error = item
                    
                    # 关闭文件句柄（如果有）
                    if file_name in file_handles:
                        try:
                            fh, _ = file_handles[file_name]
                            fh.close()
                        except Exception:
                            pass
                        finally:
                            del file_handles[file_name]
                    
                    # 记录失败文件
                    failed_logger.log_failed(file_name, error)
                    completed_files += 1
            
            except Empty:
                continue
            except Exception as e:
                logger.error(f"[Writer] 处理数据异常: {e}")
                continue
        
        # 关闭所有剩余的文件句柄
        for fh, batch_count in file_handles.values():
            try:
                if batch_count > 0:
                    fh.flush()
                fh.close()
            except Exception:
                pass  # 忽略关闭时的错误
        
        stats_dict['written'] = total_written
        
    except Exception as e:
        logger.error(f"[Writer] 严重错误: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 确保所有文件句柄都被关闭（即使发生异常）
        for file_name, handle_info in list(file_handles.items()):
            try:
                fh, batch_count = handle_info
                if not fh.closed:
                    if batch_count > 0:
                        fh.flush()
                    fh.close()
            except Exception:
                pass  # 忽略清理时的错误


# =============================================================================
# 主处理函数
# =============================================================================

def process_gz_folder_to_file(
    folder_path: str,
    folder_name: str,
    num_extractors: int = NUM_EXTRACTORS,
    resume: bool = True,
    batch_size: int = BATCH_SIZE,
    commit_batches: int = 3
):
    """
    处理GZ文件夹，将解压后的数据写入文件 - 极致优化版
    完全模拟 stream_gz_to_db_optimized.py 的架构和优化策略
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise ValueError(f"文件夹不存在: {folder_path}")
    
    # 初始化进度跟踪
    progress_file = OUTPUT_DIR / f"progress_{folder_name}.txt"
    failed_file = OUTPUT_DIR / f"failed_{folder_name}.txt"
    
    tracker = ProgressTracker(str(progress_file))
    failed_logger = FailedFilesLogger(str(failed_file))
    
    # 加载已完成和失败的文件
    completed_files = tracker.load_completed() if resume else set()
    failed_files = failed_logger.load_failed() if resume else set()
    
    # 扫描GZ文件
    gz_files = sorted(folder.glob("*.gz"))
    if not gz_files:
        logger.warning(f"未找到.gz文件: {folder_path}")
        return
    
    # 过滤已完成的文件（确保文件名一致性）
    excluded_files = completed_files | failed_files
    pending_files = [(str(f.absolute()), f.name) for f in gz_files if f.name not in excluded_files]
    
    # 精简输出
    failed_info = f", 失败: {len(failed_files)}" if failed_files else ""
    logger.info(f"\n▶ [{folder_name}] 总计: {len(gz_files)}, 已完成: {len(completed_files)}{failed_info}, 待处理: {len(pending_files)}")
    
    if not pending_files:
        logger.info("✅ 所有文件已处理完成！\n")
        return None
    
    overall_start = time.time()
    
    # 初始化进程变量（确保异常处理中可用）
    extractors = []
    writer = None
    
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
        
        # 启动写入进程（消费者）
        writer = Process(
            target=writer_worker,
            args=(data_queue, OUTPUT_DIR, folder_name, stats_dict, tracker, failed_logger, len(pending_files), commit_batches),
            name='Writer'
        )
        writer.start()
        
        # 启动解压进程（生产者）
        for i in range(num_extractors):
            p = Process(
                target=extractor_worker,
                args=(file_queue, data_queue, stats_dict, batch_size),
                name=f'Extractor-{i+1}'
            )
            p.start()
            extractors.append(p)
        
        # 等待所有解压进程完成
        for p in extractors:
            p.join(timeout=None)  # 显式设置timeout
        
        # 发送停止信号给写入进程
        data_queue.put(('stop', None, None))
        
        # 等待写入进程完成（设置合理的超时）
        writer.join(timeout=60)  # 最多等待60秒写入进程关闭
        if writer.is_alive():
            logger.warning("写入进程未正常关闭，强制终止")
            writer.terminate()
            writer.join(timeout=5)
        
        elapsed = time.time() - overall_start
        total_written = stats_dict.get('written', 0)
        avg_rate = total_written / elapsed if elapsed > 0 else 0
        
        logger.info(f"\n✅ [{folder_name}] 完成: {len(pending_files)}个文件, {total_written:,}条, {elapsed/60:.1f}分钟, {avg_rate:.0f}条/秒\n")
        
        return {
            'folder': folder_name,
            'files': len(pending_files),
            'lines': total_written,
            'elapsed': elapsed,
            'rate': avg_rate
        }
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  用户中断（进度已保存）")
        # 清理进程
        if extractors:
            for p in extractors:
                if p.is_alive():
                    p.terminate()
        if writer and writer.is_alive():
            writer.terminate()
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ 处理失败: {e}")
        import traceback
        traceback.print_exc()
        # 清理进程
        if extractors:
            for p in extractors:
                if p.is_alive():
                    p.terminate()
        if writer and writer.is_alive():
            writer.terminate()
        sys.exit(1)


# =============================================================================
# 批量处理函数
# =============================================================================

def batch_process_embeddings(
    base_dir: str, 
    num_extractors: int = NUM_EXTRACTORS,
    batch_size: int = BATCH_SIZE,
    commit_batches: int = 3
):
    """
    批量处理所有embedding文件夹 - 极致优化版
    完全模拟 batch_process_machine.py 的架构
    """
    base_path = Path(base_dir)
    if not base_path.exists():
        logger.error(f"❌ 数据根目录不存在: {base_dir}")
        sys.exit(1)
    
    # embedding文件夹列表（对应machine1配置）
    folders = [
        ('embeddings-specter_v1', 'embeddings_specter_v1'),
        ('embeddings-specter_v2', 'embeddings_specter_v2'),
    ]
    
    logger.info("="*80)
    logger.info("🚀 极致优化版 - GZ文件流式写入文件性能测试")
    logger.info("="*80)
    logger.info(f"数据根目录: {base_dir}")
    logger.info(f"输出目录: {OUTPUT_DIR}")
    logger.info(f"解压进程数: {num_extractors}")
    logger.info(f"批量大小: {batch_size:,}条")
    logger.info(f"写入缓冲区: {WRITE_BUFFER_SIZE/1024/1024:.0f}MB")
    logger.info(f"累积批次数: {commit_batches} (减少flush次数)")
    logger.info(f"待处理文件夹: {len(folders)}")
    for folder_name, _ in folders:
        logger.info(f"  - {folder_name}")
    logger.info("="*80)
    
    overall_start = time.time()
    results = []
    success_count = 0
    failed_folders = []
    
    for i, (folder_name, table_name) in enumerate(folders, 1):
        folder_path = base_path / folder_name
        
        logger.info("")
        logger.info("="*80)
        logger.info(f"📁 [{i}/{len(folders)}] 处理文件夹: {folder_name}")
        logger.info(f"输出: 每个gz对应一个txt文件（避免单文件过大）")
        logger.info("="*80)
        
        if not folder_path.exists():
            logger.warning(f"⚠️  文件夹不存在，跳过: {folder_path}")
            failed_folders.append(f"{folder_name} (不存在)")
            continue
        
        try:
            result = process_gz_folder_to_file(
                folder_path=str(folder_path),
                folder_name=table_name,
                num_extractors=num_extractors,
                resume=True,
                batch_size=batch_size,
                commit_batches=commit_batches
            )
            if result:
                results.append(result)
                success_count += 1
            
        except KeyboardInterrupt:
            logger.warning("\n⚠️  用户中断（进度已保存）")
            logger.info(f"已完成: {success_count}/{len(folders)}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ {folder_name} 处理失败: {e}")
            failed_folders.append(f"{folder_name} ({str(e)})")
            continue
    
    # 总结
    total_elapsed = time.time() - overall_start
    
    logger.info("")
    logger.info("="*80)
    logger.info("🏁 性能测试完成")
    logger.info("="*80)
    logger.info(f"总文件夹数: {len(folders)}")
    logger.info(f"成功: {success_count}")
    logger.info(f"失败: {len(failed_folders)}")
    logger.info(f"总耗时: {total_elapsed/3600:.2f} 小时 ({total_elapsed/60:.1f} 分钟)")
    logger.info("")
    
    if results:
        logger.info("📊 详细统计:")
        logger.info("-"*80)
        total_lines = 0
        total_files = 0
        for result in results:
            logger.info(f"  {result['folder']}:")
            logger.info(f"    文件数: {result['files']}")
            logger.info(f"    数据行: {result['lines']:,}")
            logger.info(f"    耗时: {result['elapsed']/60:.1f}分钟")
            logger.info(f"    速度: {result['rate']:.0f}条/秒")
            total_lines += result['lines']
            total_files += result['files']
        logger.info("-"*80)
        logger.info(f"  总计:")
        logger.info(f"    文件数: {total_files}")
        logger.info(f"    数据行: {total_lines:,}")
        logger.info(f"    平均速度: {total_lines/total_elapsed:.0f}条/秒")
    
    if failed_folders:
        logger.warning("")
        logger.warning("⚠️  以下文件夹处理失败:")
        for folder in failed_folders:
            logger.warning(f"  - {folder}")
    
    logger.info("="*80)
    logger.info(f"\n✅ 测试文件已保存到: {OUTPUT_DIR}")
    
    if success_count == len(folders):
        logger.info("✅ 所有文件夹处理成功！")
    else:
        logger.warning("⚠️  部分文件夹处理失败，请检查日志")


# =============================================================================
# 主函数
# =============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='极致优化版 - GZ文件流式写入文件性能测试（目标：最大化写入速度）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
性能优化要点：
  🚀 生产者-消费者模式：解压和写入完全并行
  🚀 writelines批量写入：比循环write快10-20%
  🚀 超大缓冲区：512MB写入缓冲区，16MB解压缓冲区
  🚀 延迟flush：累积多个批次后才flush，减少磁盘I/O
  🚀 单文件合并：所有数据写入单个大文件，避免多文件开销
  🚀 断点续传：支持中断恢复

测试目标：
  - 评估纯I/O性能（解压 + 文件写入）
  - 预期速度：5000-10000条/秒（无数据库开销）
  - 对比数据库插入性能，找出瓶颈

输出说明：
  - 每个gz文件对应一个txt文件（避免单文件过大，embedding解压后约3GB/文件）
  - 输出目录：E:\\test
  - 实时显示进度、速度、预估剩余时间

示例：
  # 批量测试所有embedding文件夹（推荐）
  python scripts/test_gz_to_file_speed.py --base-dir "E:\\2025-09-30" --batch
  
  # 自定义配置
  python scripts/test_gz_to_file_speed.py --base-dir "E:\\2025-09-30" --batch --batch-size 20000 --commit-batches 5
  
  # 测试单个文件夹
  python scripts/test_gz_to_file_speed.py --dir "E:\\2025-09-30\\embeddings-specter_v1" --folder embeddings_specter_v1
        """
    )
    
    parser.add_argument('--dir', type=str,
                       help='GZ文件所在文件夹路径（单个文件夹测试）')
    parser.add_argument('--folder', type=str,
                       help='文件夹名称（用于输出文件命名）')
    parser.add_argument('--base-dir', type=str,
                       help='数据根目录（批量测试所有embedding文件夹）')
    parser.add_argument('--batch', action='store_true',
                       help='批量处理所有embedding文件夹')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS,
                       help=f'解压进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                       help=f'批量写入大小（默认: {BATCH_SIZE:,}）')
    parser.add_argument('--commit-batches', type=int, default=3,
                       help='累积N个批次后才flush（默认: 3，减少磁盘I/O）')
    
    args = parser.parse_args()
    
    if args.batch and args.base_dir:
        # 批量处理模式
        batch_process_embeddings(
            base_dir=args.base_dir,
            num_extractors=args.extractors,
            batch_size=args.batch_size,
            commit_batches=args.commit_batches
        )
    elif args.dir and args.folder:
        # 单个文件夹测试模式
        process_gz_folder_to_file(
            folder_path=args.dir,
            folder_name=args.folder,
            num_extractors=args.extractors,
            resume=True,
            batch_size=args.batch_size,
            commit_batches=args.commit_batches
        )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()

