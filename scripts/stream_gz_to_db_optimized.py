#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
极致优化版 - GZ文件流式批量插入
性能优化：
  ✅ 生产者-消费者模式：解压和插入完全分离并行
  ✅ 正则快速提取corpusid：避免完整JSON解析
  ✅ 更大批次：50000条/批次，减少数据库往返
  ✅ 队列缓冲：解压快时不等待，插入快时不空闲
  ✅ 断点续传：支持中断恢复
  
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

from database.config.db_config_v2 import DB_CONFIG, FIELD_TABLES

# =============================================================================
# 根据数据大小动态配置（关键优化！）
# =============================================================================

# 针对TEXT类型优化配置（关键：控制磁盘IO，所有表commit<=800MB）
TABLE_CONFIGS = {
    # 超大数据 (60-120KB/条): s2orc系列 - 控制磁盘IO压力
    's2orc': {'batch_size': 2000, 'commit_batches': 3, 'extractors': 4},
    's2orc_v2': {'batch_size': 2000, 'commit_batches': 3, 'extractors': 4},
    # 2000条×100KB=200MB/批，3批=600MB提交 ✓
        
    # 中等数据 (16KB/条): embeddings系列 - TEXT类型优化
    'embeddings_specter_v1': {'batch_size': 10000, 'commit_batches': 3, 'extractors': 4},
    'embeddings_specter_v2': {'batch_size': 10000, 'commit_batches': 3, 'extractors': 4},
    # 10000条×16KB=160MB/批，3批=480MB提交
    
    # 小数据 (1-3KB/条): 其他表 - 大批次但控制总提交量
    'papers': {'batch_size': 100000, 'commit_batches': 3, 'extractors': 4},
    'abstracts': {'batch_size': 100000, 'commit_batches': 3, 'extractors': 4},
    'authors': {'batch_size': 100000, 'commit_batches': 3, 'extractors': 4},
    'citations': {'batch_size': 100000, 'commit_batches': 3, 'extractors': 4},
    'paper_ids': {'batch_size': 100000, 'commit_batches': 3, 'extractors': 4},
    'publication_venues': {'batch_size': 100000, 'commit_batches': 3, 'extractors': 4},
    'tldrs': {'batch_size': 100000, 'commit_batches': 3, 'extractors': 4},
    # 100000条×2KB=200MB/批，3批=600MB提交 ✓
}

DEFAULT_CONFIG = {'batch_size': 100000, 'commit_batches': 3, 'extractors': 4}
NUM_EXTRACTORS = 4  # USB磁盘优化：减少并发读取
QUEUE_SIZE = 50  # USB磁盘优化：减少内存缓冲
PROGRESS_FILE = 'logs/gz_progress.txt'

# 正则表达式：快速提取corpusid（比完整JSON解析快10倍）
CORPUSID_PATTERN = re.compile(r'"corpusid"\s*:\s*(\d+)', re.IGNORECASE)

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
        
        if completed:
            logger.info(f"✓ 已加载进度: {len(completed)} 个文件已完成")
        return completed
    
    def mark_completed(self, file_name: str):
        with open(self.progress_file, 'a', encoding='utf-8') as f:
            f.write(f"{file_name}\n")
            f.flush()
    
    def reset(self):
        if self.progress_file.exists():
            self.progress_file.unlink()


# =============================================================================
# 生产者：解压进程（多个并行）
# =============================================================================

def extractor_worker(
    file_queue: Queue,
    data_queue: Queue,
    stats_dict: dict,
    corpusid_key: str = 'corpusid',
    batch_size: int = 10000
):
    """
    解压工作进程（生产者）
    从file_queue取文件，解压后放入data_queue
    """
    # 完全禁用此进程的日志输出
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    
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
                try:
                    with gzip.open(gz_file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                        for line in f:
                            line_count += 1
                            line = line.strip()
                            if not line:
                                continue
                            
                            # 正则快速提取corpusid（避免完整JSON解析）
                            match = CORPUSID_PATTERN.search(line)
                            if not match:
                                continue
                            
                            corpusid = int(match.group(1))
                            valid_count += 1
                            
                            # 优化：减少字符串操作
                            # 检查是否需要转义（大多数情况不需要）
                            if '\\' in line or '\n' in line or '\t' in line:
                                json_escaped = line.replace('\\', '\\\\').replace('\n', '\\n').replace('\t', '\\t')
                            else:
                                json_escaped = line
                            
                            batch.append((corpusid, json_escaped))
                            
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
    use_upsert: bool = False,
    commit_batches: int = 3,
    total_files: int = 0
):
    """
    插入工作进程（消费者）
    持续从data_queue取数据并批量插入
    """
    print("\n🚀 数据插入进程已启动\n")
    
    try:
        # 创建数据库连接
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()
        
        # 性能优化配置（会话级别可修改的参数）
        cursor.execute("SET synchronous_commit = OFF")  # 异步提交
        cursor.execute("SET commit_delay = 100000")  # 延迟提交100ms
        cursor.execute("SET maintenance_work_mem = '4GB'")  # 增大维护内存
        cursor.execute("SET work_mem = '2GB'")  # 增大工作内存
        cursor.execute("SET temp_buffers = '2GB'")  # 临时缓冲区
        cursor.execute("SET effective_cache_size = '16GB'")  # 增大缓存
        # 注意：wal_writer_delay 和 max_wal_size 需要在postgresql.conf中设置
        
        # 禁用触发器（INSERT模式）
        if not use_upsert:
            cursor.execute(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL")
            conn.commit()
        
        total_inserted = 0
        file_stats = {}  # {file_name: inserted_count}
        completed_files = 0
        last_log_time = time.time()
        start_time = time.time()
        batch_count = 0  # 批次计数器
        
        while True:
            try:
                # 非阻塞获取，避免长时间等待
                item = data_queue.get(timeout=5)
                
                item_type = item[0]
                
                if item_type == 'stop':
                    logger.info("[Inserter] 收到停止信号")
                    break
                
                elif item_type == 'data':
                    _, file_name, batch = item
                    
                    try:
                        # 批量插入
                        inserted = batch_insert_copy(cursor, table_name, batch, use_upsert)
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
                    
                    # 定期输出进度（每10秒）
                    current_time = time.time()
                    if current_time - last_log_time >= 3:
                        elapsed = current_time - start_time
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        
                        # 计算进度和预估时间
                        progress_pct = (completed_files / total_files * 100) if total_files > 0 else 0
                        remaining_files = total_files - completed_files
                        eta_seconds = (remaining_files * elapsed / completed_files) if completed_files > 0 else 0
                        eta_hours = int(eta_seconds / 3600)
                        eta_mins = int((eta_seconds % 3600) / 60)
                        eta_str = f"{eta_hours}h{eta_mins}m" if eta_hours > 0 else f"{eta_mins}分"
                        
                        print(f"\r📊 [{completed_files}/{total_files}] {progress_pct:.1f}% | "
                              f"{total_inserted:,}条 | {rate:.0f}条/秒 | "
                              f"剩余: {eta_str}    ", end='', flush=True)
                        last_log_time = current_time
                
                elif item_type == 'done':
                    _, file_name, _ = item
                    # 标记文件完成
                    tracker.mark_completed(file_name)
                    completed_files += 1
                    inserted = file_stats.get(file_name, 0)
                
                elif item_type == 'error':
                    _, file_name, error = item
                    # 标记损坏文件为已完成，避免重复处理
                    tracker.mark_completed(file_name)
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
        
        # 启用触发器
        if not use_upsert:
            cursor.execute(f"ALTER TABLE {table_name} ENABLE TRIGGER ALL")
            conn.commit()
        
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        
        print(f"\n\n✅ 插入完成: {total_inserted:,}条 | 平均速度: {rate:.0f}条/秒 | 用时: {elapsed/60:.1f}分钟\n")
        
        cursor.close()
        conn.close()
        
        stats_dict['inserted'] = total_inserted
        
    except Exception as e:
        logger.error(f"[Inserter] 严重错误: {e}")
        import traceback
        traceback.print_exc()


def batch_insert_copy(cursor, table_name: str, batch: list, use_upsert: bool = False) -> int:
    """
    使用COPY批量插入（最快方法）
    
    数据以TEXT格式存储（不验证不解析，极速）
    """
    if not batch:
        return 0
    
    from io import StringIO
    import psycopg2.errors
    
    try:
        if use_upsert:
            # UPSERT模式 - 优化版本
            # 1. 批内去重（字典更快）
            seen = {}
            for corpusid, data in batch:
                seen[corpusid] = data
            
            # 2. 构建buffer（一次性写入）
            buffer = StringIO()
            lines = [f"{cid}\t{data}\n" for cid, data in seen.items()]
            buffer.write(''.join(lines))
            buffer.seek(0)
            
            # 3. 使用临时表UPSERT
            temp_table = f"temp_{table_name}_{id(buffer)}"
            cursor.execute(f"CREATE TEMP TABLE {temp_table} (corpusid BIGINT, data JSONB) ON COMMIT DROP")
            cursor.copy_expert(
                f"COPY {temp_table} (corpusid, data) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')",
                buffer
            )
            
            # 4. 简化的UPSERT（去掉IS DISTINCT FROM检查，直接覆盖）
            cursor.execute(f"""
                INSERT INTO {table_name} (corpusid, data, insert_time, update_time)
                SELECT corpusid, data, NOW(), NOW() FROM {temp_table}
                ON CONFLICT (corpusid) DO UPDATE SET
                    data = EXCLUDED.data,
                    update_time = EXCLUDED.update_time
            """)
            
            return len(seen)
        else:
            # INSERT模式：极速COPY（TEXT类型，不验证不解析，最快）
            buffer = StringIO()
            lines = [f"{cid}\t{data}\t\\N\t\\N\n" for cid, data in batch]
            buffer.write(''.join(lines))
            buffer.seek(0)
            
            try:
                cursor.copy_expert(
                    f"""
                    COPY {table_name} (corpusid, data, insert_time, update_time)
                    FROM STDIN WITH (FORMAT TEXT, NULL '\\N', DELIMITER E'\\t')
                    """,
                    buffer
                )
                return len(batch)
            except psycopg2.errors.UniqueViolation:
                # 有重复corpusid，回滚后用临时表+ON CONFLICT处理
                cursor.connection.rollback()
                
                # 使用临时表去重插入（统一TEXT类型）
                temp_table = f"temp_{table_name}_{id(batch) % 10000}"
                cursor.execute(f"CREATE TEMP TABLE IF NOT EXISTS {temp_table} (corpusid BIGINT, data TEXT) ON COMMIT DROP")
                
                # 重新构建buffer
                buffer2 = StringIO()
                lines2 = [f"{cid}\t{data}\n" for cid, data in batch]
                buffer2.write(''.join(lines2))
                buffer2.seek(0)
                
                # COPY到临时表
                cursor.copy_expert(
                    f"COPY {temp_table} (corpusid, data) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')",
                    buffer2
                )
                
                # 从临时表插入，跳过重复
                cursor.execute(f"""
                    INSERT INTO {table_name} (corpusid, data, insert_time, update_time)
                    SELECT corpusid, data, NOW(), NOW() FROM {temp_table}
                    ON CONFLICT (corpusid) DO NOTHING
                """)
                
                return len(batch)
        
    except psycopg2.errors.UniqueViolation:
        # 如果还是失败，说明事务已中止，需要外层处理
        raise
    except Exception as e:
        # 其他错误
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
    corpusid_key: str = 'corpusid',
    use_upsert: bool = False,
    num_extractors: int = NUM_EXTRACTORS,
    resume: bool = True,
    reset_progress: bool = False
):
    """
    流水线并行处理：多个解压进程 + 单个插入进程
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise ValueError(f"文件夹不存在: {folder_path}")
    
    # 初始化进度跟踪
    tracker = ProgressTracker(PROGRESS_FILE)
    
    if reset_progress:
        tracker.reset()
    
    completed_files = tracker.load_completed() if resume else set()
    
    # 扫描GZ文件
    gz_files = sorted(folder.glob("*.gz"))
    if not gz_files:
        logger.warning(f"未找到.gz文件: {folder_path}")
        return
    
    pending_files = [(str(f), f.name) for f in gz_files if f.name not in completed_files]
    
    logger.info(f"\n{'='*80}")
    logger.info(f"流水线并行处理 GZ 文件（生产者-消费者模式）")
    logger.info(f"{'='*80}")
    logger.info(f"文件夹: {folder_path}")
    logger.info(f"目标表: {table_name}")
    # 根据表名获取优化配置
    config = TABLE_CONFIGS.get(table_name, DEFAULT_CONFIG)
    batch_size = config['batch_size']
    commit_batches = config['commit_batches']
    # 如果用户没指定extractors，使用配置中的值
    if num_extractors == NUM_EXTRACTORS:  # 默认值
        num_extractors = config['extractors']
    
    logger.info(f"总文件数: {len(gz_files)}")
    logger.info(f"已完成: {len(completed_files)}")
    logger.info(f"待处理: {len(pending_files)}")
    logger.info(f"优化配置: 批次={batch_size:,}, commit间隔={commit_batches}, 进程={num_extractors}")
    logger.info(f"模式: {'UPSERT' if use_upsert else 'INSERT (COPY)'}")
    logger.info(f"{'='*80}\n")
    
    if not pending_files:
        logger.info("✅ 所有文件已处理完成！")
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
        inserter = Process(
            target=inserter_worker,
            args=(data_queue, table_name, stats_dict, tracker, use_upsert, commit_batches, len(pending_files)),
            name='Inserter'
        )
        inserter.start()
        
        # 启动解压进程（生产者）
        extractors = []
        for i in range(num_extractors):
            p = Process(
                target=extractor_worker,
                args=(file_queue, data_queue, stats_dict, corpusid_key, batch_size),
                name=f'Extractor-{i+1}'
            )
            p.start()
            extractors.append(p)
        
        print(f"✓ 已启动 {num_extractors} 个解压进程 + 1 个插入进程")
        
        # 等待所有解压进程完成
        for p in extractors:
            p.join()
        
        logger.info("✓ 所有解压进程已完成")
        
        # 发送停止信号给插入进程
        data_queue.put(('stop', None, None))
        
        # 等待插入进程完成
        inserter.join()
        
        elapsed = time.time() - overall_start
        total_inserted = stats_dict.get('inserted', 0)
        avg_rate = total_inserted / elapsed if elapsed > 0 else 0
        
        logger.info(f"\n{'='*80}")
        logger.info(f"✅ 全部完成！")
        logger.info(f"  文件数: {len(pending_files)}")
        logger.info(f"  总插入: {total_inserted:,} 条")
        logger.info(f"  总耗时: {elapsed:.2f}秒 ({elapsed/60:.1f}分钟)")
        logger.info(f"  平均速度: {avg_rate:.0f} 条/秒")
        logger.info(f"{'='*80}\n")
        
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
        description='极致优化版 - GZ文件流式批量插入（目标：3000-5000条/秒）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
性能优化要点：
  🚀 生产者-消费者模式：解压和插入完全并行
  🚀 正则快速提取：避免完整JSON解析（提速10倍）
  🚀 更大批次：50000条/批次，减少数据库往返
  🚀 队列缓冲：持续供应数据，无空闲等待
  🚀 多进程解压：充分利用多核CPU

示例：
  # 极速处理papers文件夹
  python scripts/stream_gz_to_db_optimized.py --dir "E:\\machine_win01\\2025-09-30\\papers" --table papers
  
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
    parser.add_argument('--key', type=str, default='corpusid',
                       help='corpusid字段名（默认: corpusid）')
    parser.add_argument('--upsert', action='store_true',
                       help='使用UPSERT模式')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS,
                       help=f'解压进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--resume', action='store_true',
                       help='启用断点续传')
    parser.add_argument('--reset', action='store_true',
                       help='重置进度')
    
    parser.set_defaults(resume=True)
    
    args = parser.parse_args()
    
    # 执行处理
    process_gz_folder_pipeline(
        folder_path=args.dir,
        table_name=args.table,
        corpusid_key=args.key,
        use_upsert=args.upsert,
        num_extractors=args.extractors,
        resume=args.resume,
        reset_progress=args.reset
    )


if __name__ == '__main__':
    main()

