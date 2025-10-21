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
# 配置优化
# =============================================================================

BATCH_SIZE = 150000                     # 更大批次，减少commit次数
QUEUE_SIZE = 20                         # 队列容量（减小，节省内存）
NUM_EXTRACTORS = 8                      # 解压进程数（推荐8个）
PROGRESS_FILE = 'logs/gz_progress.txt'
COMMIT_BATCHES = 3                      # 每3个批次commit一次（减少commit开销）

# 正则表达式：快速提取corpusid（比完整JSON解析快10倍）
CORPUSID_PATTERN = re.compile(r'"corpusid"\s*:\s*(\d+)', re.IGNORECASE)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(processName)s] - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


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
    corpusid_key: str = 'corpusid'
):
    """
    解压工作进程（生产者）
    从file_queue取文件，解压后放入data_queue
    """
    worker_name = f"Extractor-{id(file_queue) % 1000}"
    
    while True:
        try:
            # 获取文件任务
            task = file_queue.get(timeout=1)
            if task is None:  # 结束信号
                break
            
            gz_file_path, file_name = task
            start_time = time.time()
            
            logger.info(f"[{worker_name}] 🔄 解压: {file_name}")
            
            try:
                batch = []
                line_count = 0
                valid_count = 0
                
                # 流式解压
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
                        
                        # 零拷贝：直接使用原始JSON字符串
                        # 转义特殊字符用于COPY
                        json_escaped = line.replace('\\', '\\\\').replace('\n', '\\n').replace('\t', '\\t')
                        
                        batch.append((corpusid, json_escaped))
                        
                        # 批次满了，发送到队列
                        if len(batch) >= BATCH_SIZE:
                            data_queue.put(('data', file_name, batch))
                            batch = []
                
                # 发送剩余数据
                if batch:
                    data_queue.put(('data', file_name, batch))
                
                # 发送文件完成信号
                data_queue.put(('done', file_name, valid_count))
                
                elapsed = time.time() - start_time
                rate = valid_count / elapsed if elapsed > 0 else 0
                
                logger.info(f"[{worker_name}] ✅ {file_name}: 提取 {valid_count:,} 条 ({rate:.0f}条/秒)")
                
                # 更新统计
                stats_dict['extracted'] = stats_dict.get('extracted', 0) + valid_count
                
            except Exception as e:
                logger.error(f"[{worker_name}] ❌ {file_name}: {e}")
                data_queue.put(('error', file_name, str(e)))
        
        except Empty:
            continue
        except Exception as e:
            logger.error(f"[{worker_name}] 异常: {e}")
            break


# =============================================================================
# 消费者：插入进程（单个，高效批量插入）
# =============================================================================

def inserter_worker(
    data_queue: Queue,
    table_name: str,
    stats_dict: dict,
    tracker: ProgressTracker,
    use_upsert: bool = False
):
    """
    插入工作进程（消费者）
    持续从data_queue取数据并批量插入
    """
    logger.info("[Inserter] 🚀 启动插入进程")
    
    try:
        # 创建数据库连接
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()
        
        # 禁用触发器（INSERT模式）
        if not use_upsert:
            cursor.execute(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL")
            conn.commit()
            logger.info("[Inserter] ✓ 触发器已禁用")
        
        total_inserted = 0
        file_stats = {}  # {file_name: inserted_count}
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
                    
                    # 批量插入
                    inserted = batch_insert_copy(cursor, table_name, batch, use_upsert)
                    batch_count += 1
                    
                    # 每N个批次commit一次，减少commit开销
                    if batch_count >= COMMIT_BATCHES:
                        conn.commit()
                        batch_count = 0
                    
                    total_inserted += inserted
                    file_stats[file_name] = file_stats.get(file_name, 0) + inserted
                    
                    # 定期输出进度（每10秒）
                    current_time = time.time()
                    if current_time - last_log_time >= 10:
                        elapsed = current_time - start_time
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        logger.info(f"[Inserter] 📊 已插入: {total_inserted:,} 条 ({rate:.0f}条/秒) | 队列: {data_queue.qsize()}")
                        last_log_time = current_time
                
                elif item_type == 'done':
                    _, file_name, _ = item
                    # 标记文件完成
                    tracker.mark_completed(file_name)
                    inserted = file_stats.get(file_name, 0)
                    logger.info(f"[Inserter] ✅ {file_name}: 已完成 ({inserted:,} 条)")
                
                elif item_type == 'error':
                    _, file_name, error = item
                    logger.warning(f"[Inserter] ⚠️  {file_name}: 提取失败 - {error}")
            
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
            logger.info("[Inserter] ✓ 触发器已启用")
        
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        
        logger.info(f"\n[Inserter] 🏁 插入完成: {total_inserted:,} 条，平均 {rate:.0f} 条/秒\n")
        
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
    """
    if not batch:
        return 0
    
    try:
        if use_upsert:
            # UPSERT模式
            from io import StringIO
            buffer = StringIO()
            for corpusid, json_str in batch:
                buffer.write(f"{corpusid}\t{json_str}\n")
            buffer.seek(0)
            
            cursor.execute("CREATE TEMP TABLE IF NOT EXISTS temp_batch (corpusid BIGINT, data JSONB) ON COMMIT DROP")
            cursor.copy_expert(
                "COPY temp_batch (corpusid, data) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')",
                buffer
            )
            cursor.execute(f"""
                INSERT INTO {table_name} (corpusid, data)
                SELECT corpusid, data FROM temp_batch
                ON CONFLICT (corpusid) DO UPDATE SET
                    data = EXCLUDED.data,
                    update_time = NOW()
                WHERE {table_name}.data IS DISTINCT FROM EXCLUDED.data
            """)
        else:
            # INSERT模式：直接COPY（最快）
            from io import StringIO
            buffer = StringIO()
            for corpusid, json_str in batch:
                buffer.write(f"{corpusid}\t{json_str}\t\\N\t\\N\n")
            buffer.seek(0)
            
            cursor.copy_expert(
                f"""
                COPY {table_name} (corpusid, data, insert_time, update_time)
                FROM STDIN WITH (FORMAT TEXT, NULL '\\N', DELIMITER E'\\t')
                """,
                buffer
            )
        
        return len(batch)
        
    except Exception as e:
        logger.error(f"批量插入失败: {e}")
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
    logger.info(f"总文件数: {len(gz_files)}")
    logger.info(f"已完成: {len(completed_files)}")
    logger.info(f"待处理: {len(pending_files)}")
    logger.info(f"解压进程: {num_extractors}")
    logger.info(f"批次大小: {BATCH_SIZE:,}")
    logger.info(f"队列容量: {QUEUE_SIZE} 批次")
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
            args=(data_queue, table_name, stats_dict, tracker, use_upsert),
            name='Inserter'
        )
        inserter.start()
        
        # 启动解压进程（生产者）
        extractors = []
        for i in range(num_extractors):
            p = Process(
                target=extractor_worker,
                args=(file_queue, data_queue, stats_dict, corpusid_key),
                name=f'Extractor-{i+1}'
            )
            p.start()
            extractors.append(p)
        
        logger.info(f"✓ 已启动 {num_extractors} 个解压进程 + 1 个插入进程\n")
        
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

