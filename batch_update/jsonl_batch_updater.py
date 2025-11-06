import sys
from pathlib import Path
import os
import shutil

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import orjson  # 高性能JSON解析
import psycopg2
from collections import defaultdict
from tqdm import tqdm
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, Future
from db_config import get_db_config, MACHINE_DB_MAP

# 尝试导入 win32file（Windows 优化）
try:
    import win32file
    HAS_WIN32FILE = True
except ImportError:
    HAS_WIN32FILE = False


# ==================== 配置区（在此修改参数）====================
# 目录配置
FINAL_DELIVERY_DIR = Path("E:/final_delivery")  # JSONL文件目录
LOCAL_TEMP_DIR = Path("D:/jsonl_copy")           # 本地SSD缓存目录
LOG_DIR = Path(__file__).parent.parent / "logs" / "batch_update"
RUNNING_LOG = Path(__file__).parent.parent / "logs" / "running.log"

# 处理模式配置
BATCH_SIZE = 30          # 批量模式：每批处理的文件数（建议10-50）
USE_BATCH_COPY = False    # True=批量复制模式（快），False=逐个文件处理（慢但稳定）

# 数据库配置
TEMP_TABLE = "temp_import"

# 数据集类型配置
SUPPORTED_DATASETS = ['s2orc', 's2orc_v2', 'embeddings_specter_v1', 'embeddings_specter_v2', 'citations']

# 初始化
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_TEMP_DIR.mkdir(parents=True, exist_ok=True)
FAILED_LOG = LOG_DIR / f"jsonl_update_failed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

if not HAS_WIN32FILE:
    raise RuntimeError("未检测到 pywin32。请先安装: pip install pywin32")
# ==============================================================


def log_performance(stage, **metrics):
    """记录性能日志到running.log"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    metrics_str = " | ".join([f"{k}={v}" for k, v in metrics.items()])
    log_line = f"[{timestamp}] {stage} | {metrics_str}\n"
    with open(RUNNING_LOG, 'a', encoding='utf-8') as f:
        f.write(log_line)


def batch_copy_with_powershell(filenames: list[str], src_dir: Path, dst_dir: Path) -> tuple[float, int]:
    """使用 PowerShell 批量复制文件
    
    Args:
        filenames: 文件名列表
        src_dir: 源目录
        dst_dir: 目标目录
    
    Returns:
        (耗时(秒), 成功复制数)
    """
    if not filenames:
        return 0.0, 0
    
    import subprocess
    import tempfile
    
    # 创建临时文件列表
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        for fname in filenames:
            f.write(f"{fname}\n")
        temp_list_file = f.name
    
    try:
        ps_script = Path(__file__).parent / "batch_copy.ps1"
        ps_executable = shutil.which("pwsh") or shutil.which("powershell")
        
        ps_cmd = [
            ps_executable, "-NoProfile", "-ExecutionPolicy", "Bypass",
            "-File", str(ps_script),
            "-FileListPath", temp_list_file,
            "-SourceDir", str(src_dir),
            "-DestDir", str(dst_dir)
        ]
        
        t0 = time.time()
        # subprocess.run 会自动等待进程完成并清理，不会产生僵尸进程
        result = subprocess.run(ps_cmd, capture_output=True, text=True, timeout=600)
        elapsed_time = time.time() - t0
        
        # 解析输出格式：SUCCESS:123
        success_count = 0
        for line in result.stdout.splitlines():
            if line.strip().startswith("SUCCESS:"):
                success_count = int(line.split(":", 1)[1])
                break
        
        return elapsed_time, success_count
    
    finally:
        # 清理临时文件
        Path(temp_list_file).unlink(missing_ok=True)


class JSONLBatchUpdater:
    """JSONL批量更新器"""
    
    def __init__(self, machine_id, dataset):
        self.machine_id = machine_id
        self.dataset = dataset
        self.db_config = get_db_config(machine_id)
        self.conn = None
        self.failed_corpusids = []
        # 线程池：用于后台复制文件（最多1个并发）
        self.copy_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="FileCopy")
    
    def connect_db(self):
        """连接数据库"""
        print(f"连接到数据库 [{self.machine_id}: {self.db_config['database']}:{self.db_config['port']}]...")
        self.conn = psycopg2.connect(**self.db_config)
    
    def close_db(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
    
    def copy_file_to_local_bg(self, filename: str) -> str:
        """后台复制文件到本地SSD（在线程池中执行）
        
        Args:
            filename: 文件名
        
        Returns:
            filename: 成功返回文件名，失败抛出异常
        """
        file_path = FINAL_DELIVERY_DIR / filename
        local_cache = LOCAL_TEMP_DIR / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"源文件不存在: {file_path}")
        
        # 使用win32file高速复制
        win32file.CopyFile(str(file_path), str(local_cache), 0)
        return filename
    
    def ensure_indexes(self):
        """确保必要索引存在（用于JOIN和过滤）"""
        cursor = self.conn.cursor()
        start_ts = time.time()
        
        # 检查 corpusid_to_file 映射表是否存在
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'corpusid_to_file');")
        if not cursor.fetchone()[0]:
            raise RuntimeError("映射表 corpusid_to_file 不存在！需要先创建该表。")
        
        # 创建必要索引（如已存在则跳过）
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TEMP_TABLE}_corpusid ON {TEMP_TABLE} (corpusid);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TEMP_TABLE}_is_done ON {TEMP_TABLE} (is_done);")
        cursor.execute(f"ANALYZE {TEMP_TABLE};")
        
        self.conn.commit()
        elapsed = time.time() - start_ts
        
        # 记录日志
        log_performance("创建索引", time_sec=f"{elapsed:.2f}")
        
        return {"time": elapsed}

    def get_grouped_corpusids(self):
        """获取待更新记录并按文件名分组（只查询corpusid，按需获取data）
        
        Returns:
            (按文件分组的corpusid字典, 总记录数, 性能统计)
        """
        cursor = self.conn.cursor()
        sql_start = time.time()
        
        # 只查询 corpusid 和 batch_filename（轻量级查询）
        cursor.execute(f"""
            SELECT 
                m.batch_filename,
                t.corpusid
            FROM {TEMP_TABLE} t
            INNER JOIN corpusid_to_file m ON t.corpusid = m.corpusid
            WHERE t.is_done = FALSE;
        """)
        
        sql_time = time.time() - sql_start

        # 按文件名分组（只存corpusid列表）
        group_start = time.time()
        grouped = defaultdict(list)
        total_count = 0
        
        for batch_filename, corpusid in cursor:
            grouped[batch_filename].append(corpusid)
            total_count += 1
        
        group_time = time.time() - group_start

        # 记录日志
        log_performance("SQL查询", time_sec=f"{sql_time:.2f}", records=total_count)
        log_performance("Python分组", time_sec=f"{group_time:.2f}", files=len(grouped))

        stats = {"sql_time": sql_time, "group_time": group_time}
        return grouped, total_count, stats
    
    def get_file_updates(self, corpusid_list):
        """按需获取指定文件的更新数据
        
        Args:
            corpusid_list: corpusid列表
        
        Returns:
            [(corpusid, data), ...] 更新数据列表
        """
        if not corpusid_list:
            return []
        
        cursor = self.conn.cursor()
        
        # 按需查询data字段（使用索引，速度快）
        cursor.execute(f"""
            SELECT corpusid, data
            FROM {TEMP_TABLE}
            WHERE corpusid = ANY(%s) AND is_done = FALSE;
        """, (corpusid_list,))
        
        return cursor.fetchall()
    
    @staticmethod
    def _extract_corpusid(line: bytes) -> int | None:
        """从JSONL行快速提取corpusid（避免完整JSON解析）"""
        # 查找 "corpusid": 后的数字
        idx = line.find(b'"corpusid"')
        if idx == -1:
            return None
        idx = line.find(b":", idx)
        if idx == -1:
            return None
        idx += 1
        # 跳过空格
        while idx < len(line) and line[idx:idx+1] in b" \t":
            idx += 1
        # 提取数字
        end = idx
        while end < len(line) and line[end:end+1].isdigit():
            end += 1
        if end == idx:
            return None
        try:
            return int(line[idx:end])
        except ValueError:
            return None
    
    def _apply_update(self, record: dict, data: str) -> bool:
        """根据数据集类型应用更新到记录
        
        Args:
            record: JSONL记录（dict）
            data: 临时表中的data字段值
        
        Returns:
            bool: 是否成功更新（True=已更新，False=跳过）
        """
        if self.dataset in ['s2orc', 's2orc_v2']:
            # s2orc类型：更新content字段
            # 只有当 content 为空字典时才更新
            if record.get('content') and record['content'] != {}:
                return False  # content已有值，跳过
            
            # 尝试解析data（可能是JSON字符串）
            try:
                content_data = orjson.loads(data) if isinstance(data, (str, bytes)) else data
            except:
                content_data = data
            
            # 优先使用content字段，否则组合body + bibliography
            if isinstance(content_data, dict):
                if 'content' in content_data and content_data['content']:
                    record['content'] = content_data['content']
                else:
                    # 组合 body + bibliography
                    combined = {}
                    if 'body' in content_data:
                        combined.update(content_data.get('body', {}))
                    if 'bibliography' in content_data:
                        combined['bibliography'] = content_data.get('bibliography', {})
                    if combined:
                        record['content'] = combined
            else:
                # data直接是字符串，直接赋值
                record['content'] = content_data
            
            return True
        
        elif self.dataset in ['embeddings_specter_v1', 'embeddings_specter_v2']:
            # embedding类型：更新embedding字段
            # 只有当 embedding 为空字典时才更新
            if record.get('embedding') and record['embedding'] != {}:
                return False  # embedding已有值，跳过
            
            # 确定model名称
            model_name = 'specter_v1' if self.dataset == 'embeddings_specter_v1' else 'specter_v2'
            
            # 构造embedding结构（vector直接使用data字段值）
            record['embedding'] = {
                "model": model_name,
                "vector": data
            }
            return True
        
        elif self.dataset == 'citations':
            # citations类型：暂时不处理（逻辑待定）
            return False
        
        else:
            # 未知类型，跳过处理
            return False

    
    def update_jsonl_file_from_local(self, filename, updates, skip_copy_in=False, skip_copy_out=False, copy_in_time=0):
        """在本地SSD上更新JSONL文件
        
        Args:
            filename: 文件名
            updates: [(corpusid, data), ...] 更新列表
            skip_copy_in: 是否跳过复制入（流水线模式时已后台复制）
            skip_copy_out: 是否跳过复制出（批量模式时批量复制回）
            copy_in_time: 复制入耗时（流水线模式传入）
        
        Returns:
            (成功数, 失败corpusid列表)
        """
        file_path = FINAL_DELIVERY_DIR / filename
        if not file_path.exists():
            return (0, [cid for cid, _ in updates])
        
        updates_dict = {cid: data for cid, data in updates}
        if not updates_dict:
            return (0, [])
        
        local_cache = LOCAL_TEMP_DIR / filename
        local_output = LOCAL_TEMP_DIR / f"{filename}.out"
        
        process_time = 0
        copy_out_time = 0
        
        # 步骤1: 复制到本地SSD（如果未跳过）
        if not skip_copy_in:
            t0 = time.time()
            win32file.CopyFile(str(file_path), str(local_cache), 0)
            copy_in_time = time.time() - t0
        
        # 步骤2: 在本地SSD上处理文件（优化版）
        t0 = time.time()
        
        # 一次性读取整个文件（32GB内存足够）
        with open(local_cache, 'rb') as fin:
            content = fin.read()
        
        # 按行分割（保留最后的空行信息）
        lines = content.split(b'\n')
        has_trailing_newline = content.endswith(b'\n')
        
        updated_corpusids = []
        output_lines = []
        
        # 批量处理所有行
        for line in lines:
            if not line:  # 空行直接跳过
                continue
            
            corpusid = self._extract_corpusid(line)
            
            if corpusid in updates_dict:
                # 需要更新：解析JSON → 应用更新 → 序列化
                record = orjson.loads(line)
                data = updates_dict[corpusid]
                
                # 根据数据集类型应用更新
                if self._apply_update(record, data):
                    updated_corpusids.append(corpusid)
                    output_lines.append(orjson.dumps(record))
                else:
                    # 跳过更新（字段已有值），保留原样
                    output_lines.append(line)
            else:
                # 不需要更新：原样保留
                output_lines.append(line)
        
        # 一次性写入（避免多次I/O）
        with open(local_output, 'wb', buffering=16*1024*1024) as fout:
            fout.write(b'\n'.join(output_lines))
            if has_trailing_newline and output_lines:
                fout.write(b'\n')
        
        process_time = time.time() - t0
        
        # 步骤3: 复制回外置盘（批量模式时统一复制）
        if not skip_copy_out:
            t0 = time.time()
            temp_file = file_path.with_suffix('.tmp')
            win32file.CopyFile(str(local_output), str(temp_file), 0)
            temp_file.replace(file_path)
            copy_out_time = time.time() - t0
            local_cache.unlink(missing_ok=True)
            local_output.unlink(missing_ok=True)
        
        # 记录日志（逐个文件模式或流水线模式）
        if not skip_copy_out:  # 批量模式不记录
            log_performance(
                "单文件处理",
                file=filename,
                copy_in_sec=f"{copy_in_time:.2f}",
                process_sec=f"{process_time:.2f}",
                copy_out_sec=f"{copy_out_time:.2f}",
                updates=len(updated_corpusids)
            )
        
        failed = [cid for cid in updates_dict.keys() if cid not in updated_corpusids]
        return (len(updated_corpusids), failed)
    
    
    def mark_as_done(self, corpusids):
        """标记corpusid为已处理"""
        if not corpusids:
            return
        
        cursor = self.conn.cursor()
        t0 = time.time()
        
        # 批量更新
        cursor.execute(f"""
            UPDATE {TEMP_TABLE} 
            SET is_done = TRUE 
            WHERE corpusid = ANY(%s)
        """, (corpusids,))
        
        self.conn.commit()
        elapsed = time.time() - t0
        
        # 记录日志
        log_performance("数据库更新", records=len(corpusids), time_sec=f"{elapsed:.2f}")
    
    def run(self):
        """执行批量更新（配置参数见文件顶部配置区）"""
        print("=" * 80)
        print("JSONL批量更新器")
        print("=" * 80)
        mode = f"批量复制模式（每批{BATCH_SIZE}个文件）" if USE_BATCH_COPY else "逐个文件处理模式"
        print(f"  处理模式: {mode}")
        print(f"  数据集类型: {self.dataset}")
        
        overall_start = time.time()
        
        try:
            # ========== 准备阶段 ==========
            db_start = time.time()
            print("\n【准备阶段】")
            
            print("  [1/3] 连接数据库...", end='', flush=True)
            self.connect_db()
            print(f" 完成 ({time.time() - db_start:.2f}秒)")
            
            print("  [2/3] 检查索引...", end='', flush=True)
            self.ensure_indexes()
            print(f" 完成")
            
            print("  [3/3] 查询并分组数据...", end='', flush=True)
            grouped_corpusids, total_count, stats = self.get_grouped_corpusids()
            print(f" 完成")
            
            if not grouped_corpusids:
                print("  没有待处理的记录，退出")
                return
            
            prep_time = time.time() - db_start
            print(f"  待处理记录: {total_count} 条")
            print(f"  分组结果: {len(grouped_corpusids)} 个文件")
            print(f"  准备耗时: {prep_time:.2f}秒")
            
            # ========== 更新阶段 ==========
            update_start = time.time()
            print(f"\n【更新阶段】")
            
            total_success = 0
            total_failed = 0
            filenames_list = list(grouped_corpusids.keys())
            
            # 根据配置确定批次大小
            if USE_BATCH_COPY:
                # 批量复制模式：N个文件一批
                batch_size = BATCH_SIZE
                total_batches = (len(filenames_list) + batch_size - 1) // batch_size
            else:
                # 逐个文件模式：每次处理1个文件
                batch_size = 1
                total_batches = len(filenames_list)
            
            # 流水线模式：追踪下一个文件的后台复制任务
            next_copy_future = None
            
            with tqdm(total=len(filenames_list), desc="  处理文件") as pbar:
                for batch_idx in range(total_batches):
                    batch_start = batch_idx * batch_size
                    batch_end = min(batch_start + batch_size, len(filenames_list))
                    batch_filenames = filenames_list[batch_start:batch_end]
                    
                    if USE_BATCH_COPY:
                        # ========== 批量复制模式 ==========
                        print(f"\n  批次 {batch_idx + 1}/{total_batches}: 批量复制 {len(batch_filenames)} 个文件...")
                        
                        # 步骤1: 批量复制到本地SSD
                        copy_in_time, _ = batch_copy_with_powershell(
                            batch_filenames, FINAL_DELIVERY_DIR, LOCAL_TEMP_DIR
                        )
                        
                        # 步骤2: 在本地SSD上处理每个文件
                        for filename in batch_filenames:
                            if not (LOCAL_TEMP_DIR / filename).exists():
                                pbar.update(1)
                                continue
                            
                            # 按需获取该文件的更新数据
                            corpusid_list = grouped_corpusids[filename]
                            updates = self.get_file_updates(corpusid_list)
                            
                            success_count, failed = self.update_jsonl_file_from_local(
                                filename, updates, skip_copy_in=True, skip_copy_out=True
                            )
                            
                            total_success += success_count
                            total_failed += len(failed)
                            
                            # 标记已成功更新的记录
                            success_ids = [cid for cid, _ in updates if cid not in failed]
                            if success_ids:
                                self.mark_as_done(success_ids)
                            
                            # 记录失败的corpusid
                            if failed:
                                self.failed_corpusids.extend(failed)
                                with open(FAILED_LOG, 'a', encoding='utf-8') as f:
                                    for cid in failed:
                                        f.write(f"{cid}\t{filename}\tFAILED\n")
                            
                            pbar.update(1)
                        
                        # 步骤3: 批量复制回外置盘
                        out_filenames = [f"{fname}.out" for fname in batch_filenames]
                        copy_out_time, _ = batch_copy_with_powershell(
                            out_filenames, LOCAL_TEMP_DIR, FINAL_DELIVERY_DIR
                        )
                        
                        # 步骤4: 替换原文件并清理本地文件
                        for fname in batch_filenames:
                            temp_file = FINAL_DELIVERY_DIR / f"{fname}.out"
                            if temp_file.exists():
                                temp_file.replace(FINAL_DELIVERY_DIR / fname)
                                (LOCAL_TEMP_DIR / fname).unlink(missing_ok=True)
                                (LOCAL_TEMP_DIR / f"{fname}.out").unlink(missing_ok=True)
                    
                    else:
                        # ========== 逐个文件处理模式（流水线并行优化）==========
                        filename = batch_filenames[0]
                        
                        # === 阶段1：等待/执行当前文件复制 ===
                        copy_in_time = 0
                        
                        if batch_idx == 0:
                            # 第一个文件：同步复制（冷启动）
                            t0 = time.time()
                            try:
                                self.copy_file_to_local_bg(filename)
                                copy_in_time = time.time() - t0
                            except Exception as e:
                                print(f"\n  ✗ 复制失败 {filename}: {e}")
                                pbar.update(1)
                                continue
                        else:
                            # 后续文件：等待后台复制完成
                            t0 = time.time()
                            try:
                                next_copy_future.result()  # 阻塞等待
                                copy_in_time = time.time() - t0
                            except Exception as e:
                                print(f"\n  ✗ 后台复制失败 {filename}: {e}")
                                pbar.update(1)
                                continue
                        
                        # === 阶段2：启动下一个文件的后台复制 + 处理当前文件 ===
                        # 启动下一个文件的后台复制
                        if batch_idx < total_batches - 1:
                            next_filename = filenames_list[batch_idx + 1]
                            next_copy_future = self.copy_executor.submit(
                                self.copy_file_to_local_bg, next_filename
                            )
                        
                        # 按需获取该文件的更新数据
                        corpusid_list = grouped_corpusids[filename]
                        updates = self.get_file_updates(corpusid_list)
                        
                        # === 阶段3：等待下一个文件复制完成（如果有）===
                        if next_copy_future:
                            try:
                                next_copy_future.result()  # 阻塞等待
                            except Exception as e:
                                # 下一个文件复制失败，记录但不影响当前文件
                                print(f"\n  ⚠️  下一个文件后台复制失败: {e}")
                        
                        # === 阶段4：处理并写回当前文件 ===
                        # 处理文件（已在本地SSD）
                        success_count, failed = self.update_jsonl_file_from_local(
                            filename, updates, skip_copy_in=True, skip_copy_out=False, 
                            copy_in_time=copy_in_time
                        )
                        
                        total_success += success_count
                        total_failed += len(failed)
                        
                        # 标记已成功更新的记录
                        success_ids = [cid for cid, _ in updates if cid not in failed]
                        if success_ids:
                            self.mark_as_done(success_ids)
                        
                        # 记录失败的corpusid
                        if failed:
                            self.failed_corpusids.extend(failed)
                            with open(FAILED_LOG, 'a', encoding='utf-8') as f:
                                for cid in failed:
                                    f.write(f"{cid}\t{filename}\tFAILED\n")
                        
                        pbar.update(1)
            
            update_time = time.time() - update_start
            total_time = time.time() - overall_start
            
            print("\n" + "=" * 80)
            print("【完成统计】")
            print("=" * 80)
            print(f"  成功更新: {total_success} 条")
            print(f"  失败记录: {total_failed} 条")
            print(f"  处理文件: {len(grouped_corpusids)} 个")
            print(f"  准备阶段: {prep_time:.2f}秒")
            print(f"  更新阶段: {update_time:.2f}秒")
            print(f"  总耗时: {total_time:.2f}秒")
            if update_time > 0:
                print(f"  更新速度: {total_success/update_time:.0f} 条/秒")
            print("=" * 80)
            
            if self.failed_corpusids:
                print(f"\n失败记录已保存到: {FAILED_LOG}")
        
        finally:
            # 清理线程池
            self.copy_executor.shutdown(wait=True)
            # 清理数据库连接
            self.close_db()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="JSONL批量更新器")
    parser.add_argument("--machine", required=True, choices=list(MACHINE_DB_MAP.keys()), 
                        help="目标机器（必需）")
    parser.add_argument("--dataset", required=True, choices=SUPPORTED_DATASETS,
                        help="数据集类型（必需）")
    args = parser.parse_args()
    
    # 配置参数见文件顶部配置区
    updater = JSONLBatchUpdater(machine_id=args.machine, dataset=args.dataset)
    updater.run()

