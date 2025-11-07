import sys
from pathlib import Path
import os
import shutil
import mmap

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import orjson  # 高性能JSON解析
import psycopg2
from psycopg2 import OperationalError
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

# 多机器模式：最大重试次数
MAX_RETRIES = 3
RETRY_DELAY = 2  # 秒

# 性能优化配置
BATCH_MARK_SIZE = 10  # 批量标记is_done的批次大小

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
    """JSONL批量更新器（支持单机器和多机器模式）"""
    
    def __init__(self, machine_id=None, machine_list=None):
        """
        初始化更新器
        
        Args:
            machine_id: 单机器模式的机器ID
            machine_list: 多机器模式的机器列表（如['machine0', 'machine2']）
        """
        # 判断运行模式
        if machine_list:
            # 多机器模式
            self.mode = 'multi'
            self.machine_list = machine_list
            self.primary_machine = machine_list[0]  # 主机器（连接外置硬盘）
            self.connections = {}  # {machine_id: conn}
            self.cursors = {}  # {machine_id: cursor}
        else:
            # 单机器模式
            self.mode = 'single'
            self.machine_list = [machine_id]
            self.primary_machine = machine_id
            self.connections = {}
            self.cursors = {}
        
        self.failed_corpusids = []
        # 线程池：用于后台复制文件和流水线查询（支持并发copy+query）
        self.copy_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="Pipeline")
    
    def _connect_single_db(self, machine_id):
        """连接单个数据库（带重试机制）"""
        db_config = get_db_config(machine_id)
        
        for attempt in range(MAX_RETRIES):
            try:
                conn = psycopg2.connect(**db_config)
                cursor = conn.cursor()
                return machine_id, conn, cursor, None
            except OperationalError as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    return machine_id, None, None, str(e)
        
        return machine_id, None, None, "Unknown error"
    
    def connect_db(self):
        """连接所有数据库（支持多机器模式并行连接，带重试机制）"""
        if len(self.machine_list) == 1:
            # 单机器模式：直接连接
            machine_id = self.machine_list[0]
            db_config = get_db_config(machine_id)
            print(f"连接到数据库 [{machine_id}: {db_config['database']}:{db_config['port']}]...", end=' ', flush=True)
            
            mid, conn, cursor, error = self._connect_single_db(machine_id)
            if error:
                print(f"✗ 连接失败")
                raise RuntimeError(f"无法连接到 {machine_id}: {error}")
            
            self.connections[mid] = conn
            self.cursors[mid] = cursor
            print("✓")
        else:
            # 多机器模式：并行连接
            print(f"并行连接 {len(self.machine_list)} 台数据库...")
            
            with ThreadPoolExecutor(max_workers=len(self.machine_list)) as executor:
                futures = {executor.submit(self._connect_single_db, mid): mid 
                          for mid in self.machine_list}
                
                for future in futures:
                    machine_id, conn, cursor, error = future.result()
                    db_config = get_db_config(machine_id)
                    
                    if error:
                        print(f"  [{machine_id}: {db_config['database']}:{db_config['port']}] ✗ 失败")
                        raise RuntimeError(f"无法连接到 {machine_id}: {error}")
                    
                    self.connections[machine_id] = conn
                    self.cursors[machine_id] = cursor
                    print(f"  [{machine_id}: {db_config['database']}:{db_config['port']}] ✓")
    
    def close_db(self):
        """关闭所有数据库连接"""
        for machine_id, conn in self.connections.items():
            if conn:
                conn.close()
        for machine_id, cursor in self.cursors.items():
            if cursor:
                cursor.close()
    
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
        """确保必要索引存在（在所有机器上执行）"""
        start_ts = time.time()
        
        for machine_id in self.machine_list:
            cursor = self.cursors[machine_id]
            conn = self.connections[machine_id]
            
            # 创建必要索引（如已存在则跳过）
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TEMP_TABLE}_corpusid ON {TEMP_TABLE} (corpusid);")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TEMP_TABLE}_is_done ON {TEMP_TABLE} (is_done);")
            cursor.execute(f"ANALYZE {TEMP_TABLE};")
            conn.commit()
        
        # 检查 corpusid_to_file 映射表是否存在（只在主机器上检查）
        primary_cursor = self.cursors[self.primary_machine]
        primary_cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'corpusid_to_file');")
        if not primary_cursor.fetchone()[0]:
            raise RuntimeError("映射表 corpusid_to_file 不存在！需要先创建该表。")
        
        elapsed = time.time() - start_ts
        log_performance("创建索引", machines=','.join(self.machine_list), time_sec=f"{elapsed:.2f}")
        
        return {"time": elapsed}

    def get_grouped_corpusids(self):
        """获取待更新记录并按文件名分组（支持多机器模式）
        
        Returns:
            单机器模式: (按文件分组的corpusid列表字典, 总记录数, 性能统计)
            多机器模式: (按文件分组的(corpusid, sources)元组列表字典, 总记录数, 性能统计)
        """
        sql_start = time.time()
        
        if self.mode == 'single':
            # 单机器模式（向后兼容）
            cursor = self.cursors[self.primary_machine]
            
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
            log_performance("SQL查询", time_sec=f"{sql_time:.2f}", records=total_count)
            log_performance("Python分组", time_sec=f"{group_time:.2f}", files=len(grouped))
            
            stats = {"sql_time": sql_time, "group_time": group_time}
            return grouped, total_count, stats
        
        else:
            # 多机器模式：分布式并行查询并合并
            # Step 1: 并行查询每台机器的corpusid
            def query_machine_corpusids(machine_id):
                """查询单台机器的corpusid"""
                cursor = self.cursors[machine_id]
                cursor.execute(f"SELECT corpusid FROM {TEMP_TABLE} WHERE is_done = FALSE;")
                corpusids = [row[0] for row in cursor.fetchall()]
                return machine_id, corpusids
            
            all_corpusids_with_source = {}  # {corpusid: [machine_ids]}
            machine_stats = {}
            
            # 并行查询所有机器
            with ThreadPoolExecutor(max_workers=len(self.machine_list)) as executor:
                futures = {executor.submit(query_machine_corpusids, mid): mid 
                          for mid in self.machine_list}
                
                for future in futures:
                    machine_id, corpusids = future.result()
                    machine_stats[machine_id] = len(corpusids)
                    
                    for corpusid in corpusids:
                        if corpusid not in all_corpusids_with_source:
                            all_corpusids_with_source[corpusid] = []
                        all_corpusids_with_source[corpusid].append(machine_id)
            
            sql_time = time.time() - sql_start
            
            # Step 2: 与 corpusid_to_file 关联（在主机器上执行）
            group_start = time.time()
            primary_cursor = self.cursors[self.primary_machine]
            
            all_corpusids = list(all_corpusids_with_source.keys())
            if not all_corpusids:
                return {}, 0, {"sql_time": sql_time, "group_time": 0, "machine_stats": machine_stats, "overlap": 0}
            
            primary_cursor.execute(f"""
                SELECT m.batch_filename, m.corpusid
                FROM corpusid_to_file m
                WHERE m.corpusid = ANY(%s);
            """, (all_corpusids,))
            
            # 按文件名分组（存储(corpusid, sources)元组）
            grouped = defaultdict(list)
            total_count = 0
            
            for batch_filename, corpusid in primary_cursor:
                sources = all_corpusids_with_source[corpusid]
                grouped[batch_filename].append((corpusid, sources))
                total_count += 1
            
            group_time = time.time() - group_start
            
            # 统计重复corpusid（同时存在于多台机器）
            overlap_count = sum(1 for sources in all_corpusids_with_source.values() if len(sources) > 1)
            
            # 记录日志
            log_performance("分布式SQL查询", 
                           time_sec=f"{sql_time:.2f}", 
                           **{f"{mid}_records": cnt for mid, cnt in machine_stats.items()})
            log_performance("Python分组", 
                           time_sec=f"{group_time:.2f}", 
                           files=len(grouped),
                           overlap=overlap_count)
            
            stats = {
                "sql_time": sql_time, 
                "group_time": group_time,
                "machine_stats": machine_stats,
                "overlap": overlap_count
            }
            return grouped, total_count, stats
    
    def get_file_updates(self, corpusid_list_or_tuples):
        """按需获取指定文件的更新数据（支持多机器模式）
        
        Args:
            单机器模式: corpusid_list - corpusid列表
            多机器模式: corpusid_with_sources - [(corpusid, sources), ...] 元组列表
        
        Returns:
            {corpusid: {'specter_v1': ..., 'specter_v2': ..., 'content': ...}, ...} 合并后的数据字典
        """
        if not corpusid_list_or_tuples:
            return {}
        
        if self.mode == 'single':
            # 单机器模式：查询所有字段（与多机器模式统一）
            cursor = self.cursors[self.primary_machine]
            
            cursor.execute(f"""
                SELECT corpusid, specter_v1, specter_v2, content, citations, references
                FROM {TEMP_TABLE}
                WHERE corpusid = ANY(%s) AND is_done = FALSE;
            """, (corpusid_list_or_tuples,))
            
            # 构造与多机器模式相同的返回格式
            updates_merged = {}
            for corpusid, specter_v1, specter_v2, content, citations, references in cursor:
                if corpusid not in updates_merged:
                    updates_merged[corpusid] = {}
                
                # 只记录非空字段（反序列化JSON字符串）
                if specter_v1:
                    try:
                        updates_merged[corpusid]['specter_v1'] = orjson.loads(specter_v1)
                    except:
                        pass
                
                if specter_v2:
                    try:
                        updates_merged[corpusid]['specter_v2'] = orjson.loads(specter_v2)
                    except:
                        pass
                
                if content:
                    try:
                        updates_merged[corpusid]['content'] = orjson.loads(content)
                    except:
                        pass
                
                if citations:
                    try:
                        updates_merged[corpusid]['citations'] = orjson.loads(citations)
                    except:
                        pass
                
                if references:
                    try:
                        updates_merged[corpusid]['references'] = orjson.loads(references)
                    except:
                        pass
            
            return updates_merged
        
        else:
            # 多机器模式：分别查询并合并数据
            # Step 1: 按机器分离corpusid
            machine_corpusids = defaultdict(list)
            for corpusid, sources in corpusid_list_or_tuples:
                for machine_id in sources:
                    machine_corpusids[machine_id].append(corpusid)
            
            # Step 2: 并行查询各机器
            updates_merged = {}
            
            for machine_id, corpusids in machine_corpusids.items():
                if not corpusids:
                    continue
                
                cursor = self.cursors[machine_id]
                
                # 查询所有字段（只返回非空字段）
                cursor.execute(f"""
                    SELECT corpusid, specter_v1, specter_v2, content, citations, references
                    FROM {TEMP_TABLE}
                    WHERE corpusid = ANY(%s) AND is_done = FALSE;
                """, (corpusids,))
                
                # Step 3: 合并数据（处理冲突）
                for corpusid, specter_v1, specter_v2, content, citations, references in cursor:
                    if corpusid not in updates_merged:
                        updates_merged[corpusid] = {}
                    
                    # 只记录非空字段（反序列化JSON字符串）
                    if specter_v1 and 'specter_v1' not in updates_merged[corpusid]:
                        try:
                            updates_merged[corpusid]['specter_v1'] = orjson.loads(specter_v1)
                        except:
                            pass  # 解析失败，跳过
                    
                    if specter_v2 and 'specter_v2' not in updates_merged[corpusid]:
                        try:
                            updates_merged[corpusid]['specter_v2'] = orjson.loads(specter_v2)
                        except:
                            pass
                    
                    # content冲突处理：主机器优先（如果主机器已有，不覆盖）
                    if content and 'content' not in updates_merged[corpusid]:
                        try:
                            updates_merged[corpusid]['content'] = orjson.loads(content)
                        except:
                            pass
                    elif content and machine_id == self.primary_machine:
                        # 如果当前是主机器且有content，覆盖之前的
                        try:
                            updates_merged[corpusid]['content'] = orjson.loads(content)
                        except:
                            pass
                    
                    # citations和references处理（主机器优先）
                    if citations and 'citations' not in updates_merged[corpusid]:
                        try:
                            updates_merged[corpusid]['citations'] = orjson.loads(citations)
                        except:
                            pass
                    elif citations and machine_id == self.primary_machine:
                        try:
                            updates_merged[corpusid]['citations'] = orjson.loads(citations)
                        except:
                            pass
                    
                    if references and 'references' not in updates_merged[corpusid]:
                        try:
                            updates_merged[corpusid]['references'] = orjson.loads(references)
                        except:
                            pass
                    elif references and machine_id == self.primary_machine:
                        try:
                            updates_merged[corpusid]['references'] = orjson.loads(references)
                        except:
                            pass
            
            return updates_merged
    
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
    
    def _parse_data(self, data):
        """解析临时表中的data字段值（确保返回对象而非字符串）
        
        Args:
            data: 临时表中的data字段值（可能是对象、字符串或bytes）
        
        Returns:
            解析后的Python对象（dict、list等）
        """
        # 如果已经是Python对象，直接返回
        if isinstance(data, (dict, list)):
            return data
        
        # 如果是字符串或bytes，尝试解析JSON
        if isinstance(data, (str, bytes)):
            try:
                return orjson.loads(data)
            except:
                # 解析失败，返回原始数据
                return data
        
        # 其他类型直接返回
        return data
    
    def _apply_update(self, record: dict, merged_data: dict) -> bool:
        """应用更新到记录（使用新embedding结构）
        
        Args:
            record: JSONL记录（dict）
            merged_data: {'specter_v1': [...], 'specter_v2': [...], 'content': {...}}
        
        Returns:
            bool: 是否成功更新（True=已更新，False=跳过）
        """
        updated = False
        
        # ========== 处理 embedding 字段（新结构）==========
        if 'specter_v1' in merged_data or 'specter_v2' in merged_data:
            # 初始化新embedding结构
            new_embedding = {}
            
            # 迁移旧数据（如果存在且为specter_v2）
            old_embedding = record.get('embedding', {})
            if isinstance(old_embedding, dict) and old_embedding:
                if old_embedding.get('model') == 'specter_v2' and old_embedding.get('vector'):
                    new_embedding["2"] = {
                        "model": "specter_v2",
                        "vector": old_embedding['vector']
                    }
            
            # 添加新数据（覆盖旧数据）
            if 'specter_v1' in merged_data:
                new_embedding["1"] = {
                    "model": "specter_v1",
                    "vector": merged_data['specter_v1']
                }
                updated = True
            
            if 'specter_v2' in merged_data:
                new_embedding["2"] = {
                    "model": "specter_v2",
                    "vector": merged_data['specter_v2']
                }
                updated = True
            
            # 更新record
            if new_embedding:
                record['embedding'] = new_embedding
        
        # ========== 处理 content 字段 ==========
        if 'content' in merged_data:
            # 只有当原字段为空时才添加
            if not record.get('content') or record.get('content') == {}:
                record['content'] = merged_data['content']
                updated = True
        
        # ========== 处理 citations 字段 ==========
        if 'citations' in merged_data:
            citations_data = merged_data['citations']
            # 确保citations_data是列表
            if isinstance(citations_data, list):
                record['citations'] = citations_data
                # 构造detailsOfCitations
                record['detailsOfCitations'] = {
                    "offset": 0,
                    "data": citations_data
                }
                updated = True
        
        # ========== 处理 references 字段 ==========
        if 'references' in merged_data:
            references_data = merged_data['references']
            # 确保references_data是列表
            if isinstance(references_data, list):
                record['references'] = references_data
                # 构造detailsOfReference
                record['detailsOfReference'] = {
                    "offset": 0,
                    "data": references_data
                }
                updated = True
        
        return updated

    
    def update_jsonl_file_from_local(self, filename, updates, skip_copy_in=False, skip_copy_out=False, copy_in_time=0):
        """在本地SSD上更新JSONL文件
        
        Args:
            filename: 文件名
            updates: {corpusid: {'specter_v1': ..., 'specter_v2': ..., 'content': ...}, ...}
            skip_copy_in: 是否跳过复制入
            skip_copy_out: 是否跳过复制出
            copy_in_time: 复制入耗时
        
        Returns:
            (成功数, 失败corpusid列表)
        """
        file_path = FINAL_DELIVERY_DIR / filename
        if not file_path.exists():
            return (0, list(updates.keys()))
        
        if not updates:
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
        
        # 步骤2: 在本地SSD上处理文件（使用mmap加速）
        t0 = time.time()
        
        # 使用mmap读取文件（对大文件更高效）
        with open(local_cache, 'rb') as fin:
            file_size = os.path.getsize(local_cache)
            if file_size == 0:
                content = b''
            else:
                with mmap.mmap(fin.fileno(), 0, access=mmap.ACCESS_READ) as mmapped:
                    content = mmapped.read()
        
        # 按行分割
        lines = content.split(b'\n')
        has_trailing_newline = content.endswith(b'\n')
        
        updated_corpusids = []
        output_lines = []
        
        # 批量处理所有行
        for line in lines:
            if not line:
                continue
            
            corpusid = self._extract_corpusid(line)
            
            if corpusid in updates:
                # 需要更新
                record = orjson.loads(line)
                data = updates[corpusid]
                
                # 应用更新
                if self._apply_update(record, data):
                    updated_corpusids.append(corpusid)
                    output_lines.append(orjson.dumps(record))
                else:
                    # 跳过更新，保留原样
                    output_lines.append(line)
            else:
                # 不需要更新
                output_lines.append(line)
        
        # 一次性写入
        with open(local_output, 'wb', buffering=16*1024*1024) as fout:
            fout.write(b'\n'.join(output_lines))
            if has_trailing_newline and output_lines:
                fout.write(b'\n')
        
        process_time = time.time() - t0
        
        # 步骤3: 复制回外置盘
        if not skip_copy_out:
            t0 = time.time()
            temp_file = file_path.with_suffix('.tmp')
            win32file.CopyFile(str(local_output), str(temp_file), 0)
            temp_file.replace(file_path)
            copy_out_time = time.time() - t0
            local_cache.unlink(missing_ok=True)
            local_output.unlink(missing_ok=True)
        
        # 记录日志
        if not skip_copy_out:
            log_performance(
                "单文件处理",
                file=filename,
                copy_in_sec=f"{copy_in_time:.2f}",
                process_sec=f"{process_time:.2f}",
                copy_out_sec=f"{copy_out_time:.2f}",
                updates=len(updated_corpusids)
            )
        
        failed = [cid for cid in updates.keys() if cid not in updated_corpusids]
        return (len(updated_corpusids), failed)
    
    
    def mark_as_done(self, corpusids_or_dict):
        """标记corpusid为已处理（支持多机器模式）
        
        Args:
            单机器模式: corpusids - 列表
            多机器模式: corpusids_with_sources - {corpusid: [machine_ids], ...}
        """
        if not corpusids_or_dict:
            return
        
        t0 = time.time()
        
        if self.mode == 'single':
            # 单机器模式
            cursor = self.cursors[self.primary_machine]
            conn = self.connections[self.primary_machine]
            
            cursor.execute(f"""
                UPDATE {TEMP_TABLE} 
                SET is_done = TRUE 
                WHERE corpusid = ANY(%s)
            """, (corpusids_or_dict,))
            
            conn.commit()
            elapsed = time.time() - t0
            log_performance("数据库更新", records=len(corpusids_or_dict), time_sec=f"{elapsed:.2f}")
        
        else:
            # 多机器模式：按来源分别标记
            machine_corpusids = defaultdict(list)
            
            # 分离corpusid按机器
            for corpusid, sources in corpusids_or_dict.items():
                for machine_id in sources:
                    machine_corpusids[machine_id].append(corpusid)
            
            # 分别更新各机器
            total_records = 0
            for machine_id, corpusids in machine_corpusids.items():
                if not corpusids:
                    continue
                
                cursor = self.cursors[machine_id]
                conn = self.connections[machine_id]
                
                cursor.execute(f"""
                    UPDATE {TEMP_TABLE} 
                    SET is_done = TRUE 
                    WHERE corpusid = ANY(%s)
                """, (corpusids,))
                
                conn.commit()
                total_records += len(corpusids)
            
            elapsed = time.time() - t0
            log_performance("分布式数据库更新", 
                           records=total_records, 
                           machines=len(machine_corpusids),
                           time_sec=f"{elapsed:.2f}")
    
    def run(self):
        """执行批量更新（配置参数见文件顶部配置区）"""
        print("=" * 80)
        print("JSONL批量更新器")
        print("=" * 80)
        mode = f"批量复制模式（每批{BATCH_SIZE}个文件）" if USE_BATCH_COPY else "逐个文件处理模式"
        print(f"  处理模式: {mode}")
        if self.mode == 'single':
            print(f"  机器模式: 单机器 ({self.primary_machine})")
        else:
            print(f"  机器模式: 多机器（{', '.join(self.machine_list)}）")
        
        overall_start = time.time()
        
        try:
            # ========== 准备阶段 ==========
            db_start = time.time()
            print("\n【准备阶段】")
            
            print("  [1/3] 连接数据库...")
            self.connect_db()
            
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
            if self.mode == 'multi':
                for machine_id, count in stats.get('machine_stats', {}).items():
                    print(f"    - {machine_id}: {count} 条")
                print(f"    - 重复记录: {stats.get('overlap', 0)} 条")
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
            
            # 流水线模式：追踪下一个文件的后台复制和查询任务
            next_copy_future = None
            next_query_future = None
            
            # 批量标记累积器
            pending_marks_single = []  # 单机器模式：[corpusid, ...]
            pending_marks_multi = {}   # 多机器模式：{corpusid: sources, ...}
            
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
                            corpusid_list_or_tuples = grouped_corpusids[filename]
                            updates = self.get_file_updates(corpusid_list_or_tuples)
                            
                            success_count, failed = self.update_jsonl_file_from_local(
                                filename, updates, skip_copy_in=True, skip_copy_out=True
                            )
                            
                            total_success += success_count
                            total_failed += len(failed)
                            
                            # 累积待标记的记录（批量标记优化）
                            if self.mode == 'single':
                                # 单机器模式：累积corpusid
                                success_ids = [cid for cid in updates.keys() if cid not in failed]
                                pending_marks_single.extend(success_ids)
                                
                                # 达到批次大小则批量标记
                                if len(pending_marks_single) >= BATCH_MARK_SIZE:
                                    self.mark_as_done(pending_marks_single)
                                    pending_marks_single.clear()
                            else:
                                # 多机器模式：累积 {corpusid: sources}
                                for corpusid, sources in corpusid_list_or_tuples:
                                    if corpusid not in failed:
                                        pending_marks_multi[corpusid] = sources
                                
                                # 达到批次大小则批量标记
                                if len(pending_marks_multi) >= BATCH_MARK_SIZE:
                                    self.mark_as_done(pending_marks_multi)
                                    pending_marks_multi.clear()
                            
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
                        
                        # === 阶段2.1：获取当前文件的更新数据（流水线查询）===
                        corpusid_list_or_tuples = grouped_corpusids[filename]
                        
                        if batch_idx == 0:
                            # 第一个文件：同步查询
                            updates = self.get_file_updates(corpusid_list_or_tuples)
                        else:
                            # 后续文件：等待后台查询完成
                            try:
                                updates = next_query_future.result()
                            except Exception as e:
                                print(f"\n  ✗ 查询失败 {filename}: {e}")
                                pbar.update(1)
                                continue
                        
                        # === 阶段2.2：启动下一个文件的后台查询 ===
                        if batch_idx < total_batches - 1:
                            next_filename = filenames_list[batch_idx + 1]
                            next_corpusid_list = grouped_corpusids[next_filename]
                            next_query_future = self.copy_executor.submit(
                                self.get_file_updates, next_corpusid_list
                            )
                        
                        # === 阶段3：等待下一个文件复制完成（如果有）===
                        if next_copy_future:
                            try:
                                next_copy_future.result()  # 阻塞等待
                            except Exception as e:
                                # 下一个文件复制失败，记录但不影响当前文件
                                print(f"\n  ⚠️  下一个文件后台复制失败: {e}")
                        
                        # === 阶段4：处理并写回当前文件 ===
                        # 处理文件（已在本地SSD）
                        process_start = time.time()
                        success_count, failed = self.update_jsonl_file_from_local(
                            filename, updates, skip_copy_in=True, skip_copy_out=False, 
                            copy_in_time=copy_in_time
                        )
                        process_time = time.time() - process_start
                        
                        total_success += success_count
                        total_failed += len(failed)
                        
                        # 累积待标记的记录（批量标记优化）
                        if self.mode == 'single':
                            # 单机器模式：累积corpusid
                            success_ids = [cid for cid in updates.keys() if cid not in failed]
                            pending_marks_single.extend(success_ids)
                            
                            # 达到批次大小则批量标记
                            if len(pending_marks_single) >= BATCH_MARK_SIZE:
                                self.mark_as_done(pending_marks_single)
                                pending_marks_single.clear()
                            
                            # 更新进度条描述
                            pbar.set_postfix_str(f"文件: {filename[:20]}... | 更新: {success_count}/{len(updates)}")
                        else:
                            # 多机器模式：累积 {corpusid: sources}
                            for corpusid, sources in corpusid_list_or_tuples:
                                if corpusid not in failed:
                                    pending_marks_multi[corpusid] = sources
                            
                            # 达到批次大小则批量标记
                            if len(pending_marks_multi) >= BATCH_MARK_SIZE:
                                self.mark_as_done(pending_marks_multi)
                                pending_marks_multi.clear()
                            
                            # 统计各机器的更新数
                            machine_counts = defaultdict(int)
                            for corpusid, sources in corpusid_list_or_tuples:
                                if corpusid not in failed:
                                    for machine_id in sources:
                                        machine_counts[machine_id] += 1
                            
                            machine_str = ', '.join([f"{mid}: {cnt}" for mid, cnt in machine_counts.items()])
                            pbar.set_postfix_str(
                                f"文件: {filename[:20]}... | "
                                f"更新: {success_count}/{len(corpusid_list_or_tuples)} | "
                                f"{machine_str}"
                            )
                        
                        # 记录失败的corpusid
                        if failed:
                            self.failed_corpusids.extend(failed)
                            with open(FAILED_LOG, 'a', encoding='utf-8') as f:
                                for cid in failed:
                                    f.write(f"{cid}\t{filename}\tFAILED\n")
                        
                        pbar.update(1)
            
            # 处理剩余的待标记记录（批量标记优化：处理最后不足一个批次的记录）
            if self.mode == 'single' and pending_marks_single:
                self.mark_as_done(pending_marks_single)
                pending_marks_single.clear()
            elif self.mode == 'multi' and pending_marks_multi:
                self.mark_as_done(pending_marks_multi)
                pending_marks_multi.clear()
            
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
    
    parser = argparse.ArgumentParser(
        description="JSONL批量更新器（支持单机器和多机器模式，使用新embedding结构）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 单机器模式（自动处理所有数据集字段：specter_v1, specter_v2, content）
  python batch_update/jsonl_batch_updater.py --machine machine0
  
  # 多机器模式（跨机器协同更新，通过局域网合并多台机器的数据）
  python batch_update/jsonl_batch_updater.py --machines machine0,machine2

注意：
  - 单机器和多机器模式都使用新的embedding结构：{"1": {...}, "2": {...}}
  - 不再需要 --dataset 参数，会根据temp_import表的字段自动处理
        """
    )
    
    # 单机器模式参数
    parser.add_argument("--machine", choices=list(MACHINE_DB_MAP.keys()), 
                        help="单机器模式：目标机器")
    
    # 多机器模式参数
    parser.add_argument("--machines", 
                        help="多机器模式：机器列表（逗号分隔，如 machine0,machine2）")
    
    args = parser.parse_args()
    
    # 判断运行模式
    if args.machines:
        # 多机器模式
        machine_list = [m.strip() for m in args.machines.split(',')]
        
        # 验证机器ID
        invalid_machines = [m for m in machine_list if m not in MACHINE_DB_MAP]
        if invalid_machines:
            print(f"错误：无效的机器ID: {', '.join(invalid_machines)}")
            print(f"可选值: {', '.join(MACHINE_DB_MAP.keys())}")
            sys.exit(1)
        
        if len(machine_list) < 2:
            print("错误：多机器模式需要至少2台机器")
            sys.exit(1)
        
        print(f"启动多机器模式: {', '.join(machine_list)}")
        updater = JSONLBatchUpdater(machine_list=machine_list)
        updater.run()
    
    elif args.machine:
        # 单机器模式
        print(f"启动单机器模式: {args.machine}")
        updater = JSONLBatchUpdater(machine_id=args.machine)
        updater.run()
    
    else:
        print("错误：请选择运行模式")
        print("  单机器模式：需要 --machine")
        print("  多机器模式：需要 --machines")
        parser.print_help()
        sys.exit(1)

