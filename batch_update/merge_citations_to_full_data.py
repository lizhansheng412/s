"""
将 xxx_part2.jsonl 和数据库中的数据合并更新到 xxx.jsonl
支持: 双源合并 + 断点续传 + 跨机器
"""

import os
import sys
import tempfile
import shutil
import time
import sqlite3
import psycopg2
from psycopg2 import OperationalError, InterfaceError
import argparse
import re
from pathlib import Path
from typing import Dict, Any, Set, Optional
from datetime import datetime
from tqdm import tqdm
import orjson

# orjson.dumps 返回 bytes，需要解码
def json_dumps(obj):
    return orjson.dumps(obj).decode('utf-8')

json_loads = orjson.loads

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))
from db_config import get_db_config


# 需要更新的字段
CITATION_FIELDS = ["citations", "references", "detailsOfCitations", "detailsOfReference"]
# DB_FIELDS = ["specter_v1", "specter_v2", "content"]
DB_FIELDS = ["content"]
ALL_UPDATE_FIELDS = CITATION_FIELDS + DB_FIELDS

IGNORE_IS_DONE_FILTER = False
IS_DONE_FILTER_VALUE = False  # PostgreSQL boolean 类型

# 重试配置
MAX_RETRIES = 5              # 最大重试次数
RETRY_DELAY = 2              # 重试间隔(秒)
CONNECTION_TIMEOUT = 30      # 连接超时(秒)


def log(log_file: Path, msg: str):
    """日志只写入文件"""
    with open(log_file, 'a', encoding='utf-8') as f:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"[{timestamp}] {msg}\n")


def clean_json_line(line: str) -> str:
    """清理 JSON 字符串中的非法控制字符"""
    # 使用字典映射提高效率
    control_chars = {'\t': '\\t', '\n': '\\n', '\r': '\\r'}
    
    def replace_char(match):
        char = match.group(0)
        return control_chars.get(char, '')  # 其他控制字符移除
    
    return re.sub(r'[\x00-\x1f]', replace_char, line)


def init_progress_db(progress_db: Path):
    """初始化进度数据库"""
    progress_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(progress_db)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS progress (
            filename TEXT PRIMARY KEY,
            is_done BOOLEAN NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()


def get_completed_files(progress_db: Path) -> Set[str]:
    """获取已完成的文件列表"""
    conn = sqlite3.connect(progress_db)
    cursor = conn.cursor()
    cursor.execute('SELECT filename FROM progress WHERE is_done = 1')
    completed = {row[0] for row in cursor.fetchall()}
    conn.close()
    return completed


def mark_file_done(progress_db: Path, filename: str):
    """标记文件为已完成"""
    conn = sqlite3.connect(progress_db)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO progress (filename, is_done, updated_at)
        VALUES (?, 1, ?)
    ''', (filename, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def connect_pg_db(db_config: Dict[str, str], log_file: Optional[Path] = None):
    """连接PostgreSQL数据库(带重试)"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password'],
                connect_timeout=CONNECTION_TIMEOUT
            )
            return conn
        except (OperationalError, InterfaceError) as e:
            if attempt < MAX_RETRIES:
                if log_file:
                    log(log_file, f"数据库连接失败 (尝试 {attempt}/{MAX_RETRIES}): {e}, {RETRY_DELAY}秒后重试...")
                time.sleep(RETRY_DELAY)
            else:
                raise e


def is_citation_fields_empty(record: Dict[str, Any]) -> bool:
    """检查引用字段是否全为空"""
    for field in CITATION_FIELDS:
        value = record.get(field)
        if isinstance(value, list) and len(value) > 0:
            return False
        elif isinstance(value, dict):
            data = value.get("data", [])
            if isinstance(data, list) and len(data) > 0:
                return False
    return True


def is_db_fields_empty(record: Dict[str, Any]) -> bool:
    """检查数据库字段是否全为空"""
    for field in DB_FIELDS:
        value = record.get(field)
        if value and (isinstance(value, str) and value.strip() or not isinstance(value, str)):
            return False
    return True


def update_record_fields(target_record: Dict[str, Any], source_record: Dict[str, Any], 
                        fields: list, skip_if_target_not_empty: bool = False) -> int:
    """
    更新记录字段
    
    Args:
        target_record: 目标记录
        source_record: 源记录
        fields: 要更新的字段列表
        skip_if_target_not_empty: 如果目标字段不为空则跳过更新
    
    Returns:
        更新的字段数
    """
    updated = 0
    for field in fields:
        source_value = source_record.get(field)
        
        # 检查源值是否为空
        is_empty = False
        if source_value is None:
            is_empty = True
        elif isinstance(source_value, list):
            is_empty = len(source_value) == 0
        elif isinstance(source_value, dict):
            data = source_value.get("data", [])
            is_empty = not (isinstance(data, list) and len(data) > 0)
        elif isinstance(source_value, str):
            is_empty = not source_value.strip()
        
        if is_empty:
            continue
        
        # 如果需要检查目标字段是否为空
        if skip_if_target_not_empty:
            target_value = target_record.get(field)
            target_not_empty = False
            
            if target_value is not None:
                if isinstance(target_value, list):
                    target_not_empty = len(target_value) > 0
                elif isinstance(target_value, dict):
                    data = target_value.get("data", [])
                    target_not_empty = isinstance(data, list) and len(data) > 0
                elif isinstance(target_value, str):
                    target_not_empty = bool(target_value.strip())
            
            if target_not_empty:
                continue
        
        # 更新字段
        target_record[field] = source_value
        updated += 1
    
    return updated


def load_db_data(db_conn, corpusid_list: list, db_config: Dict[str, str], log_file: Path) -> Dict[int, Dict[str, Any]]:
    """
    从数据库批量加载数据(带重试和连接恢复)
    
    Returns:
        {corpusid: {content}}
    """
    if not corpusid_list:
        return {}
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # 检查连接是否有效
            try:
                db_conn.isolation_level
            except (OperationalError, InterfaceError):
                log(log_file, f"检测到数据库连接断开,正在重新连接...")
                db_conn = connect_pg_db(db_config, log_file)
            
            cursor = db_conn.cursor()
            placeholders = ','.join(['%s'] * len(corpusid_list))
            
            if IGNORE_IS_DONE_FILTER:
                query = f"""
                    SELECT corpusid, content
                    FROM temp_import
                    WHERE corpusid IN ({placeholders})
                """
                params = corpusid_list
            else:
                query = f"""
                    SELECT corpusid, content
                    FROM temp_import
                    WHERE is_done = %s AND corpusid IN ({placeholders})
                """
                params = [IS_DONE_FILTER_VALUE, *corpusid_list]
            
            cursor.execute(query, params)
            
            db_data = {}
            for row in cursor.fetchall():
                corpusid, content = row
                
                # 跳过 content 为空的记录
                if not content:
                    continue
                
                db_data[corpusid] = {
                    "content": content
                }
            
            cursor.close()
            return db_data
            
        except (OperationalError, InterfaceError) as e:
            # 回滚当前事务
            try:
                db_conn.rollback()
            except:
                pass
            
            if attempt < MAX_RETRIES:
                log(log_file, f"数据库查询失败 (尝试 {attempt}/{MAX_RETRIES}): {e}, {RETRY_DELAY}秒后重试...")
                time.sleep(RETRY_DELAY)
                try:
                    db_conn.close()
                except:
                    pass
                db_conn = connect_pg_db(db_config, log_file)
            else:
                log(log_file, f"数据库查询最终失败: {e}")
                raise e
        except Exception as e:
            # 其他错误也需要回滚
            try:
                db_conn.rollback()
            except:
                pass
            log(log_file, f"数据库查询错误: {e}")
            raise e


def process_file_pair(source_file: Path, target_file: Path, 
                     db_conn, db_config: Dict[str, str], log_file: Path) -> Dict[str, Any]:
    """处理文件对"""
    file_start = time.time()
    
    stats = {
        "source_lines": 0,
        "target_lines": 0,
        "skipped_empty": 0,
        "updated_citation": 0,
        "updated_db": 0,
        "updated_total": 0
    }
    
    timings = {
        "init_temp": 0,          # 初始化临时文件
        "load_part2": 0,         # 加载part2文件
        "load_db": 0,            # 加载数据库数据
        "process_target": 0,      # 处理目标文件
        "write_temp": 0,          # 写入临时文件
        "move_file": 0,           # 替换文件
        "total": 0                # 总耗时
    }
    
    # 初始化变量(用于日志记录)
    part2_data = {}
    db_data = {}
    
    # 阶段1: 创建临时文件 (使用字符串路径)
    t1 = time.time()
    temp_fd, temp_path_str = tempfile.mkstemp(
        suffix='.jsonl',
        dir=str(target_file.parent),
        prefix='.tmp_'
    )
    os.close(temp_fd)
    temp_path = Path(temp_path_str)
    timings["init_temp"] = time.time() - t1
    
    try:
        BUFFER_SIZE = 4 * 1024 * 1024  # 4MB缓冲区 (从1MB增加)
        
        # 使用字符串路径避免 Windows 路径问题
        with open(str(source_file), 'r', encoding='utf-8', buffering=BUFFER_SIZE) as f_source, \
             open(str(target_file), 'r', encoding='utf-8', buffering=BUFFER_SIZE) as f_target, \
             open(str(temp_path), 'w', encoding='utf-8', buffering=BUFFER_SIZE) as f_temp:
            
                # 阶段2: 预加载part2文件数据到内存
                t2 = time.time()
                part2_data = {}
                for line in f_source:
                    line = line.strip()
                    if not line:
                        continue
                    stats["source_lines"] += 1
                    
                    # 清理控制字符
                    try:
                        record = json_loads(line)
                    except (ValueError, Exception) as e:
                        # 尝试清理后再解析
                        try:
                            cleaned_line = clean_json_line(line)
                            record = json_loads(cleaned_line)
                            log(log_file, f"警告: 清理了非法字符 (part2文件第{stats['source_lines']}行)")
                        except (ValueError, Exception):
                            log(log_file, f"跳过: part2文件第{stats['source_lines']}行解析失败 - {str(e)}")
                            continue
                    
                    if not is_citation_fields_empty(record):
                        corpusid = record.get("corpusid")
                        if corpusid is not None:
                            part2_data[corpusid] = record
                    else:
                        stats["skipped_empty"] += 1
                timings["load_part2"] = time.time() - t2
                
                # 阶段3: 从数据库批量加载数据(如果提供了连接)
                t3 = time.time()
                db_data = {}
                if db_conn:
                    corpusid_list = list(part2_data.keys())
                    # 分批查询(每次5000个)
                    BATCH_SIZE = 5000
                    for i in range(0, len(corpusid_list), BATCH_SIZE):
                        batch = corpusid_list[i:i+BATCH_SIZE]
                        batch_data = load_db_data(db_conn, batch, db_config, log_file)
                        db_data.update(batch_data)
                timings["load_db"] = time.time() - t3
                
                # 阶段4: 流式处理目标文件并写入临时文件
                t4 = time.time()
                write_buffer = []
                WRITE_BATCH = 10000  # 从5000增加到10000
                write_time = 0
                
                for target_line in f_target:
                    target_line = target_line.strip()
                    if not target_line:
                        continue
                    
                    stats["target_lines"] += 1
                    
                    # 清理控制字符
                    try:
                        target_record = json_loads(target_line)
                    except (ValueError, Exception) as e:
                        # 尝试清理后再解析
                        try:
                            cleaned_line = clean_json_line(target_line)
                            target_record = json_loads(cleaned_line)
                            log(log_file, f"警告: 清理了非法字符 (目标文件第{stats['target_lines']}行)")
                        except (ValueError, Exception):
                            log(log_file, f"跳过: 目标文件第{stats['target_lines']}行解析失败 - {str(e)}")
                            # 目标文件解析失败，保留原始行
                            write_buffer.append(target_line + '\n')
                            if len(write_buffer) >= WRITE_BATCH:
                                tw = time.time()
                                f_temp.writelines(write_buffer)
                                write_time += time.time() - tw
                                write_buffer = []
                            continue
                    
                    target_corpusid = target_record.get("corpusid")
                    
                    record_updated = False
                    
                    # 更新引用字段(从part2文件)
                    if target_corpusid in part2_data:
                        cnt = update_record_fields(
                            target_record, 
                            part2_data[target_corpusid], 
                            CITATION_FIELDS,
                            skip_if_target_not_empty=False
                        )
                        if cnt > 0:
                            stats["updated_citation"] += 1
                            record_updated = True
                    
                    # 更新数据库字段(如果目标字段为空)
                    if target_corpusid in db_data:
                        cnt = update_record_fields(
                            target_record, 
                            db_data[target_corpusid], 
                            DB_FIELDS,
                            skip_if_target_not_empty=True  # 目标不为空则跳过
                        )
                        if cnt > 0:
                            stats["updated_db"] += 1
                            record_updated = True
                    
                    if record_updated:
                        stats["updated_total"] += 1
                    
                    # 批量写入
                    write_buffer.append(json_dumps(target_record) + '\n')
                    if len(write_buffer) >= WRITE_BATCH:
                        tw = time.time()
                        f_temp.writelines(write_buffer)
                        write_time += time.time() - tw
                        write_buffer = []
                
                # 写入剩余数据
                if write_buffer:
                    tw = time.time()
                    f_temp.writelines(write_buffer)
                    write_time += time.time() - tw
                
                timings["process_target"] = time.time() - t4 - write_time
                timings["write_temp"] = write_time
        
        # 阶段5: 替换文件 (优化：直接替换避免删除)
        t5 = time.time()
        target_file_str = str(target_file)
        temp_path_str = str(temp_path)
        
        # Windows上直接替换（os.replace比删除再重命名更快）
        os.replace(temp_path_str, target_file_str)
        timings["move_file"] = time.time() - t5
        
    except Exception as e:
        try:
            if os.path.exists(str(temp_path)):
                os.remove(str(temp_path))
        except:
            pass
        raise e
    
    timings["total"] = time.time() - file_start
    stats["time"] = timings["total"]
    stats["timings"] = timings
    
    # 记录详细日志
    log(log_file, f"完成: {source_file.name}")
    log(log_file, f"  阶段耗时:")
    log(log_file, f"    - 初始化临时文件: {timings['init_temp']:.3f}s")
    log(log_file, f"    - 加载part2文件: {timings['load_part2']:.3f}s ({stats['source_lines']}行, {len(part2_data)}条有效)")
    log(log_file, f"    - 加载数据库数据: {timings['load_db']:.3f}s ({len(db_data)}条)")
    log(log_file, f"    - 处理目标文件: {timings['process_target']:.3f}s ({stats['target_lines']}行)")
    log(log_file, f"    - 写入临时文件: {timings['write_temp']:.3f}s")
    log(log_file, f"    - 替换文件: {timings['move_file']:.3f}s")
    log(log_file, f"  总耗时: {timings['total']:.3f}s")
    log(log_file, f"  更新统计: {stats['updated_total']}/{stats['target_lines']} (引用:{stats['updated_citation']}, DB:{stats['updated_db']})")
    
    return stats


def main():
    parser = argparse.ArgumentParser(description='合并part2和数据库数据到目标文件')
    parser.add_argument('--source-dir', required=True, help='源文件目录(xxx_part2.jsonl)')
    parser.add_argument('--target-dir', required=True, help='目标文件目录(xxx.jsonl)')
    parser.add_argument('--machine', required=True, 
                       choices=['machine0', 'machine2'],
                       help='机器ID (machine0自动连接远程machine2数据库)')
    
    args = parser.parse_args()
    
    # 配置路径
    SOURCE_DIR = args.source_dir
    TARGET_DIR = args.target_dir
    PROGRESS_DB = Path(__file__).parent.parent / "logs" / "merge_progress.db"
    LOG_FILE = Path(__file__).parent.parent / "logs" / f"merge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    start_time = time.time()
    
    # 初始化
    init_progress_db(PROGRESS_DB)
    
    # 加载数据库配置
    if args.machine == 'machine0':
        # machine0 自动连接远程 machine2 数据库
        db_config = get_db_config('machine2')
        print(f"📡 machine0: 自动连接远程 machine2 数据库")
    else:
        # machine2 使用本地数据库配置
        db_config = get_db_config('machine2')
        print(f"💾 machine2: 使用本地数据库配置")
    
    log(LOG_FILE, "=" * 80)
    log(LOG_FILE, "引用数据合并工具 - 双源合并+断点续传")
    log(LOG_FILE, "=" * 80)
    log(LOG_FILE, f"机器ID: {args.machine}")
    if args.machine == 'machine0':
        log(LOG_FILE, f"数据库模式: 远程连接 machine2")
    else:
        log(LOG_FILE, f"数据库模式: 本地")
    log(LOG_FILE, f"源目录: {SOURCE_DIR}")
    log(LOG_FILE, f"目标目录: {TARGET_DIR}")
    log(LOG_FILE, f"数据库: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    log(LOG_FILE, f"进度数据库: {PROGRESS_DB}")
    log(LOG_FILE, f"日志文件: {LOG_FILE}")
    log(LOG_FILE, f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(LOG_FILE, "=" * 80)
    
    # 检查目录
    source_path = Path(SOURCE_DIR)
    target_path = Path(TARGET_DIR)
    
    if not source_path.exists():
        print(f"❌ 错误: 源目录不存在: {SOURCE_DIR}")
        return
    
    if not target_path.exists():
        print(f"❌ 错误: 目标目录不存在: {TARGET_DIR}")
        return
    
    # 连接数据库
    db_conn = None
    try:
        db_conn = connect_pg_db(db_config, LOG_FILE)
        print(f"✓ 数据库连接成功: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        log(LOG_FILE, f"数据库连接成功")
    except Exception as e:
        print(f"❌ 数据库连接失败(已重试{MAX_RETRIES}次): {e}")
        log(LOG_FILE, f"数据库连接失败: {e}")
        return
    
    try:
        # 获取所有源文件
        source_files = sorted(source_path.glob("*_part2.jsonl"))
        
        if not source_files:
            print("❌ 错误: 源目录中没有找到 *_part2.jsonl 文件")
            return
        
        # 对于 machine2，只处理目标目录中存在对应文件的源文件
        if args.machine == 'machine2':
            # 获取目标目录所有文件名（不含后缀）
            target_files_set = set()
            for target_file in target_path.glob("*.jsonl"):
                if not target_file.name.endswith("_part2.jsonl"):
                    target_files_set.add(target_file.stem)
            
            log(LOG_FILE, f"目标目录文件数: {len(target_files_set)}")
            
            # 过滤源文件：只保留目标目录中存在对应文件的
            filtered_source_files = []
            for source_file in source_files:
                source_name = source_file.stem  # 例如: "4f694c82_part2"
                if source_name.endswith("_part2"):
                    base_name = source_name[:-6]  # 去掉 "_part2"
                    if base_name in target_files_set:
                        filtered_source_files.append(source_file)
            
            source_files = filtered_source_files
            log(LOG_FILE, f"匹配的源文件数: {len(source_files)}")
            
            if not source_files:
                print("❌ 错误: 没有找到与目标目录匹配的源文件")
                return
        
        # 获取已完成的文件
        completed_files = get_completed_files(PROGRESS_DB)
        
        # 过滤出未完成的文件
        pending_files = [f for f in source_files if f.name not in completed_files]
        
        log(LOG_FILE, f"总文件数: {len(source_files)}")
        log(LOG_FILE, f"已完成: {len(completed_files)}")
        log(LOG_FILE, f"待处理: {len(pending_files)}")
        
        if len(completed_files) > 0:
            print(f"\n📊 断点续传: 发现 {len(completed_files)} 个已完成文件,继续处理剩余 {len(pending_files)} 个文件\n")
        else:
            print(f"\n📊 开始处理 {len(pending_files)} 个文件\n")
        
        if len(pending_files) == 0:
            print("✅ 所有文件已处理完成!")
            return
        
        # 全局统计
        global_stats = {
            "total_files": len(source_files),
            "completed_before": len(completed_files),
            "processed_now": 0,
            "skipped": 0,
            "total_updated": 0,
            "total_citation": 0,
            "total_db": 0
        }
        
        # 进度条
        pbar = tqdm(
            pending_files,
            total=len(pending_files),
            desc="处理进度",
            unit="file",
            bar_format='{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}, {postfix}]',
            ncols=120
        )
        
        # 处理文件
        for source_file in pbar:
            source_name = source_file.stem
            pbar.set_postfix_str(source_file.name, refresh=False)
            
            if not source_name.endswith("_part2"):
                global_stats["skipped"] += 1
                continue
            
            base_name = source_name[:-6]
            target_file = target_path / f"{base_name}.jsonl"
            
            # Machine2 已预先过滤，无需检查；Machine0 需要检查
            if args.machine != 'machine2':
                # 使用 os.path.isfile 更可靠
                if not os.path.isfile(str(target_file)):
                    log(LOG_FILE, f"跳过: 目标文件不存在 - {target_file.name}")
                    global_stats["skipped"] += 1
                    continue
            
            try:
                stats = process_file_pair(source_file, target_file, db_conn, db_config, LOG_FILE)
                
                # 标记为已完成
                mark_file_done(PROGRESS_DB, source_file.name)
                
                global_stats["processed_now"] += 1
                global_stats["total_updated"] += stats["updated_total"]
                global_stats["total_citation"] += stats["updated_citation"]
                global_stats["total_db"] += stats["updated_db"]
                
            except Exception as e:
                log(LOG_FILE, f"错误: 处理 {source_file.name} 失败 - {str(e)}")
                print(f"⚠️  处理 {source_file.name} 失败: {e}")
                global_stats["skipped"] += 1
                continue
        
        pbar.close()
        
        # 最终统计
        total_time = time.time() - start_time
        avg_time = total_time / global_stats["processed_now"] if global_stats["processed_now"] > 0 else 0
        
        print("\n" + "=" * 80)
        print("处理完成!")
        print("=" * 80)
        print(f"总文件数: {global_stats['total_files']}")
        print(f"之前已完成: {global_stats['completed_before']}")
        print(f"本次处理: {global_stats['processed_now']}")
        print(f"跳过: {global_stats['skipped']}")
        print(f"总更新记录: {global_stats['total_updated']}")
        print(f"  - 引用字段更新: {global_stats['total_citation']}")
        print(f"  - 数据库字段更新: {global_stats['total_db']}")
        print(f"平均速度: {avg_time:.2f}s/file")
        print(f"总耗时: {total_time:.2f}s ({total_time/60:.1f}分钟)")
        print(f"\n日志文件: {LOG_FILE}")
        print(f"进度数据库: {PROGRESS_DB}")
        print("=" * 80)
        
        log(LOG_FILE, "\n" + "=" * 80)
        log(LOG_FILE, "处理完成 - 全局统计")
        log(LOG_FILE, "=" * 80)
        log(LOG_FILE, f"总文件数: {global_stats['total_files']}")
        log(LOG_FILE, f"之前已完成: {global_stats['completed_before']}")
        log(LOG_FILE, f"本次处理: {global_stats['processed_now']}")
        log(LOG_FILE, f"跳过: {global_stats['skipped']}")
        log(LOG_FILE, f"总更新记录: {global_stats['total_updated']}")
        log(LOG_FILE, f"  - 引用字段更新: {global_stats['total_citation']}")
        log(LOG_FILE, f"  - 数据库字段更新: {global_stats['total_db']}")
        log(LOG_FILE, f"平均速度: {avg_time:.2f}s/file")
        log(LOG_FILE, f"总耗时: {total_time:.2f}s ({total_time/60:.1f}分钟)")
        log(LOG_FILE, f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log(LOG_FILE, "=" * 80)
        
    finally:
        if db_conn:
            db_conn.close()
            print("\n✓ 数据库连接已关闭")


if __name__ == "__main__":
    main()
