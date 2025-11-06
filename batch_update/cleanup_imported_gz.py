"""
清理已成功导入的gz文件以释放磁盘空间
不删除数据库记录，只删除实际文件
支持定时监控磁盘空间并自动清理
"""
import sys
from pathlib import Path

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import psycopg2
import shutil
import time
from datetime import datetime
from db_config import get_db_config
from init_temp_table import GZ_LOG_TABLE

FAILED_LOG_BASE = Path(__file__).parent.parent / "logs" / "batch_update"
DISK_THRESHOLD_GB = 30  # 磁盘空间阈值
CHECK_INTERVAL_SEC = 900  # 检查间隔（15分钟）


def get_failed_log_path(data_type):
    """获取指定数据集的失败日志路径"""
    return FAILED_LOG_BASE / data_type / "gz_import_failed.txt"


def get_disk_free_space_gb(drive='D:\\'):
    """获取磁盘剩余空间（GB）"""
    try:
        stat = shutil.disk_usage(drive)
        return stat.free / (1024**3)
    except Exception as e:
        print(f"ERROR: Failed to get disk space - {e}")
        return None


def load_failed_files(data_type):
    """从失败日志中读取指定dataset的失败文件列表"""
    failed_log = get_failed_log_path(data_type)
    if not failed_log.exists():
        return set()
    
    failed_files = set()
    try:
        with open(failed_log, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # 解析: [timestamp] filename | error=xxx
                if line.startswith('['):
                    try:
                        parts = line.split('|')
                        if len(parts) >= 1:
                            filename = parts[0].split(']', 1)[1].strip()
                            failed_files.add(filename)
                    except:
                        continue
    except Exception as e:
        print(f"WARNING: Failed to read failed log - {e}")
        return set()
    
    return failed_files


def cleanup_imported_files(gz_directory, data_type, machine_id='machine0'):
    """删除已成功导入和失败的gz文件（分别统计）"""
    gz_dir = Path(gz_directory)
    
    if not gz_dir.exists():
        return 0
    
    conn = None
    cursor = None
    
    try:
        # 获取已导入的文件
        db_config = get_db_config(machine_id)
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute(
            f"SELECT filename FROM {GZ_LOG_TABLE} WHERE data_type = %s",
            (data_type,)
        )
        imported_files = set(row[0] for row in cursor.fetchall())
        
        # 获取失败的文件
        failed_files = load_failed_files(data_type)
        
        # 分别删除已导入和失败的文件
        deleted_imported = 0
        deleted_failed = 0
        error_count = 0
        total_size_freed = 0
        
        # 删除已导入的文件
        for filename in imported_files:
            gz_file = gz_dir / filename
            if not gz_file.exists():
                continue
            try:
                total_size_freed += gz_file.stat().st_size
                gz_file.unlink()
                deleted_imported += 1
            except:
                error_count += 1
        
        # 删除失败的文件
        for filename in failed_files:
            if filename in imported_files:  # 避免重复删除
                continue
            gz_file = gz_dir / filename
            if not gz_file.exists():
                continue
            try:
                total_size_freed += gz_file.stat().st_size
                gz_file.unlink()
                deleted_failed += 1
            except:
                error_count += 1
        
        # 显示结果
        freed_gb = total_size_freed / (1024**3)
        if deleted_imported > 0 or deleted_failed > 0:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Cleanup: "
                  f"imported={deleted_imported}, failed={deleted_failed}, "
                  f"error={error_count}, freed={freed_gb:.2f}GB")
        
        return freed_gb
        
    except Exception as e:
        print(f"ERROR: Cleanup failed - {e}")
        return 0
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def monitor_and_cleanup(gz_directory, data_type, machine_id='machine0'):
    """定时监控磁盘空间，空间不足时自动清理"""
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Monitor started: "
          f"dataset={data_type}, threshold={DISK_THRESHOLD_GB}GB, interval={CHECK_INTERVAL_SEC}s")
    
    while True:
        try:
            free_space = get_disk_free_space_gb('D:\\')
            
            if free_space is not None and free_space < DISK_THRESHOLD_GB:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                      f"D: {free_space:.2f}GB - LOW! Cleanup triggered...")
                cleanup_imported_files(gz_directory, data_type, machine_id)
            
            time.sleep(CHECK_INTERVAL_SEC)
            
        except KeyboardInterrupt:
            print("\nMonitor stopped")
            break
        except Exception as e:
            print(f"ERROR: {e}")
            time.sleep(CHECK_INTERVAL_SEC)

