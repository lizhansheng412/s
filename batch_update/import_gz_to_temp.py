"""
从gz文件快速导入数据到临时表
使用COPY命令和优化的导入策略
"""
import sys
from pathlib import Path

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import gzip
import orjson  # 比json快2-3倍
import psycopg2
from datetime import datetime
import time
import threading
from db_config import get_db_config, MACHINE_DB_MAP
from init_temp_table import GZ_LOG_TABLE, DATASET_TYPES
from cleanup_imported_gz import DISK_THRESHOLD_GB

TEMP_TABLE = "temp_import"
CHUNK_SIZE = 60000  # 单文件一次COPY（54000行+余量，确保任何文件都一次完成）
RUNNING_LOG = Path(__file__).parent.parent / "logs" / "running.log"
FAILED_LOG_BASE = Path(__file__).parent.parent / "logs" / "batch_update"


def get_copy_sql(data_type):
    """根据数据集类型生成对应的COPY SQL命令"""
    if data_type == 'embeddings_specter_v1':
        field = 'specter_v1'
    elif data_type == 'embeddings_specter_v2':
        field = 'specter_v2'
    elif data_type in ('s2orc', 's2orc_v2'):
        field = 'content'
    elif data_type == 'citations':
        # citations 使用两个字段：citations 和 references（references是保留字，需要双引号）
        return (
            f'COPY {TEMP_TABLE} (corpusid, citations, "references", is_done) '
            "FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '')"
        )
    else:
        raise ValueError(f"不支持的数据集类型: {data_type}")
    
    return (
        f"COPY {TEMP_TABLE} (corpusid, {field}, is_done) "
        "FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '')"
    )


def get_failed_log_path(data_type):
    """获取指定数据集的失败日志路径"""
    return FAILED_LOG_BASE / data_type / "gz_import_failed.txt"


def log_performance(stage, **metrics):
    """记录性能日志到running.log"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    metrics_str = " | ".join([f"{k}={v}" for k, v in metrics.items()])
    log_line = f"[{timestamp}] {stage} | {metrics_str}\n"
    with open(RUNNING_LOG, 'a', encoding='utf-8') as f:
        f.write(log_line)


def log_failed_file(filename, data_type, error):
    """记录失败的gz文件"""
    failed_log = get_failed_log_path(data_type)
    failed_log.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_line = f"[{timestamp}] {filename} | error={error}\n"
    with open(failed_log, 'a', encoding='utf-8') as f:
        f.write(log_line)


class CopyStream:
    """将行迭代器包装为psycopg2 copy_expert可消费的流"""

    def __init__(self, iterator, chunk_size=65536):
        self.iterator = iterator
        self.chunk_size = chunk_size
        self._buffer = bytearray()
        self._exhausted = False

    def readable(self):  # 与io接口兼容
        return True

    def read(self, size=-1):
        if size == -1:
            chunks = [bytes(self._buffer)]
            self._buffer.clear()
            for chunk in self.iterator:
                chunks.append(chunk)
            self._exhausted = True
            return b''.join(chunks)

        while len(self._buffer) < max(size, self.chunk_size) and not self._exhausted:
            try:
                self._buffer.extend(next(self.iterator))
            except StopIteration:
                self._exhausted = True
                break

        if not self._buffer:
            return b''

        if size >= len(self._buffer):
            data = bytes(self._buffer)
            self._buffer.clear()
            return data

        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data


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


def delete_gz_file(gz_path):
    """删除gz文件以释放存储空间"""
    try:
        gz_path.unlink()
        print(f"  🗑️  已删除文件以释放空间")
        return True
    except Exception as e:
        print(f"  ⚠️  删除文件失败（忽略）: {e}")
        return False


def start_cleanup_monitor(gz_directory, data_type, machine_id):
    """在后台线程启动清理监控
    
    注意：使用 daemon=True 确保主进程结束时监控线程也会自动终止
    """
    try:
        # 导入监控函数
        from cleanup_imported_gz import monitor_and_cleanup
        
        # 启动守护线程：主进程结束时会自动终止
        monitor_thread = threading.Thread(
            target=monitor_and_cleanup,
            args=(gz_directory, data_type, machine_id),
            daemon=True,  # 守护线程：主进程退出时自动终止
            name="DiskSpaceMonitor"
        )
        monitor_thread.start()
        
        print(f"已启动磁盘空间监控 (阈值: {DISK_THRESHOLD_GB}GB, 间隔: 15分钟)")
    except Exception as e:
        print(f"WARNING: Failed to start cleanup monitor - {e}")


def is_file_imported(cursor, filename, data_type):
    """检查文件是否已导入"""
    cursor.execute(
        f"SELECT 1 FROM {GZ_LOG_TABLE} WHERE filename = %s AND data_type = %s LIMIT 1",
        (filename, data_type)
    )
    return cursor.fetchone() is not None


def log_imported_file(cursor, filename, data_type):
    """记录已成功导入的文件"""
    cursor.execute(
        f"INSERT INTO {GZ_LOG_TABLE} (filename, data_type) VALUES (%s, %s) ON CONFLICT (filename, data_type) DO NOTHING",
        (filename, data_type)
    )


def import_gz_to_temp_fast(gz_file_path, data_type=None, machine_id='machine0'):
    """
    从单个gz文件快速导入数据到临时表
    使用COPY命令，分块写入，减少内存峰值
    
    优化点：
    - 使用orjson（比json快2-3倍）
    - 分块COPY（每3万行，5万行文件分2次）
    - 关闭同步提交（提速20-30%）
    - gzip内部缓冲优化
    
    Args:
        gz_file_path: gz文件路径
        data_type: 数据集类型（必需，用于记录和跳过已处理文件）
        machine_id: 目标机器ID
    """
    if data_type is None:
        raise ValueError(f"必须指定--dataset参数，可选值: {', '.join(DATASET_TYPES)}")
    
    if data_type not in DATASET_TYPES:
        raise ValueError(f"无效的数据集类型: {data_type}，可选值: {', '.join(DATASET_TYPES)}")
    
    conn = None
    cursor = None
    start_time = time.time()
    gz_path = Path(gz_file_path)
    filename = gz_path.name
    
    try:
        # 连接数据库
        db_config = get_db_config(machine_id)
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 检查文件是否已导入
        if is_file_imported(cursor, filename, data_type):
            print(f"⊙ 跳过（已导入）: {filename} [{data_type}]")
            # 删除已导入的文件以释放空间
            delete_gz_file(gz_path)
            return
        
        print(f"处理文件: {filename} [{data_type}]")
        
        # 关闭同步提交，提速20-30%（临时数据可接受风险）
        cursor.execute("SET LOCAL synchronous_commit = OFF;")
        
        # 根据数据集类型获取COPY SQL
        copy_sql = get_copy_sql(data_type)
        
        total_count = 0

        def row_iterator():
            nonlocal total_count

            try:
                with gzip.open(gz_file_path, 'rt', encoding='utf-8', errors='replace') as f:
                    for line in f:
                        try:
                            data = orjson.loads(line.strip())
                        except Exception:
                            continue

                        corpusid = data.get('corpusid')
                        if corpusid is None:
                            continue

                        # 根据数据集类型提取不同的字段
                        data_to_store = None
                        
                        if data_type in ('s2orc', 's2orc_v2'):
                            # s2orc / s2orc_v2：优先使用 content，否则组合 body + bibliography
                            data_to_store = data.get('content')
                            if data_to_store is None:
                                data_to_store = {}
                                if 'body' in data:
                                    data_to_store['body'] = data['body']
                                if 'bibliography' in data:
                                    data_to_store['bibliography'] = data['bibliography']
                                
                                # 如果既没有 body 也没有 bibliography，跳过该记录
                                if not data_to_store:
                                    continue
                        
                        elif data_type in ('embeddings_specter_v1', 'embeddings_specter_v2'):
                            # embeddings：直接提取 vector 字段
                            if 'vector' in data:
                                data_to_store = data['vector']
                            else:
                                continue
                        
                        elif data_type == 'citations':
                            # citations：暂不处理
                            continue
                        
                        else:
                            # 未知数据集类型
                            continue

                        content_json = orjson.dumps(data_to_store).decode('utf-8')
                        # TEXT格式：手动转义特殊字符
                        escaped = content_json.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                        row = f"{corpusid}\t{escaped}\tf\n"
                        total_count += 1
                        yield row.encode('utf-8')

            except Exception as e:
                raise RuntimeError(f"文件读取失败: {e}") from e

        parse_start = time.time()
        print("读取并解析gz文件...")

        try:
            cursor.copy_expert(copy_sql, CopyStream(row_iterator()))
        except RuntimeError as e:
            print(f"✗ 文件读取失败: {e}")
            print(f"  跳过该文件，继续处理下一个...")
            log_failed_file(filename, data_type, str(e))
            delete_gz_file(gz_path)
            return
 
        # 记录已成功导入的文件
        log_imported_file(cursor, filename, data_type)
 
        conn.commit()
        
        # 性能统计
        total_time = time.time() - start_time
        speed = total_count / total_time if total_time > 0 else 0
        
        print(f"✓ 成功导入 {total_count} 条记录 [{data_type}]")
        print(f"  耗时: {total_time:.2f}秒 | 速度: {speed:.0f} 条/秒")
        
        # 删除成功导入的文件以释放空间
        delete_gz_file(gz_path)
        
    except Exception as e:
        print(f"✗ 导入失败: {e}")
        if conn:
            conn.rollback()
        # 删除失败的文件以释放空间
        delete_gz_file(gz_path)
        raise
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def import_multiple_gz_fast(gz_directory, data_type=None, delete_gz=False, machine_id='machine0'):
    """
    顺序导入目录下的所有gz文件
    使用分块COPY和优化的解析策略
    
    Args:
        gz_directory: 包含gz文件的目录
        data_type: 数据集类型（必需，用于记录和跳过已处理文件）
        delete_gz: 是否在处理完成后删除所有gz文件（默认False）
        machine_id: 目标机器ID
    """
    if data_type is None:
        raise ValueError(f"必须指定--dataset参数，可选值: {', '.join(DATASET_TYPES)}")
    
    if data_type not in DATASET_TYPES:
        raise ValueError(f"无效的数据集类型: {data_type}，可选值: {', '.join(DATASET_TYPES)}")
    
    gz_dir = Path(gz_directory)
    all_gz_files = sorted(gz_dir.glob("*.gz"))
    
    if not all_gz_files:
        print(f"在 {gz_directory} 中没有找到gz文件")
        return
    
    conn = None
    cursor = None
    overall_start = time.time()
    
    # 启动磁盘空间监控
    start_cleanup_monitor(gz_directory, data_type, machine_id)
    
    try:
        # 连接数据库
        db_config = get_db_config(machine_id)
        print(f"连接到数据库 [{machine_id}: {db_config['database']}:{db_config['port']}]...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # 检查已导入的文件
        cursor.execute(
            f"SELECT filename FROM {GZ_LOG_TABLE} WHERE data_type = %s",
            (data_type,)
        )
        imported_files = set(row[0] for row in cursor.fetchall())
        
        # 如果启用了delete_gz，删除已成功导入的文件（释放空间）
        if delete_gz:
            skipped_imported = [f for f in all_gz_files if f.name in imported_files]
            if skipped_imported:
                print(f"正在删除 {len(skipped_imported)} 个已导入文件以释放空间...")
                deleted_imported = 0
                for gz_file in skipped_imported:
                    try:
                        if gz_file.exists():
                            gz_file.unlink()
                            deleted_imported += 1
                    except Exception:
                        # 静默忽略删除错误
                        pass
                if deleted_imported > 0:
                    print(f"✓ 已删除 {deleted_imported} 个已导入文件")
        
        # 加载已知失败的文件
        failed_files = load_failed_files(data_type)
        if failed_files:
            print(f"从失败日志中加载了 {len(failed_files)} 个已知失败文件")
        
        # 如果启用了delete_gz，删除已知失败的文件（释放空间）
        if delete_gz:
            skipped_failed = [f for f in all_gz_files if f.name in failed_files]
            if skipped_failed:
                print(f"正在删除 {len(skipped_failed)} 个已知失败文件以释放空间...")
                deleted_failed = 0
                for gz_file in skipped_failed:
                    try:
                        if gz_file.exists():
                            gz_file.unlink()
                            deleted_failed += 1
                    except Exception:
                        # 静默忽略删除错误
                        pass
                if deleted_failed > 0:
                    print(f"✓ 已删除 {deleted_failed} 个已知失败文件")
        
        # 过滤待处理文件（排除已导入和已知失败的）
        gz_files = [f for f in all_gz_files if f.name not in imported_files and f.name not in failed_files]
        
        print("=" * 80)
        print(f"数据集: {data_type}")
        print(f"总文件数: {len(all_gz_files)}")
        if delete_gz:
            print(f"已导入（已删除）: {len(imported_files)}")
            print(f"已知失败（已删除）: {len(failed_files)}")
        else:
            print(f"已导入（跳过）: {len(imported_files)}")
            print(f"已知失败（跳过）: {len(failed_files)}")
        print(f"待处理: {len(gz_files)}")
        print("=" * 80)
        
        if not gz_files:
            print("没有待处理文件")
            return
        
        # 关闭同步提交，提速20-30%
        cursor.execute("SET synchronous_commit = OFF;")
        
        # 根据数据集类型获取COPY SQL
        copy_sql = get_copy_sql(data_type)
        
        total_imported = 0
        skipped_count = 0
        failed_count = 0
        import_times = []
        
        for i, gz_file in enumerate(gz_files, 1):
            filename = gz_file.name
            
            file_start = time.time()
            print(f"\n[{i}/{len(gz_files)}] 处理文件: {filename}")
            
            file_count = 0

            try:
                print("  读取并解析gz文件...")
                def row_iterator():
                    nonlocal file_count

                    with gzip.open(gz_file, 'rt', encoding='utf-8', errors='replace') as f:
                        for line in f:
                            try:
                                data = orjson.loads(line.strip())
                            except Exception:
                                continue

                            corpusid = data.get('corpusid')
                            if corpusid is None:
                                continue

                            # 根据数据集类型提取不同的字段
                            data_to_store = None
                            
                            if data_type in ('s2orc', 's2orc_v2'):
                                # s2orc / s2orc_v2：优先使用 content，否则组合 body + bibliography
                                data_to_store = data.get('content')
                                if data_to_store is None:
                                    data_to_store = {}
                                    if 'body' in data:
                                        data_to_store['body'] = data['body']
                                    if 'bibliography' in data:
                                        data_to_store['bibliography'] = data['bibliography']
                                    
                                    # 如果既没有 body 也没有 bibliography，跳过该记录
                                    if not data_to_store:
                                        continue
                            
                            elif data_type in ('embeddings_specter_v1', 'embeddings_specter_v2'):
                                # embeddings：直接提取 vector 字段
                                if 'vector' in data:
                                    data_to_store = data['vector']
                                else:
                                    continue
                            
                            elif data_type == 'citations':
                                # citations：暂不处理
                                continue
                            
                            else:
                                # 未知数据集类型
                                continue

                            content_json = orjson.dumps(data_to_store).decode('utf-8')
                            # TEXT格式：手动转义特殊字符
                            escaped = content_json.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                            row = f"{corpusid}\t{escaped}\tf\n"
                            file_count += 1
                            yield row.encode('utf-8')

                cursor.copy_expert(copy_sql, CopyStream(row_iterator()))

                # 记录已成功导入的文件
                log_imported_file(cursor, filename, data_type)

                conn.commit()

                file_time = time.time() - file_start
                import_times.append(file_time)
                total_imported += file_count
                file_speed = file_count / file_time if file_time > 0 else 0

                print(f"  ✓ {filename} 完成 ({file_count} 条 | {file_time:.2f}秒 | {file_speed:.0f} 条/秒)")
 
            except Exception as e:
                print(f"  ✗ {filename} 失败: {e}")
                print(f"  跳过该文件，继续处理下一个...")
                log_failed_file(filename, data_type, str(e))
                failed_count += 1
                # 回滚当前事务，以便继续处理下一个文件
                if conn:
                    conn.rollback()
                continue
        
        import_total_time = time.time() - overall_start
        success_count = len(gz_files) - failed_count
        avg_speed = total_imported / import_total_time if import_total_time > 0 else 0
        
        print("\n" + "=" * 80)
        print(f"【导入完成】")
        print(f"  数据集类型: {data_type}")
        print(f"  待处理文件数: {len(gz_files)} 个")
        print(f"  成功导入: {success_count} 个")
        print(f"  失败: {failed_count} 个")
        print(f"  记录数: {total_imported} 条")
        print(f"  总耗时: {import_total_time:.2f} 秒")
        if total_imported > 0:
            print(f"  平均速度: {avg_speed:.0f} 条/秒")
        print("=" * 80)
        
        if failed_count > 0:
            print(f"\n⚠️  有 {failed_count} 个文件导入失败，详细信息已记录到:")
            print(f"   {get_failed_log_path(data_type)}")
        
        # 根据开关决定是否清空整个目录的所有 .gz 文件
        if delete_gz:
            print("\n" + "=" * 80)
            print(f"正在清空目录 {gz_directory} 中的所有gz文件...")
            remaining_gz_files = list(gz_dir.glob("*.gz"))
            deleted_count = 0
            
            for gz_file in remaining_gz_files:
                try:
                    if gz_file.exists():
                        gz_file.unlink()
                        deleted_count += 1
                except Exception as e:
                    print(f"⚠️  删除 {gz_file.name} 失败: {e}")
            
            print(f"✓ 已删除 {deleted_count} 个gz文件")
            print("=" * 80)
        else:
            remaining_count = len(list(gz_dir.glob("*.gz")))
            if remaining_count > 0:
                print(f"\n💡 目录中还有 {remaining_count} 个gz文件未删除")
                print(f"   如需清空，请添加 --delete-gz 参数")
        
        print("\n提示: 数据已导入，无需排序（更新阶段会在Python层面排序）")
        
        # 记录日志
        log_performance(
            "GZ导入完成",
            dataset=data_type,
            success=success_count,
            skipped=skipped_count,
            failed=failed_count,
            records=total_imported,
            time_sec=f"{import_total_time:.2f}",
            speed_per_sec=f"{avg_speed:.0f}"
        )
        
    except Exception as e:
        print(f"✗ 导入失败: {e}")
        if conn:
            conn.rollback()
        raise
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="从gz文件快速导入数据到临时表",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
数据集类型:
  {', '.join(DATASET_TYPES)}

使用示例:
  # 导入到 machine0 (默认)
  python batch_update/import_gz_to_temp.py D:\\gz_temp\\s2orc --dataset s2orc
  
  # 导入到 machine1
  python batch_update/import_gz_to_temp.py D:\\gz_temp\\s2orc --dataset s2orc --machine machine1
  
  # 导入并删除所有gz文件
  python batch_update/import_gz_to_temp.py D:\\gz_temp\\s2orc --dataset s2orc --delete-gz
  
  # 自动流水线模式（导入→建索引→更新JSONL）
  python batch_update/import_gz_to_temp.py D:\\gz_temp\\s2orc --dataset s2orc --auto-pipeline
        """
    )
    parser.add_argument("path", help="gz文件路径或包含gz文件的目录")
    parser.add_argument(
        "--dataset",
        required=True,
        choices=DATASET_TYPES,
        help="数据集类型（必需）"
    )
    parser.add_argument("--machine", default="machine0", choices=list(MACHINE_DB_MAP.keys()), 
                        help="目标机器 (默认: machine0)")
    parser.add_argument(
        "--delete-gz",
        action="store_true",
        help="处理完成后删除所有gz文件（默认不删除，需要明确指定）"
    )
    parser.add_argument(
        "--auto-pipeline",
        action="store_true",
        help="自动流水线模式：导入完成后自动执行建索引和JSONL更新"
    )
    args = parser.parse_args()
    
    path = Path(args.path)
    
    # 执行导入
    if path.is_file():
        import_gz_to_temp_fast(path, data_type=args.dataset, machine_id=args.machine)
    elif path.is_dir():
        import_multiple_gz_fast(path, data_type=args.dataset, delete_gz=args.delete_gz, machine_id=args.machine)
    else:
        print(f"错误: {path} 不是有效的文件或目录")
        sys.exit(1)
    
    # 如果启用自动流水线，继续执行后续步骤
    if args.auto_pipeline:
        print("\n" + "=" * 80)
        print("【自动流水线】步骤 2/3: 创建索引")
        print("=" * 80)
        
        try:
            from init_temp_table import create_indexes
            create_indexes(machine_id=args.machine)
        except Exception as e:
            print(f"✗ 创建索引失败: {e}")
            sys.exit(1)
        
        print("\n" + "=" * 80)
        print("【自动流水线】步骤 3/3: 更新JSONL文件")
        print("=" * 80)
        
        try:
            from jsonl_batch_updater import JSONLBatchUpdater
            updater = JSONLBatchUpdater(machine_id=args.machine)
            updater.run()
        except Exception as e:
            print(f"✗ 更新JSONL失败: {e}")
            sys.exit(1)
        
        print("\n" + "=" * 80)
        print("【自动流水线完成】所有步骤执行成功！")
        print("=" * 80)

