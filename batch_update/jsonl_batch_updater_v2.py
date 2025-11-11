"""
JSONL批量更新器 V2 - Machine2作为主机
新逻辑：
1. Machine2作为主机，处理JSONL文件
2. 从Machine2的temp_import获取本地字段（specter_v1/v2, content）
3. 通过局域网向Machine0查询citations/references数据（使用JSONL文件的corpusid范围 [start_id, start_id+50000]）
4. 合并数据后更新JSONL文件
5. 仅标记Machine2中已有字段的corpusid为完成
"""
import sys
from pathlib import Path

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import orjson
import psycopg2
from collections import defaultdict
from tqdm import tqdm
from datetime import datetime
import time
import mmap
from concurrent.futures import ThreadPoolExecutor
from db_config import get_db_config

# 尝试导入 win32file（Windows 优化）
try:
    import win32file
    HAS_WIN32FILE = True
except ImportError:
    HAS_WIN32FILE = False

# ==================== 配置区 ====================
FINAL_DELIVERY_DIR = Path("F:/final_delivery")  # JSONL文件目录（外置硬盘）
LOCAL_TEMP_DIR = Path("D:/jsonl_copy")           # 本地SSD缓存目录
LOG_DIR = Path(__file__).parent.parent / "logs" / "batch_update"
RUNNING_LOG = Path(__file__).parent.parent / "logs" / "running.log"

TEMP_TABLE = "temp_import"
JSONL_CORPUSID_RANGE = 50000  # JSONL文件corpusid范围大小
TITLE_BATCH_SIZE = 100000     # title查询批次大小
BATCH_MARK_SIZE = 100000      # 批量标记完成的批次大小

# 初始化
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_TEMP_DIR.mkdir(parents=True, exist_ok=True)
FAILED_LOG = LOG_DIR / f"jsonl_update_v2_failed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

if not HAS_WIN32FILE:
    raise RuntimeError("未检测到 pywin32。请先安装: pip install pywin32")
# ==================================================


def log_performance(stage, **metrics):
    """记录性能日志"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    metrics_str = " | ".join([f"{k}={v}" for k, v in metrics.items()])
    log_line = f"[{timestamp}] {stage} | {metrics_str}\n"
    with open(RUNNING_LOG, 'a', encoding='utf-8') as f:
        f.write(log_line)


def extract_first_corpusid(jsonl_path: Path) -> int:
    """提取JSONL文件第一行的corpusid"""
    with open(jsonl_path, 'rb') as f:
        first_line = f.readline()
        if not first_line:
            raise ValueError(f"文件为空: {jsonl_path}")
        
        data = orjson.loads(first_line)
        corpusid = data.get('corpusid')
        if corpusid is None:
            raise ValueError(f"第一行没有corpusid字段: {jsonl_path}")
        
        return int(corpusid)


class JSONLBatchUpdaterV2:
    """JSONL批量更新器V2 - Machine2作为主机"""
    
    def __init__(self, primary_machine='machine2', auxiliary_machine='machine0'):
        """
        初始化更新器
        
        Args:
            primary_machine: 主机器（处理JSONL，获取本地字段） - 默认machine2
            auxiliary_machine: 辅助机器（提供citations/references查询） - 默认machine0
        """
        self.primary_machine = primary_machine
        self.auxiliary_machine = auxiliary_machine
        self.connections = {}
        self.cursors = {}
        self.failed_corpusids = []
        
        print(f"初始化更新器:")
        print(f"  主机器: {primary_machine} (处理JSONL + 本地字段)")
        print(f"  辅助机器: {auxiliary_machine} (提供citations/references)")
    
    def connect_db(self):
        """连接两台机器的数据库"""
        print("\n连接数据库...")
        
        for machine_id in [self.primary_machine, self.auxiliary_machine]:
            db_config = get_db_config(machine_id)
            print(f"  [{machine_id}: {db_config['database']}:{db_config['port']}]...", end=' ', flush=True)
            
            try:
                conn = psycopg2.connect(**db_config)
                cursor = conn.cursor()
                self.connections[machine_id] = conn
                self.cursors[machine_id] = cursor
                print("✓")
            except Exception as e:
                print(f"✗ 失败: {e}")
                raise
    
    def close_db(self):
        """关闭数据库连接"""
        for conn in self.connections.values():
            if conn:
                conn.close()
        for cursor in self.cursors.values():
            if cursor:
                cursor.close()
    
    def get_grouped_files(self):
        """获取主机器temp_import中待处理的数据，按文件分组
        
        Returns:
            dict: {filename: [corpusid列表]}
        """
        print("\n查询待处理数据（按文件分组）...")
        cursor = self.cursors[self.primary_machine]
        
        cursor.execute(f"""
            SELECT 
                m.batch_filename,
                array_agg(t.corpusid) as corpusids
            FROM {TEMP_TABLE} t
            INNER JOIN corpusid_to_file m ON t.corpusid = m.corpusid
            WHERE t.is_done = FALSE
            GROUP BY m.batch_filename
        """)
        
        grouped = {}
        total_count = 0
        
        for batch_filename, corpusids in cursor:
            grouped[batch_filename] = corpusids
            total_count += len(corpusids)
        
        print(f"  ✓ 发现 {len(grouped)} 个文件，共 {total_count:,} 条待处理记录")
        return grouped, total_count
    
    def get_machine2_updates(self, corpusid_list):
        """从Machine2获取本地字段数据
        
        Args:
            corpusid_list: corpusid列表
        
        Returns:
            dict: {corpusid: {字段: 值}}
        """
        cursor = self.cursors[self.primary_machine]
        
        cursor.execute(f"""
            SELECT corpusid, specter_v1, specter_v2, content
            FROM {TEMP_TABLE}
            WHERE corpusid = ANY(%s) AND is_done = FALSE
        """, (corpusid_list,))
        
        updates = {}
        for corpusid, specter_v1, specter_v2, content in cursor:
            updates[corpusid] = {}
            
            # 解析JSON字段
            if specter_v1:
                try:
                    updates[corpusid]['specter_v1'] = orjson.loads(specter_v1)
                except:
                    pass
            
            if specter_v2:
                try:
                    updates[corpusid]['specter_v2'] = orjson.loads(specter_v2)
                except:
                    pass
            
            if content:
                try:
                    updates[corpusid]['content'] = orjson.loads(content)
                except:
                    pass
        
        return updates
    
    def query_citations_from_machine0(self, start_corpusid, end_corpusid):
        """从Machine0查询citations/references数据（通过局域网）
        
        Args:
            start_corpusid: 起始corpusid
            end_corpusid: 结束corpusid（包含）
        
        Returns:
            dict: {
                corpusid: {
                    'citations': [{"corpusid": x, "title": "..."}],
                    'references': [{"corpusid": y, "title": "..."}]
                }
            }
        """
        cursor = self.cursors[self.auxiliary_machine]
        
        # 步骤1: 查询temp_references（获取ref_ids数组）
        cursor.execute("""
            SELECT corpusid, ref_ids
            FROM temp_references
            WHERE corpusid >= %s AND corpusid <= %s
        """, (start_corpusid, end_corpusid))
        
        ref_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 步骤2: 查询temp_citations（获取cite_ids数组）
        cursor.execute("""
            SELECT corpusid, cite_ids
            FROM temp_citations
            WHERE corpusid >= %s AND corpusid <= %s
        """, (start_corpusid, end_corpusid))
        
        cite_data = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 步骤3: 收集所有需要查询title的corpusid
        all_ids = set()
        for ref_ids in ref_data.values():
            if ref_ids:
                all_ids.update(ref_ids)
        for cite_ids in cite_data.values():
            if cite_ids:
                all_ids.update(cite_ids)
        
        # 步骤4: 分批查询title（避免IN子句过大）
        title_map = {}
        if all_ids:
            all_ids_list = list(all_ids)
            
            for i in range(0, len(all_ids_list), TITLE_BATCH_SIZE):
                batch_ids = all_ids_list[i:i+TITLE_BATCH_SIZE]
                
                cursor.execute("""
                    SELECT corpusid, COALESCE(title, '') as title
                    FROM corpusid_mapping_title
                    WHERE corpusid = ANY(%s)
                """, (batch_ids,))
                
                title_map.update({row[0]: row[1] for row in cursor.fetchall()})
        
        # 步骤5: 构造最终结果（JSON格式）
        result = {}
        
        # 合并所有可能的corpusid（有references或citations的）
        all_corpusids = set(ref_data.keys()) | set(cite_data.keys())
        
        for corpusid in all_corpusids:
            result[corpusid] = {}
            
            # 构造references
            ref_ids = ref_data.get(corpusid, [])
            if ref_ids:
                result[corpusid]['references'] = [
                    {"corpusid": rid, "title": title_map.get(rid, "")}
                    for rid in ref_ids
                ]
            else:
                result[corpusid]['references'] = []
            
            # 构造citations
            cite_ids = cite_data.get(corpusid, [])
            if cite_ids:
                result[corpusid]['citations'] = [
                    {"corpusid": cid, "title": title_map.get(cid, "")}
                    for cid in cite_ids
                ]
            else:
                result[corpusid]['citations'] = []
        
        return result
    
    def update_jsonl_file(self, jsonl_path: Path, machine2_updates, machine0_updates):
        """更新JSONL文件（mmap方式）
        
        Args:
            jsonl_path: JSONL文件路径
            machine2_updates: Machine2本地字段数据
            machine0_updates: Machine0查询的citations/references数据
        
        Returns:
            tuple: (成功更新数, 失败corpusid列表)
        """
        temp_output = jsonl_path.parent / f"{jsonl_path.name}.out"
        success_count = 0
        failed = []
        
        try:
            with open(jsonl_path, 'rb') as infile, \
                 open(temp_output, 'wb') as outfile:
                
                # 使用mmap映射输入文件
                with mmap.mmap(infile.fileno(), 0, access=mmap.ACCESS_READ) as mmapped:
                    line_start = 0
                    
                    while True:
                        # 查找下一个换行符
                        line_end = mmapped.find(b'\n', line_start)
                        if line_end == -1:
                            # 最后一行（没有换行符）
                            if line_start < len(mmapped):
                                line = mmapped[line_start:]
                            else:
                                break
                        else:
                            line = mmapped[line_start:line_end]
                        
                        if not line.strip():
                            line_start = line_end + 1
                            continue
                        
                        try:
                            # 解析JSON
                            record = orjson.loads(line)
                            corpusid = record.get('corpusid')
                            
                            if corpusid is None:
                                outfile.write(line + b'\n')
                                line_start = line_end + 1
                                continue
                            
                            # 合并Machine2的本地字段
                            if corpusid in machine2_updates:
                                m2_data = machine2_updates[corpusid]
                                
                                if 'specter_v1' in m2_data:
                                    if 'embedding' not in record:
                                        record['embedding'] = {}
                                    record['embedding']['specter_v1'] = m2_data['specter_v1']
                                
                                if 'specter_v2' in m2_data:
                                    if 'embedding' not in record:
                                        record['embedding'] = {}
                                    record['embedding']['specter_v2'] = m2_data['specter_v2']
                                
                                if 'content' in m2_data:
                                    record['content'] = m2_data['content']
                            
                            # 合并Machine0的citations/references字段
                            if corpusid in machine0_updates:
                                m0_data = machine0_updates[corpusid]
                                
                                if 'citations' in m0_data:
                                    record['citations'] = m0_data['citations']
                                
                                if 'references' in m0_data:
                                    record['references'] = m0_data['references']
                            
                            # 写入更新后的记录
                            outfile.write(orjson.dumps(record) + b'\n')
                            success_count += 1
                            
                        except Exception as e:
                            # 解析或更新失败，保留原始行
                            outfile.write(line + b'\n')
                            if corpusid:
                                failed.append(corpusid)
                        
                        if line_end == -1:
                            break
                        line_start = line_end + 1
        
        except Exception as e:
            print(f"    ✗ 文件更新失败: {e}")
            if temp_output.exists():
                temp_output.unlink()
            raise
        
        return success_count, failed
    
    def mark_as_done(self, corpusid_list):
        """批量标记Machine2中的记录为已完成（仅标记有本地字段的corpusid）
        
        Args:
            corpusid_list: corpusid列表
        """
        if not corpusid_list:
            return
        
        cursor = self.cursors[self.primary_machine]
        conn = self.connections[self.primary_machine]
        
        cursor.execute(f"""
            UPDATE {TEMP_TABLE}
            SET is_done = TRUE
            WHERE corpusid = ANY(%s)
        """, (corpusid_list,))
        
        conn.commit()
    
    def run(self):
        """执行完整更新流程"""
        print("=" * 80)
        print("JSONL批量更新器 V2 - Machine2作为主机")
        print("=" * 80)
        
        overall_start = time.time()
        
        try:
            # 连接数据库
            self.connect_db()
            
            # 获取待处理文件列表
            grouped_files, total_count = self.get_grouped_files()
            
            if not grouped_files:
                print("\n没有待处理的数据")
                return
            
            # 逐文件处理
            total_success = 0
            total_failed = 0
            pending_marks = []  # 待标记的corpusid（批量优化）
            
            print(f"\n开始处理 {len(grouped_files)} 个文件...")
            
            with tqdm(grouped_files.items(), desc="处理进度", unit="file") as pbar:
                for filename, machine2_corpusids in pbar:
                    file_start = time.time()
                    
                    try:
                        # 步骤1: 复制文件到本地SSD
                        src_file = FINAL_DELIVERY_DIR / filename
                        local_file = LOCAL_TEMP_DIR / filename
                        
                        if not src_file.exists():
                            print(f"\n  ✗ 文件不存在: {src_file}")
                            continue
                        
                        win32file.CopyFile(str(src_file), str(local_file), 0)
                        
                        # 步骤2: 提取JSONL文件的corpusid范围
                        start_corpusid = extract_first_corpusid(local_file)
                        end_corpusid = start_corpusid + JSONL_CORPUSID_RANGE
                        
                        # 步骤3: 从Machine2获取本地字段数据
                        machine2_updates = self.get_machine2_updates(machine2_corpusids)
                        
                        # 步骤4: 从Machine0查询citations/references（使用JSONL范围）
                        machine0_updates = self.query_citations_from_machine0(
                            start_corpusid, end_corpusid
                        )
                        
                        # 步骤5: 更新JSONL文件
                        success, failed = self.update_jsonl_file(
                            local_file, machine2_updates, machine0_updates
                        )
                        
                        # 步骤6: 回写文件到外置硬盘
                        temp_output = LOCAL_TEMP_DIR / f"{filename}.out"
                        if temp_output.exists():
                            win32file.CopyFile(str(temp_output), str(src_file), 0)
                            temp_output.unlink()
                        
                        # 步骤7: 清理本地缓存
                        local_file.unlink(missing_ok=True)
                        
                        # 步骤8: 批量标记完成（仅标记Machine2有数据的corpusid）
                        pending_marks.extend(machine2_corpusids)
                        if len(pending_marks) >= BATCH_MARK_SIZE:
                            self.mark_as_done(pending_marks)
                            pending_marks.clear()
                        
                        total_success += success
                        total_failed += len(failed)
                        
                        if failed:
                            self.failed_corpusids.extend(failed)
                            with open(FAILED_LOG, 'a', encoding='utf-8') as f:
                                for cid in failed:
                                    f.write(f"{cid}\t{filename}\tFAILED\n")
                        
                        file_time = time.time() - file_start
                        pbar.set_postfix_str(f"耗时{file_time:.1f}s | 成功{success}")
                        
                    except Exception as e:
                        print(f"\n  ✗ 文件处理失败 [{filename}]: {e}")
                        # 清理可能的临时文件
                        (LOCAL_TEMP_DIR / filename).unlink(missing_ok=True)
                        (LOCAL_TEMP_DIR / f"{filename}.out").unlink(missing_ok=True)
                        continue
            
            # 处理剩余待标记记录
            if pending_marks:
                self.mark_as_done(pending_marks)
                pending_marks.clear()
            
            # 统计结果
            total_time = time.time() - overall_start
            
            print("\n" + "=" * 80)
            print("【完成统计】")
            print("=" * 80)
            print(f"  成功更新: {total_success} 条")
            print(f"  失败记录: {total_failed} 条")
            print(f"  处理文件: {len(grouped_files)} 个")
            print(f"  总耗时: {total_time:.2f}秒")
            if total_time > 0:
                print(f"  更新速度: {total_success/total_time:.0f} 条/秒")
            print("=" * 80)
            
            if self.failed_corpusids:
                print(f"\n失败记录已保存到: {FAILED_LOG}")
            
            log_performance(
                "V2更新完成",
                files=len(grouped_files),
                success=total_success,
                failed=total_failed,
                time_sec=f"{total_time:.2f}",
                speed=f"{total_success/total_time:.0f}" if total_time > 0 else "0"
            )
        
        finally:
            self.close_db()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="JSONL批量更新器 V2 - Machine2作为主机",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # Machine2作为主机（默认配置）
  python batch_update/jsonl_batch_updater_v2.py
  
  # 自定义机器配置
  python batch_update/jsonl_batch_updater_v2.py --primary machine2 --auxiliary machine0

工作流程:
  1. 从Machine2的temp_import表查询待处理数据（按文件分组）
  2. 逐个处理JSONL文件：
     - 复制文件到本地SSD
     - 提取文件首行corpusid，计算范围 [X, X+50000]
     - 从Machine2获取本地字段（specter_v1/v2, content）
     - 从Machine0查询citations/references（使用JSONL范围）
     - 合并数据，更新JSONL文件
     - 回写文件到外置硬盘
     - 标记Machine2中已处理的corpusid为完成
  3. 显示统计结果

注意事项:
  - JSONL文件的corpusid范围用于向Machine0查询数据
  - Machine2的temp_import表用于获取本地字段和标记完成
  - 两者独立，不需要交集
        """
    )
    
    parser.add_argument("--primary", default="machine2", 
                        help="主机器（处理JSONL，获取本地字段，默认: machine2）")
    parser.add_argument("--auxiliary", default="machine0", 
                        help="辅助机器（提供citations/references查询，默认: machine0）")
    
    args = parser.parse_args()
    
    print(f"启动JSONL批量更新器V2")
    print(f"  主机器: {args.primary}")
    print(f"  辅助机器: {args.auxiliary}")
    
    updater = JSONLBatchUpdaterV2(
        primary_machine=args.primary,
        auxiliary_machine=args.auxiliary
    )
    updater.run()

