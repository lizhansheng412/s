"""
JSONL批量更新器 V3 - 直接生成新JSONL文件
简化逻辑：
1. 从Machine2的corpusid_to_file表获取文件列表（已分组，每文件≤50000个corpusid）
2. 通过局域网向Machine0查询citations/references数据
3. 直接生成新JSONL文件到E:\copy_final目录
4. 仅包含5个字段：corpusid, citations, references, detailsOfCitations, detailsOfReference
5. 跳过没有citations和references的corpusid
"""
import sys
from pathlib import Path

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import orjson
import psycopg2
from tqdm import tqdm
from datetime import datetime
import time
from db_config import get_db_config

# ==================== 配置区 ====================
COPY_FINAL_DIR = Path("E:/copy_final")      # 新JSONL输出目录
RUNNING_LOG = Path(__file__).parent.parent / "logs" / "running.log"

TITLE_BATCH_SIZE = 100000                   # title查询批次大小
OUTPUT_SUFFIX = "_part2.jsonl"              # 输出文件后缀

# 初始化
COPY_FINAL_DIR.mkdir(parents=True, exist_ok=True)
FAILED_LOG = COPY_FINAL_DIR / f"jsonl_v3_failed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
# ==================================================


def log_performance(stage, **metrics):
    """记录性能日志"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    metrics_str = " | ".join([f"{k}={v}" for k, v in metrics.items()])
    log_line = f"[{timestamp}] {stage} | {metrics_str}\n"
    with open(RUNNING_LOG, 'a', encoding='utf-8') as f:
        f.write(log_line)


class JSONLBatchUpdaterV3:
    """JSONL批量更新器V3 - 直接生成新文件"""
    
    def __init__(self, primary_machine='machine2', auxiliary_machine='machine0'):
        """
        初始化更新器
        
        Args:
            primary_machine: 主机器（提供corpusid_to_file表） - 默认machine2
            auxiliary_machine: 辅助机器（提供citations/references查询） - 默认machine0
        """
        self.primary_machine = primary_machine
        self.auxiliary_machine = auxiliary_machine
        self.connections = {}
        self.cursors = {}
        self.failed_files = []
        
        print(f"初始化更新器V3:")
        print(f"  主机器: {primary_machine} (提供corpusid列表)")
        print(f"  辅助机器: {auxiliary_machine} (提供citations/references)")
        print(f"  输出目录: {COPY_FINAL_DIR}")
    
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
        """从Machine2获取文件列表（已按文件分组）
        
        Returns:
            dict: {filename: [corpusid列表]}
            int: 总corpusid数量
        """
        print("\n从Machine2查询corpusid_to_file表（按文件分组）...")
        cursor = self.cursors[self.primary_machine]
        
        query_start = time.time()
        cursor.execute("""
            SELECT 
                batch_filename,
                array_agg(corpusid ORDER BY corpusid) as corpusids
            FROM corpusid_to_file
            GROUP BY batch_filename
            ORDER BY batch_filename
        """)
        
        grouped = {}
        total_count = 0
        
        for batch_filename, corpusids in cursor:
            grouped[batch_filename] = corpusids
            total_count += len(corpusids)
        
        query_time = time.time() - query_start
        print(f"  ✓ 发现 {len(grouped)} 个文件，共 {total_count:,} 个corpusid")
        print(f"  ✓ 查询耗时: {query_time:.2f}秒")
        
        log_performance(
            "查询corpusid_to_file",
            files=len(grouped),
            total_corpusids=f"{total_count:,}",
            time_sec=f"{query_time:.2f}"
        )
        
        return grouped, total_count
    
    def query_citations_from_machine0(self, corpusid_list, filename):
        """从Machine0查询citations/references数据（通过局域网）
        
        Args:
            corpusid_list: corpusid列表（已排序）
            filename: 文件名（用于日志）
        
        Returns:
            dict: {
                corpusid: {
                    'citations': [{"corpusid": x, "title": "..."}],
                    'references': [{"corpusid": y, "title": "..."}]
                }
            }
        """
        query_start = time.time()
        cursor = self.cursors[self.auxiliary_machine]
        
        # 步骤1: 查询temp_references（获取ref_ids数组）
        t1 = time.time()
        cursor.execute("""
            SELECT corpusid, ref_ids
            FROM temp_references
            WHERE corpusid = ANY(%s)
        """, (corpusid_list,))
        ref_data = {row[0]: row[1] for row in cursor.fetchall()}
        t_ref = time.time() - t1
        
        # 步骤2: 查询temp_citations（获取cite_ids数组）
        t2 = time.time()
        cursor.execute("""
            SELECT corpusid, cite_ids
            FROM temp_citations
            WHERE corpusid = ANY(%s)
        """, (corpusid_list,))
        cite_data = {row[0]: row[1] for row in cursor.fetchall()}
        t_cite = time.time() - t2
        
        # 步骤3: 收集所有需要查询title的corpusid
        all_ids = set()
        for ref_ids in ref_data.values():
            if ref_ids:
                all_ids.update(ref_ids)
        for cite_ids in cite_data.values():
            if cite_ids:
                all_ids.update(cite_ids)
        
        # 步骤4: 分批查询title（避免IN子句过大）
        t3 = time.time()
        title_map = {}
        title_batches = 0
        if all_ids:
            all_ids_list = list(all_ids)
            
            for i in range(0, len(all_ids_list), TITLE_BATCH_SIZE):
                batch_ids = all_ids_list[i:i+TITLE_BATCH_SIZE]
                title_batches += 1
                
                cursor.execute("""
                    SELECT corpusid, COALESCE(title, '') as title
                    FROM corpusid_mapping_title
                    WHERE corpusid = ANY(%s)
                """, (batch_ids,))
                
                title_map.update({row[0]: row[1] for row in cursor.fetchall()})
        t_title = time.time() - t3
        
        # 步骤5: 构造最终结果（JSON格式）
        t4 = time.time()
        result = {}
        
        # 遍历所有corpusid
        for corpusid in corpusid_list:
            ref_ids = ref_data.get(corpusid, [])
            cite_ids = cite_data.get(corpusid, [])
            
            # 跳过没有citations和references的corpusid
            if not ref_ids and not cite_ids:
                continue
            
            result[corpusid] = {}
            
            # 构造references
            if ref_ids:
                result[corpusid]['references'] = [
                    {"corpusid": rid, "title": title_map.get(rid, "")}
                    for rid in ref_ids
                ]
            else:
                result[corpusid]['references'] = []
            
            # 构造citations
            if cite_ids:
                result[corpusid]['citations'] = [
                    {"corpusid": cid, "title": title_map.get(cid, "")}
                    for cid in cite_ids
                ]
            else:
                result[corpusid]['citations'] = []
        
        t_build = time.time() - t4
        
        # 统计并记录耗时
        total_time = time.time() - query_start
        log_performance(
            "Machine0查询",
            file=filename,
            input_corpusids=len(corpusid_list),
            ref_count=len(ref_data),
            cite_count=len(cite_data),
            title_ids=len(all_ids),
            title_batches=title_batches,
            result_count=len(result),
            skipped=len(corpusid_list) - len(result),
            time_ref=f"{t_ref:.2f}s",
            time_cite=f"{t_cite:.2f}s",
            time_title=f"{t_title:.2f}s",
            time_build=f"{t_build:.2f}s",
            total_time=f"{total_time:.2f}s"
        )
        
        return result
    
    def generate_jsonl_file(self, filename, corpusid_list, machine0_data):
        """生成新的JSONL文件
        
        Args:
            filename: 原文件名（如 6996f911.jsonl）
            corpusid_list: corpusid列表（已排序）
            machine0_data: Machine0查询的数据
        
        Returns:
            tuple: (写入记录数, 跳过记录数)
        """
        # 构造输出文件名：6996f911.jsonl -> 6996f911_part2.jsonl
        base_name = filename.replace('.jsonl', '')
        output_filename = f"{base_name}{OUTPUT_SUFFIX}"
        output_path = COPY_FINAL_DIR / output_filename
        
        write_start = time.time()
        written_count = 0
        skipped_count = 0
        
        try:
            with open(output_path, 'wb') as f:
                for corpusid in corpusid_list:
                    # 检查是否有数据
                    if corpusid not in machine0_data:
                        skipped_count += 1
                        continue
                    
                    m0_data = machine0_data[corpusid]
                    
                    # 构造完整记录（仅5个字段）
                    record = {
                        "corpusid": corpusid,
                        "citations": m0_data['citations'],
                        "detailsOfCitations": {
                            "offset": 0,
                            "data": m0_data['citations']
                        },
                        "references": m0_data['references'],
                        "detailsOfReference": {
                            "offset": 0,
                            "data": m0_data['references']
                        }
                    }
                    
                    # 写入JSONL行
                    f.write(orjson.dumps(record) + b'\n')
                    written_count += 1
            
            write_time = time.time() - write_start
            
            log_performance(
                "生成JSONL文件",
                file=output_filename,
                written=written_count,
                skipped=skipped_count,
                time_sec=f"{write_time:.2f}"
            )
            
            return written_count, skipped_count
        
        except Exception as e:
            print(f"    ✗ 文件生成失败: {e}")
            if output_path.exists():
                output_path.unlink()
            raise
    
    def run(self):
        """执行完整更新流程"""
        print("=" * 80)
        print("JSONL批量更新器 V3 - 直接生成新文件")
        print("=" * 80)
        
        overall_start = time.time()
        
        try:
            # 连接数据库
            self.connect_db()
            
            # 获取文件列表（已分组）
            grouped_files, total_corpusids = self.get_grouped_files()
            
            if not grouped_files:
                print("\n没有待处理的数据")
                return
            
            # 逐文件处理
            total_written = 0
            total_skipped = 0
            processed_files = 0
            
            print(f"\n开始处理 {len(grouped_files)} 个文件...")
            
            with tqdm(grouped_files.items(), desc="处理进度", unit="file") as pbar:
                for filename, corpusid_list in pbar:
                    file_start = time.time()
                    
                    try:
                        # 步骤1: 从Machine0查询citations/references
                        machine0_data = self.query_citations_from_machine0(
                            corpusid_list, filename
                        )
                        
                        # 步骤2: 生成新JSONL文件
                        written, skipped = self.generate_jsonl_file(
                            filename, corpusid_list, machine0_data
                        )
                        
                        total_written += written
                        total_skipped += skipped
                        processed_files += 1
                        
                        file_time = time.time() - file_start
                        pbar.set_postfix_str(
                            f"耗时{file_time:.1f}s | 写入{written} | 跳过{skipped}"
                        )
                        
                    except Exception as e:
                        print(f"\n  ✗ 文件处理失败 [{filename}]: {e}")
                        self.failed_files.append(filename)
                        
                        # 记录失败
                        with open(FAILED_LOG, 'a', encoding='utf-8') as f:
                            f.write(f"{filename}\t{str(e)}\n")
                        
                        continue
            
            # 统计结果
            total_time = time.time() - overall_start
            
            print("\n" + "=" * 80)
            print("【完成统计】")
            print("=" * 80)
            print(f"  处理文件: {processed_files}/{len(grouped_files)} 个")
            print(f"  输入corpusid: {total_corpusids:,} 个")
            print(f"  写入记录: {total_written:,} 条")
            print(f"  跳过记录: {total_skipped:,} 条")
            print(f"  写入比例: {total_written/total_corpusids*100:.2f}%")
            print(f"  总耗时: {total_time:.2f}秒 ({total_time/60:.1f}分钟)")
            if total_time > 0:
                print(f"  处理速度: {total_corpusids/total_time:.0f} corpusid/秒")
                print(f"            {total_written/total_time:.0f} 记录/秒")
            print(f"  输出目录: {COPY_FINAL_DIR}")
            print("=" * 80)
            
            if self.failed_files:
                print(f"\n失败文件 ({len(self.failed_files)}):")
                for fname in self.failed_files[:10]:  # 显示前10个
                    print(f"  - {fname}")
                if len(self.failed_files) > 10:
                    print(f"  ... 还有 {len(self.failed_files)-10} 个")
                print(f"\n详细信息已保存到: {FAILED_LOG}")
            
            log_performance(
                "V3完成",
                files=f"{processed_files}/{len(grouped_files)}",
                input_corpusids=f"{total_corpusids:,}",
                written=f"{total_written:,}",
                skipped=f"{total_skipped:,}",
                write_ratio=f"{total_written/total_corpusids*100:.2f}%",
                failed_files=len(self.failed_files),
                time_sec=f"{total_time:.2f}",
                speed_corpusid=f"{total_corpusids/total_time:.0f}" if total_time > 0 else "0",
                speed_record=f"{total_written/total_time:.0f}" if total_time > 0 else "0"
            )
        
        finally:
            self.close_db()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="JSONL批量更新器 V3 - 直接生成新JSONL文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 默认配置（machine2提供corpusid，machine0提供citations/references）
  python batch_update/jsonl_batch_updater_v3.py
  
  # 自定义机器配置
  python batch_update/jsonl_batch_updater_v3.py --primary machine2 --auxiliary machine0

工作流程:
  1. 从Machine2的corpusid_to_file表查询所有文件（按文件分组）
  2. 逐个处理文件：
     - 从Machine0查询该文件所有corpusid的citations/references
     - 分批查询title（每批100000个）
     - 构造JSONL记录（仅5个字段）
     - 跳过没有citations和references的corpusid
     - 写入新文件到E:/copy_final目录
  3. 显示统计结果

输出文件命名规则:
  输入: 6996f911.jsonl
  输出: E:/copy_final/6996f911_part2.jsonl

JSONL记录结构（仅5个字段）:
  {
    "corpusid": 12345,
    "citations": [{"corpusid": 100, "title": "..."}],
    "detailsOfCitations": {"offset": 0, "data": [...]},
    "references": [{"corpusid": 200, "title": "..."}],
    "detailsOfReference": {"offset": 0, "data": [...]}
  }

注意事项:
  - 每个文件包含最多50000个corpusid（已在corpusid_to_file表中分组）
  - 跳过没有citations和references的corpusid
  - 不修改原文件，直接生成新文件
  - 不需要Machine2的temp_import表
        """
    )
    
    parser.add_argument("--primary", default="machine2", 
                        help="主机器（提供corpusid_to_file表，默认: machine2）")
    parser.add_argument("--auxiliary", default="machine0", 
                        help="辅助机器（提供citations/references查询，默认: machine0）")
    
    args = parser.parse_args()
    
    print(f"启动JSONL批量更新器V3")
    print(f"  主机器: {args.primary}")
    print(f"  辅助机器: {args.auxiliary}")
    
    updater = JSONLBatchUpdaterV3(
        primary_machine=args.primary,
        auxiliary_machine=args.auxiliary
    )
    updater.run()

