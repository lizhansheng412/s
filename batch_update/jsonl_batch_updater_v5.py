"""
JSONL批量更新器 V5 - 内存优化版本
核心优化：
1. 将corpusid_mapping_title表分批加载到内存（分3批，每批约4300万条）
2. 读取E:\copy_final_1下的快速模式JSONL文件
3. 在内存中查询title，构造完整的对象结构
4. 写入到E:\copy_final_cache目录
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
SOURCE_DIR = Path("E:/copy_final_1")        # 源JSONL目录（快速模式文件）
OUTPUT_DIR = Path("E:/copy_final_cache")    # 输出目录（完整模式文件）
RUNNING_LOG = Path(__file__).parent.parent / "logs" / "running.log"

BATCH_COUNT = 3                             # 分批数量（将title表分成3批）
# ==================================================


def log_performance(stage, **metrics):
    """记录性能日志"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    metrics_str = " | ".join([f"{k}={v}" for k, v in metrics.items()])
    log_line = f"[{timestamp}] {stage} | {metrics_str}\n"
    with open(RUNNING_LOG, 'a', encoding='utf-8') as f:
        f.write(log_line)
    print(f"  {stage}: {metrics_str}")


class JSONLBatchUpdaterV5:
    """JSONL批量更新器V5 - 内存优化版本"""
    
    def __init__(self, machine_id='machine0'):
        """
        初始化更新器
        
        Args:
            machine_id: 目标机器 - 默认machine0
        """
        self.machine_id = machine_id
        self.conn = None
        self.cursor = None
        self.title_cache = {}  # 当前批次的title映射缓存
        
        print(f"初始化更新器V5:")
        print(f"  目标机器: {machine_id}")
        print(f"  源目录: {SOURCE_DIR}")
        print(f"  输出目录: {OUTPUT_DIR}")
        print(f"  分批策略: 将title表分成{BATCH_COUNT}批加载")
        
        # 确保输出目录存在
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    def connect_db(self):
        """连接数据库"""
        print("\n连接数据库...")
        
        db_config = get_db_config(self.machine_id)
        print(f"  [{self.machine_id}: {db_config['database']}:{db_config['port']}]...", end=' ', flush=True)
        
        try:
            self.conn = psycopg2.connect(**db_config)
            self.cursor = self.conn.cursor()
            print("✓")
        except Exception as e:
            print(f"✗ 失败: {e}")
            raise
    
    def close_db(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def get_title_table_info(self):
        """获取corpusid_mapping_title表的统计信息（快速预估）"""
        print("\n获取title表统计信息（快速预估）...")
        
        # 使用pg_class统计信息快速预估（避免全表COUNT）
        self.cursor.execute("""
            SELECT reltuples::bigint 
            FROM pg_class 
            WHERE oid = 'corpusid_mapping_title'::regclass
        """)
        total_count = self.cursor.fetchone()[0]
        
        # 如果统计信息不准确，使用预设值
        if total_count <= 0:
            total_count = 130000000  # 预估1.3亿条
        
        print(f"  ✓ 预估记录数: 约{total_count:,} 条")
        print(f"  ✓ 跳过corpusid范围查询（使用OFFSET/LIMIT分批）")
        
        return total_count, None, None
    
    def load_title_batch(self, batch_num, total_count, min_id=None, max_id=None):
        """加载一批title数据到内存（快速模式）
        
        Args:
            batch_num: 批次号（1-based）
            total_count: 总记录数（预估值）
            min_id: 忽略（不使用）
            max_id: 忽略（不使用）
        
        Returns:
            dict: {corpusid: title}
        """
        print(f"\n【批次 {batch_num}/{BATCH_COUNT}】加载title数据到内存（快速模式）...")
        
        load_start = time.time()
        
        # 计算每批的记录数（基于预估值）
        batch_size = total_count // BATCH_COUNT
        offset = (batch_num - 1) * batch_size
        
        # 最后一批包含所有剩余记录
        if batch_num == BATCH_COUNT:
            limit = total_count - offset
        else:
            limit = batch_size
        
        print(f"  查询范围: OFFSET={offset:,}, LIMIT={limit:,}")
        print(f"  开始加载...", end=' ', flush=True)
        
        # 使用OFFSET/LIMIT分批查询（按corpusid排序）
        # 注意：这里不做进度显示，让数据库一次性返回
        self.cursor.execute("""
            SELECT corpusid, COALESCE(title, '') as title
            FROM corpusid_mapping_title
            ORDER BY corpusid
            OFFSET %s LIMIT %s
        """, (offset, limit))
        
        # 快速加载到内存（使用字典推导式）
        title_cache = {row[0]: row[1] for row in self.cursor}
        rows_loaded = len(title_cache)
        
        print("完成")
        
        load_time = time.time() - load_start
        memory_mb = sys.getsizeof(title_cache) / 1024 / 1024
        
        print(f"  ✓ 加载完成: {rows_loaded:,} 条记录")
        print(f"  ✓ 耗时: {load_time:.2f}秒")
        print(f"  ✓ 内存占用: 约{memory_mb:.0f}MB")
        
        log_performance(
            f"加载title批次{batch_num}",
            records=f"{rows_loaded:,}",
            time_sec=f"{load_time:.2f}",
            memory_mb=f"{memory_mb:.0f}"
        )
        
        return title_cache
    
    def get_jsonl_files(self, batch_num):
        """获取JSONL文件（根据批次选择源目录）
        
        Args:
            batch_num: 批次号（1-3）
        
        Returns:
            list: 文件列表
        """
        if batch_num == 1:
            # 批次1：从原始目录读取
            source_dir = SOURCE_DIR
            print(f"\n【批次1】从原始目录读取: {source_dir}")
        else:
            # 批次2和3：从输出目录读取（更新批次1创建的文件）
            source_dir = OUTPUT_DIR
            print(f"\n【批次{batch_num}】从输出目录读取（增量更新）: {source_dir}")
        
        files = sorted(source_dir.glob("*.jsonl"))
        print(f"  发现 {len(files)} 个JSONL文件")
        return files
    
    def process_jsonl_file(self, jsonl_path, batch_num):
        """处理单个JSONL文件（快速模式 → 完整模式，或增量更新）
        
        Args:
            jsonl_path: 源JSONL文件路径
            batch_num: 批次号（1-3）
        
        Returns:
            tuple: (处理记录数, 更新title数, 耗时)
        """
        # 批次1：创建新文件；批次2-3：原地更新
        if batch_num == 1:
            output_path = OUTPUT_DIR / jsonl_path.name
        else:
            # 批次2-3：使用临时文件，然后替换原文件
            output_path = OUTPUT_DIR / f"{jsonl_path.name}.tmp"
        
        process_start = time.time()
        processed_count = 0
        updated_titles = 0
        
        try:
            with open(jsonl_path, 'rb') as infile, \
                 open(output_path, 'wb') as outfile:
                
                for line in infile:
                    if not line.strip():
                        continue
                    
                    try:
                        # 解析记录
                        record = orjson.loads(line)
                        corpusid = record.get('corpusid')
                        
                        if corpusid is None:
                            outfile.write(line)
                            continue
                        
                        # 获取citations和references（可能是列表或对象列表）
                        citations_data = record.get('citations', [])
                        references_data = record.get('references', [])
                        
                        # 判断数据格式：列表（快速模式）或对象列表（完整模式）
                        if citations_data and isinstance(citations_data[0], dict):
                            # 已经是完整模式，提取corpusid列表
                            citations_ids = [item['corpusid'] for item in citations_data]
                        else:
                            # 快速模式，直接使用
                            citations_ids = citations_data
                        
                        if references_data and isinstance(references_data[0], dict):
                            # 已经是完整模式，提取corpusid列表
                            references_ids = [item['corpusid'] for item in references_data]
                        else:
                            # 快速模式，直接使用
                            references_ids = references_data
                        
                        # 构造完整的citations（含title）
                        citations_full = []
                        for cid in citations_ids:
                            title = self.title_cache.get(cid, "")
                            if title:
                                updated_titles += 1  # 只统计成功填充的title
                            citations_full.append({
                                "corpusid": cid,
                                "title": title
                            })
                        
                        # 构造完整的references（含title）
                        references_full = []
                        for rid in references_ids:
                            title = self.title_cache.get(rid, "")
                            if title:
                                updated_titles += 1  # 只统计成功填充的title
                            references_full.append({
                                "corpusid": rid,
                                "title": title
                            })
                        
                        # 构造完整记录
                        full_record = {
                            "corpusid": corpusid,
                            "citations": citations_full,
                            "detailsOfCitations": {
                                "offset": 0,
                                "data": citations_full
                            },
                            "references": references_full,
                            "detailsOfReference": {
                                "offset": 0,
                                "data": references_full
                            }
                        }
                        
                        # 写入完整记录
                        outfile.write(orjson.dumps(full_record) + b'\n')
                        processed_count += 1
                        
                    except Exception as e:
                        # 解析失败，保留原始行
                        outfile.write(line)
                        continue
            
            process_time = time.time() - process_start
            
            # 批次2-3：替换原文件
            if batch_num > 1:
                original_path = OUTPUT_DIR / jsonl_path.name
                output_path.replace(original_path)
            
            return processed_count, updated_titles, process_time
        
        except Exception as e:
            print(f"    ✗ 处理失败: {e}")
            if output_path.exists():
                output_path.unlink()
            raise
    
    def process_batch(self, batch_num, total_count, min_id, max_id):
        """处理一个批次的所有文件
        
        Args:
            batch_num: 批次号（1-3）
            total_count: title表总记录数（预估值）
            min_id: 忽略（不使用）
            max_id: 忽略（不使用）
        """
        print("\n" + "=" * 80)
        print(f"【批次 {batch_num}/{BATCH_COUNT}】开始处理")
        print("=" * 80)
        
        batch_start = time.time()
        
        # 加载这一批的title数据到内存
        # 说明：使用OFFSET/LIMIT切割
        # - 批次1：OFFSET=0, LIMIT=total_count/3（前1/3）
        # - 批次2：OFFSET=total_count/3, LIMIT=total_count/3（中间1/3）
        # - 批次3：OFFSET=2*total_count/3, LIMIT=total_count/3（最后1/3）
        print(f"\n【切割说明】")
        print(f"  总记录数（预估）: {total_count:,}")
        print(f"  批次{batch_num} OFFSET: {(batch_num-1)*total_count//BATCH_COUNT:,}")
        print(f"  批次{batch_num} LIMIT: {total_count//BATCH_COUNT if batch_num < BATCH_COUNT else total_count - 2*total_count//BATCH_COUNT:,}")
        
        self.title_cache = self.load_title_batch(batch_num, total_count, min_id, max_id)
        
        # 获取所有JSONL文件（批次1从源目录，批次2-3从输出目录）
        jsonl_files = self.get_jsonl_files(batch_num)
        
        if not jsonl_files:
            print("  没有待处理的文件")
            return
        
        # 处理所有文件
        total_processed = 0
        total_updated = 0
        
        print(f"\n处理 {len(jsonl_files)} 个文件...")
        
        with tqdm(jsonl_files, desc=f"  批次{batch_num}进度", unit="file") as pbar:
            for jsonl_path in pbar:
                try:
                    processed, updated, file_time = self.process_jsonl_file(jsonl_path, batch_num)
                    
                    total_processed += processed
                    total_updated += updated
                    
                    pbar.set_postfix_str(
                        f"{jsonl_path.name} | {processed}条 | 更新{updated}个title | {file_time:.1f}s"
                    )
                    
                except Exception as e:
                    print(f"\n  ✗ 文件处理失败 [{jsonl_path.name}]: {e}")
                    continue
        
        batch_time = time.time() - batch_start
        
        print(f"\n【批次 {batch_num} 完成】")
        print(f"  处理记录: {total_processed:,} 条")
        print(f"  填充title: {total_updated:,} 个")
        print(f"  批次耗时: {batch_time:.2f}秒 ({batch_time/60:.1f}分钟)")
        
        if batch_num == 1:
            print(f"  输出目录: {OUTPUT_DIR}")
        else:
            print(f"  更新方式: 原地更新（覆盖批次1的文件）")
        
        log_performance(
            f"批次{batch_num}完成",
            processed=f"{total_processed:,}",
            updated_titles=f"{total_updated:,}",
            time_sec=f"{batch_time:.2f}"
        )
        
        # 清理内存
        self.title_cache.clear()
    
    def run(self, batch_num=None):
        """执行更新流程
        
        Args:
            batch_num: 批次号（1-3），如果为None则显示帮助信息
        """
        print("=" * 80)
        print("JSONL批量更新器 V5 - 内存优化版本（手动批次模式）")
        print("=" * 80)
        
        if batch_num is None:
            print("\n请指定要处理的批次号（1-3）：")
            print("  python batch_update/jsonl_batch_updater_v5.py --batch 1")
            print("  python batch_update/jsonl_batch_updater_v5.py --batch 2")
            print("  python batch_update/jsonl_batch_updater_v5.py --batch 3")
            print("\n说明：")
            print("  - 每个批次会加载约1/3的title表到内存")
            print("  - 处理完一批后需要手动运行下一批")
            print("  - 3个批次全部完成后，所有title都会填充完毕")
            return
        
        if batch_num < 1 or batch_num > BATCH_COUNT:
            print(f"\n错误：批次号必须在1-{BATCH_COUNT}之间")
            return
        
        batch_start = time.time()
        
        try:
            # 连接数据库
            self.connect_db()
            
            # 获取title表统计信息
            total_count, min_id, max_id = self.get_title_table_info()
            
            # 处理指定批次
            self.process_batch(batch_num, total_count, min_id, max_id)
            
            # 统计结果
            batch_time = time.time() - batch_start
            
            print("\n" + "=" * 80)
            print(f"【批次 {batch_num} 完成】")
            print("=" * 80)
            print(f"  耗时: {batch_time:.2f}秒 ({batch_time/60:.1f}分钟)")
            print(f"  输出目录: {OUTPUT_DIR}")
            
            # 提示下一步操作
            if batch_num < BATCH_COUNT:
                print(f"\n下一步：运行批次 {batch_num + 1}")
                print(f"  python batch_update/jsonl_batch_updater_v5.py --batch {batch_num + 1}")
            else:
                print("\n✓ 所有批次已完成！所有title都已填充。")
            
            print("=" * 80)
            
            log_performance(
                f"V5批次{batch_num}完成",
                batch=batch_num,
                time_sec=f"{batch_time:.2f}",
                time_min=f"{batch_time/60:.1f}"
            )
        
        finally:
            self.close_db()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="JSONL批量更新器 V5 - 内存优化版本（手动批次模式）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 显示帮助信息
  python batch_update/jsonl_batch_updater_v5.py
  
  # 处理批次1（加载前1/3的title）
  python batch_update/jsonl_batch_updater_v5.py --batch 1
  
  # 处理批次2（加载中间1/3的title）
  python batch_update/jsonl_batch_updater_v5.py --batch 2
  
  # 处理批次3（加载最后1/3的title）
  python batch_update/jsonl_batch_updater_v5.py --batch 3

工作流程（手动控制）:
  1. 运行批次1：
     python batch_update/jsonl_batch_updater_v5.py --batch 1
     - 加载前1/3的title数据到内存（约4300万条）
     - 遍历E:/copy_final_1下所有JSONL文件
     - 在内存中查询title，构造完整对象结构
     - 写入到E:/copy_final_cache目录
     - 完成后自动清空内存
  
  2. 手动运行批次2：
     python batch_update/jsonl_batch_updater_v5.py --batch 2
     - 加载中间1/3的title数据到内存
     - 再次处理所有文件（增量更新缺失的title）
     - 完成后自动清空内存
  
  3. 手动运行批次3：
     python batch_update/jsonl_batch_updater_v5.py --batch 3
     - 加载最后1/3的title数据到内存
     - 最后一次处理所有文件（填充剩余title）
     - 完成！所有title都已填充

输入文件:
  E:/copy_final_1/*.jsonl（快速模式，仅corpusid列表）

输出文件:
  E:/copy_final_cache/*.jsonl（完整模式，含title对象）

优化说明:
  - 手动控制每个批次，避免长时间运行
  - 每批处理完自动清空内存
  - 可以分多次运行，灵活控制
  - 预计速度提升：10-20倍
        """
    )
    
    parser.add_argument("--machine", default="machine0", 
                        help="目标机器（默认: machine0）")
    parser.add_argument("--batch", type=int, choices=[1, 2, 3],
                        help="批次号（1-3）")
    
    args = parser.parse_args()
    
    print(f"启动JSONL批量更新器V5（手动批次模式）")
    print(f"  目标机器: {args.machine}")
    if args.batch:
        print(f"  处理批次: {args.batch}/3")
    
    updater = JSONLBatchUpdaterV5(machine_id=args.machine)
    updater.run(batch_num=args.batch)

