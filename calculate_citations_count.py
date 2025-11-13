#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计算 progress 中所有文件的 cite_ids 总数（优化版）
- progress 表在 SQLite 中
- corpusid_to_file 和 temp_citations 表在 PostgreSQL 中
"""

import sqlite3
import psycopg2
from typing import List, Tuple, Dict
import json
from collections import defaultdict


class CitationCounterOptimized:
    def __init__(self, sqlite_db_path: str, pg_config: dict):
        """
        初始化数据库连接
        
        Args:
            sqlite_db_path: SQLite 数据库文件路径
            pg_config: PostgreSQL 连接配置字典，包含 host, port, database, user, password
        """
        self.sqlite_db_path = sqlite_db_path
        self.pg_config = pg_config
        self.sqlite_conn = None
        self.pg_conn = None
    
    def connect_databases(self):
        """建立数据库连接"""
        # 连接 SQLite
        self.sqlite_conn = sqlite3.connect(self.sqlite_db_path)
        print(f"✓ 已连接到 SQLite: {self.sqlite_db_path}")
        
        # 连接 PostgreSQL
        self.pg_conn = psycopg2.connect(**self.pg_config)
        print(f"✓ 已连接到 PostgreSQL: {self.pg_config['database']}")
    
    def close_databases(self):
        """关闭数据库连接"""
        if self.sqlite_conn:
            self.sqlite_conn.close()
            print("✓ 已关闭 SQLite 连接")
        if self.pg_conn:
            self.pg_conn.close()
            print("✓ 已关闭 PostgreSQL 连接")
    
    def get_all_filenames(self) -> List[str]:
        """从 progress 表获取所有文件名"""
        cursor = self.sqlite_conn.cursor()
        cursor.execute("SELECT filename FROM progress")
        filenames = [row[0] for row in cursor.fetchall()]
        cursor.close()
        print(f"✓ 从 progress 获取到 {len(filenames)} 个文件名")
        return filenames
    
    def clean_filename(self, filename: str) -> str:
        """清理文件名，去除 _part2 等后缀"""
        if '_part' in filename:
            base_name = filename.split('_part')[0]
            return base_name + '.jsonl'
        return filename
    
    def get_filename_to_corpusids_mapping(self, filenames: List[str]) -> Dict[str, List[int]]:
        """
        批量查询所有文件名对应的 corpusid 列表（优化版）
        
        Args:
            filenames: 原始文件名列表
            
        Returns:
            字典：{原始文件名: [corpusid列表]}
        """
        # 清理文件名并建立映射
        clean_to_original = {}
        clean_filenames = []
        for filename in filenames:
            clean = self.clean_filename(filename)
            if clean not in clean_to_original:
                clean_to_original[clean] = []
                clean_filenames.append(clean)
            clean_to_original[clean].append(filename)
        
        print(f"✓ 清理后的唯一文件名数: {len(clean_filenames)}")
        print("✓ 正在批量查询 corpusid_to_file 表...")
        
        # 批量查询所有 corpusid
        cursor = self.pg_conn.cursor()
        cursor.execute(
            """
            SELECT batch_filename, corpusid 
            FROM corpusid_to_file 
            WHERE batch_filename = ANY(%s)
            """,
            (clean_filenames,)
        )
        
        # 建立清理后文件名到 corpusid 的映射
        clean_filename_to_corpusids = defaultdict(list)
        for batch_filename, corpusid in cursor:
            clean_filename_to_corpusids[batch_filename].append(corpusid)
        cursor.close()
        
        print(f"✓ 查询到 {len(clean_filename_to_corpusids)} 个文件的 corpusid 数据")
        
        # 映射回原始文件名
        result = {}
        for clean_name, original_names in clean_to_original.items():
            corpusids = clean_filename_to_corpusids.get(clean_name, [])
            for original_name in original_names:
                result[original_name] = corpusids
        
        return result
    
    def get_all_cite_counts_batch(self, all_corpusids: List[int], batch_size: int = 100000) -> Dict[int, int]:
        """
        分批批量查询所有 corpusid 的 cite_ids 数量（优化版）
        
        Args:
            all_corpusids: 所有需要查询的 corpusid 列表
            batch_size: 每批查询的数量，默认 100000
            
        Returns:
            字典：{corpusid: cite_ids数量}
        """
        if not all_corpusids:
            return {}
        
        total_corpusids = len(all_corpusids)
        print(f"✓ 正在分批查询 {total_corpusids:,} 个 corpusid 的引用数据...")
        print(f"✓ 每批查询 {batch_size:,} 个，共 {(total_corpusids + batch_size - 1) // batch_size} 批")
        
        result = {}
        cursor = self.pg_conn.cursor()
        
        # 分批查询
        for i in range(0, total_corpusids, batch_size):
            batch = all_corpusids[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_corpusids + batch_size - 1) // batch_size
            
            print(f"  - 正在查询第 {batch_num}/{total_batches} 批 ({len(batch):,} 个 corpusid)...", end='')
            
            # 使用 SQL 聚合函数直接在数据库中计算数组长度
            cursor.execute(
                """
                SELECT corpusid, COALESCE(array_length(cite_ids, 1), 0) as cite_count
                FROM temp_citations 
                WHERE corpusid = ANY(%s)
                """,
                (batch,)
            )
            
            batch_result = {corpusid: cite_count for corpusid, cite_count in cursor}
            result.update(batch_result)
            
            print(f" 完成 (找到 {len(batch_result):,} 条记录)")
        
        cursor.close()
        
        print(f"✓ 总共查询到 {len(result):,} 个 corpusid 的引用数据")
        
        return result
    
    def calculate_total_citations_optimized(self) -> Tuple[int, dict]:
        """
        优化版：使用批量查询计算所有文件的所有 corpusid 的所有 cite_ids 总数
        
        Returns:
            (总数, 详细统计字典)
        """
        # 1. 获取所有文件名
        filenames = self.get_all_filenames()
        
        print("\n" + "=" * 60)
        print("开始优化计算...")
        print("=" * 60)
        
        # 2. 批量查询所有文件名对应的 corpusid
        filename_to_corpusids = self.get_filename_to_corpusids_mapping(filenames)
        
        # 3. 收集所有唯一的 corpusid
        all_corpusids = set()
        for corpusids in filename_to_corpusids.values():
            all_corpusids.update(corpusids)
        
        print(f"✓ 总共需要查询 {len(all_corpusids)} 个唯一 corpusid 的引用数据")
        
        # 4. 批量查询所有 corpusid 的引用数量
        corpusid_to_cite_count = self.get_all_cite_counts_batch(list(all_corpusids))
        
        # 5. 统计每个文件的数据
        print("\n" + "=" * 60)
        print("正在汇总统计...")
        print("=" * 60)
        
        total_count = 0
        file_statistics = {}
        
        for idx, filename in enumerate(filenames, 1):
            corpusids = filename_to_corpusids.get(filename, [])
            
            # 计算该文件的引用总数
            file_citation_count = sum(
                corpusid_to_cite_count.get(cid, 0) for cid in corpusids
            )
            
            file_statistics[filename] = {
                'corpusid_count': len(corpusids),
                'citation_count': file_citation_count
            }
            
            total_count += file_citation_count
            
            # 每处理 100 个文件打印一次进度
            if idx % 100 == 0 or idx == len(filenames):
                print(f"[{idx}/{len(filenames)}] 已处理 {idx} 个文件，当前累计引用数: {total_count:,}")
        
        print("\n" + "=" * 60)
        print(f"✓ 计算完成！")
        
        return total_count, file_statistics
    
    def print_summary(self, total_count: int, file_statistics: dict):
        """打印统计摘要"""
        print("\n" + "=" * 60)
        print("统计摘要")
        print("=" * 60)
        
        total_corpusids = sum(stat['corpusid_count'] for stat in file_statistics.values())
        files_with_data = sum(1 for stat in file_statistics.values() if stat['corpusid_count'] > 0)
        files_with_citations = sum(1 for stat in file_statistics.values() if stat['citation_count'] > 0)
        
        print(f"总文件数: {len(file_statistics):,}")
        print(f"有数据的文件数: {files_with_data:,}")
        print(f"有引用的文件数: {files_with_citations:,}")
        print(f"总 corpusid 数: {total_corpusids:,}")
        print(f"总引用数 (cite_ids 总数): {total_count:,}")
        print("=" * 60)
        
        # 打印前 20 个文件的详细信息
        print("\n前 20 个文件的详细统计:")
        print("-" * 60)
        for idx, (filename, stat) in enumerate(list(file_statistics.items())[:20], 1):
            if stat['corpusid_count'] > 0:
                print(f"{idx}. {filename}:")
                print(f"   - corpusid 数量: {stat['corpusid_count']:,}")
                print(f"   - 引用数量: {stat['citation_count']:,}")
        
        # 找出引用数最多的前 10 个文件
        print("\n引用数最多的前 10 个文件:")
        print("-" * 60)
        sorted_files = sorted(
            file_statistics.items(), 
            key=lambda x: x[1]['citation_count'], 
            reverse=True
        )[:10]
        
        for idx, (filename, stat) in enumerate(sorted_files, 1):
            if stat['citation_count'] > 0:
                print(f"{idx}. {filename}:")
                print(f"   - corpusid 数量: {stat['corpusid_count']:,}")
                print(f"   - 引用数量: {stat['citation_count']:,}")
    
    def save_results_to_file(self, total_count: int, file_statistics: dict, output_file: str = "citation_statistics.txt"):
        """保存统计结果到文件"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("Citation Statistics Report\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"总文件数: {len(file_statistics):,}\n")
            f.write(f"总 corpusid 数: {sum(stat['corpusid_count'] for stat in file_statistics.values()):,}\n")
            f.write(f"总引用数 (cite_ids 总数): {total_count:,}\n\n")
            
            f.write("=" * 80 + "\n")
            f.write("各文件详细统计:\n")
            f.write("=" * 80 + "\n\n")
            
            for filename, stat in sorted(file_statistics.items()):
                f.write(f"{filename}:\n")
                f.write(f"  - corpusid 数量: {stat['corpusid_count']:,}\n")
                f.write(f"  - 引用数量: {stat['citation_count']:,}\n\n")
        
        print(f"\n✓ 详细统计结果已保存到: {output_file}")
    
    def run(self):
        """执行完整的计算流程"""
        try:
            self.connect_databases()
            total_count, file_statistics = self.calculate_total_citations_optimized()
            self.print_summary(total_count, file_statistics)
            self.save_results_to_file(total_count, file_statistics)
            return total_count, file_statistics
        except Exception as e:
            print(f"\n❌ 发生错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.close_databases()


def main():
    # 配置数据库连接参数
    SQLITE_DB_PATH = r"D:\projects\S2ORC\s2orc_merge\logs\merge_progress.db"
    
    # 从 db_config.py 导入配置
    try:
        from db_config import get_db_config
        PG_CONFIG = get_db_config('machine0')
    except ImportError:
        # 如果无法导入，使用手动配置
        PG_CONFIG = {
            'host': 'localhost',
            'port': 5430,
            'database': 's2orc_d0',
            'user': 'postgres',
            'password': '333444',
            'client_encoding': 'utf8'
        }
    
    # 创建计数器并运行
    counter = CitationCounterOptimized(SQLITE_DB_PATH, PG_CONFIG)
    counter.run()


if __name__ == "__main__":
    main()
