#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化 SQLite 数据库表，用于记录 gz 文件处理状态

支持不同机器的不同数据集类型：
- machine0: s2orc, s2orc_v2
- machine2: papers, abstracts, tldrs, citations, authors, publication_venues
- machine3: embeddings_specter_v1, embeddings_specter_v2
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from enum import Enum


# 不同机器的数据集类型配置
MACHINE_DATASETS = {
    'machine0': ['s2orc', 's2orc_v2'],
    'machine2': ['papers', 'abstracts', 'tldrs', 'citations', 'authors', 'publication_venues'],
    'machine3': ['embeddings_specter_v1', 'embeddings_specter_v2']
}


class DatasetType(Enum):
    """数据集类型枚举（所有可能的类型）"""
    # machine2 数据集
    PAPERS = "papers"
    ABSTRACTS = "abstracts"
    TLDRS = "tldrs"
    CITATIONS = "citations"
    AUTHORS = "authors"
    PUBLICATION_VENUES = "publication_venues"
    
    # machine0 数据集
    S2ORC = "s2orc"
    S2ORC_V2 = "s2orc_v2"
    
    # machine3 数据集
    EMBEDDINGS_SPECTER_V1 = "embeddings_specter_v1"
    EMBEDDINGS_SPECTER_V2 = "embeddings_specter_v2"


class ProcessRecorder:
    """处理记录器"""
    
    def __init__(self, machine: str = 'machine2', db_path: str = None):
        """初始化数据库连接
        
        Args:
            machine: 机器ID ('machine0', 'machine2', 'machine3')
            db_path: SQLite 数据库文件路径（可选，默认根据机器自动选择）
        """
        if machine not in MACHINE_DATASETS:
            raise ValueError(f"Invalid machine: {machine}. Valid: {list(MACHINE_DATASETS.keys())}")
        
        self.machine = machine
        self.allowed_datasets = MACHINE_DATASETS[machine]
        
        # 根据机器选择数据库存储位置
        if db_path:
            self.db_path = db_path
        else:
            if machine == 'machine2':
                db_dir = Path(r'D:\sqlite')
            else:  # machine0 or machine3
                db_dir = Path(r'E:\sqlite')
            
            db_dir.mkdir(parents=True, exist_ok=True)
            self.db_path = str(db_dir / f"{machine}_process_record.db")
        
        self.conn = None
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表结构"""
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # 构建数据集类型检查约束
        dataset_check = "', '".join(self.allowed_datasets)
        
        # 创建表
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                insert_time TIMESTAMP NOT NULL,
                CHECK(dataset_type IN ('{dataset_check}')),
                UNIQUE(filename, dataset_type)
            )
        """)
        
        # 创建索引以提高查询效率
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_dataset_type 
            ON processed_files(dataset_type)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_insert_time 
            ON processed_files(insert_time)
        """)
        
        self.conn.commit()
    
    def add_record(self, filename: str, dataset_type: DatasetType) -> bool:
        """添加处理记录
        
        Args:
            filename: gz 文件名
            dataset_type: 数据集类型
            
        Returns:
            bool: 添加成功返回 True，已存在返回 False
        """
        # 检查数据集类型是否允许
        if dataset_type.value not in self.allowed_datasets:
            raise ValueError(f"Dataset type '{dataset_type.value}' not allowed for {self.machine}. "
                           f"Allowed: {self.allowed_datasets}")
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO processed_files (filename, dataset_type, insert_time)
                VALUES (?, ?, ?)
            """, (filename, dataset_type.value, datetime.now()))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
    
    def is_processed(self, filename: str, dataset_type: DatasetType) -> bool:
        """检查文件是否已处理
        
        Args:
            filename: gz 文件名
            dataset_type: 数据集类型
            
        Returns:
            bool: 已处理返回 True，否则返回 False
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM processed_files WHERE filename = ? AND dataset_type = ?
        """, (filename, dataset_type.value))
        return cursor.fetchone()[0] > 0
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


def main():
    """主函数：初始化数据库"""
    import argparse
    
    parser = argparse.ArgumentParser(description="初始化处理记录数据库")
    parser.add_argument('--machine', default='machine2', 
                       choices=['machine0', 'machine2', 'machine3'],
                       help='机器ID (默认: machine2)')
    args = parser.parse_args()
    
    print(f"初始化 {args.machine} 处理记录数据库...")
    recorder = ProcessRecorder(machine=args.machine)
    print(f"✓ 数据库已创建: {recorder.db_path}")
    recorder.close()


if __name__ == "__main__":
    main()

