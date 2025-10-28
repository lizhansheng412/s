#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量处理脚本 - corpusid 到 gz 文件名映射
支持 m1/m2/m3 硬盘配置
"""

import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.new_test.disk_config import get_disk_config
from scripts.new_test.stream_gz_to_mapping import (
    process_gz_folder_to_mapping, 
    NUM_EXTRACTORS, 
    NUM_INSERTERS
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def batch_process_disk_mapping(disk_id: str, base_dir: str, 
                               num_extractors: int = NUM_EXTRACTORS,
                               num_inserters: int = NUM_INSERTERS,
                               resume: bool = True):
    """批量处理硬盘数据"""
    if disk_id not in ['m1', 'm2', 'm3']:
        logger.error(f"❌ 只支持 m1/m2/m3 硬盘")
        sys.exit(1)
    
    config = get_disk_config(disk_id)
    folders = config['folders']
    fields = config['fields']
    
    logger.info("="*60)
    logger.info(f"🚀 批量处理启动 | 硬盘: {disk_id}")
    logger.info(f"   {config['description']}")
    logger.info(f"数据根目录: {base_dir}")
    logger.info(f"待处理: {len(folders)} 个文件夹")
    for folder, field in zip(folders, fields):
        logger.info(f"  {folder} → {field}")
    logger.info(f"进程配置: 提取={num_extractors}, 插入={num_inserters}")
    logger.info("="*60)
    
    base_path = Path(base_dir)
    if not base_path.exists():
        logger.error(f"❌ 数据根目录不存在: {base_dir}")
        sys.exit(1)
    
    overall_start = time.time()
    success_count = 0
    failed_folders = []
    
    for i, (folder_name, field_name) in enumerate(zip(folders, fields), 1):
        folder_path = base_path / folder_name
        
        logger.info("")
        logger.info(f"📁 [{i}/{len(folders)}] {folder_name} → {field_name}")
        logger.info("-"*60)
        
        if not folder_path.exists():
            logger.warning(f"⚠️  文件夹不存在: {folder_path}")
            failed_folders.append(f"{folder_name} (不存在)")
            continue
        
        try:
            process_gz_folder_to_mapping(
                folder_path=str(folder_path),
                field_name=field_name,
                num_extractors=num_extractors,
                num_inserters=num_inserters,
                resume=resume,
                reset_progress=False
            )
            
            success_count += 1
            logger.info(f"✅ {folder_name} 完成\n")
            
        except KeyboardInterrupt:
            logger.warning(f"\n⚠️  用户中断 | 已完成: {success_count}/{len(folders)}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ {folder_name} 失败: {e}")
            failed_folders.append(f"{folder_name} ({str(e)})")
            continue
    
    elapsed = time.time() - overall_start
    
    logger.info("")
    logger.info("="*60)
    logger.info("🏁 批量处理完成")
    logger.info(f"成功: {success_count}/{len(folders)} | 耗时: {elapsed/3600:.2f}小时")
    
    if failed_folders:
        logger.warning("⚠️  失败列表:")
        for folder in failed_folders:
            logger.warning(f"  {folder}")
    
    logger.info("="*60)
    
    if success_count == len(folders):
        logger.info("✅ 全部成功！")
        if disk_id == 'm3':
            logger.info("💡 所有硬盘处理完成后运行: python scripts/new_test/init_corpusid_mapping.py --finalize")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='批量处理 corpusid 映射')
    parser.add_argument('--disk', type=str, required=True, 
                       choices=['m1', 'm2', 'm3'], 
                       help='硬盘ID')
    parser.add_argument('--base-dir', type=str, required=True, help='硬盘根目录')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS, 
                       help=f'提取进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--inserters', type=int, default=NUM_INSERTERS, 
                       help=f'插入进程数（默认: {NUM_INSERTERS}）')
    parser.add_argument('--no-resume', action='store_true', help='禁用断点续传')
    
    args = parser.parse_args()
    
    batch_process_disk_mapping(
        disk_id=args.disk,
        base_dir=args.base_dir,
        num_extractors=args.extractors,
        num_inserters=args.inserters,
        resume=not args.no_resume
    )


if __name__ == '__main__':
    main()

