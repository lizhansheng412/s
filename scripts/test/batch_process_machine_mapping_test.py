#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量处理脚本（映射表模式）
只处理 machine1/machine2 的大数据集
"""

import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from machine_config import get_machine_config
from scripts.test.stream_gz_to_mapping_table import process_gz_folder_to_mapping, NUM_EXTRACTORS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def batch_process_machine_mapping(machine_id: str, base_dir: str, 
                                  num_extractors: int = NUM_EXTRACTORS, 
                                  resume: bool = True):
    """批量处理硬盘上的文件夹（写入本机 Machine1 数据库）"""
    if machine_id not in ['machine1', 'machine2']:
        logger.error(f"❌ 只支持 machine1/machine2，当前: {machine_id}")
        sys.exit(1)
    
    config = get_machine_config(machine_id)
    folders = config['folders']
    tables = config['tables']
    
    logger.info("="*80)
    logger.info(f"🚀 批量处理启动（映射表模式）")
    logger.info(f"机器: {machine_id} | 配置: {config['description']}")
    logger.info(f"数据根目录: {base_dir}")
    logger.info(f"待处理: {len(folders)} 个文件夹")
    for folder, table in zip(folders, tables):
        logger.info(f"  - {folder} → {table}")
    logger.info("="*80)
    
    base_path = Path(base_dir)
    if not base_path.exists():
        logger.error(f"❌ 数据根目录不存在: {base_dir}")
        sys.exit(1)
    
    overall_start = time.time()
    success_count = 0
    failed_folders = []
    
    for i, (folder_name, dataset_type) in enumerate(zip(folders, tables), 1):
        folder_path = base_path / folder_name
        
        logger.info("")
        logger.info("="*80)
        logger.info(f"📁 [{i}/{len(folders)}] 处理: {folder_name} → {dataset_type}")
        logger.info("="*80)
        
        if not folder_path.exists():
            logger.warning(f"⚠️  文件夹不存在: {folder_path}")
            failed_folders.append(f"{folder_name} (不存在)")
            continue
        
        try:
            process_gz_folder_to_mapping(
                folder_path=str(folder_path),
                dataset_type=dataset_type,
                num_extractors=num_extractors,
                resume=resume,
                reset_progress=False
            )
            
            success_count += 1
            logger.info(f"✅ [{i}/{len(folders)}] {folder_name} 完成\n")
            
        except KeyboardInterrupt:
            logger.warning(f"\n⚠️  用户中断 | 已完成: {success_count}/{len(folders)}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ [{i}/{len(folders)}] {folder_name} 失败: {e}")
            failed_folders.append(f"{folder_name} ({str(e)})")
            continue
    
    elapsed = time.time() - overall_start
    
    logger.info("")
    logger.info("="*80)
    logger.info("🏁 批量处理完成")
    logger.info("="*80)
    logger.info(f"机器: {machine_id}")
    logger.info(f"总数: {len(folders)} | 成功: {success_count} | 失败: {len(failed_folders)}")
    logger.info(f"总耗时: {elapsed/3600:.2f} 小时")
    
    if failed_folders:
        logger.warning("")
        logger.warning("⚠️  失败列表:")
        for folder in failed_folders:
            logger.warning(f"  - {folder}")
    
    logger.info("="*80)
    
    if success_count == len(folders):
        logger.info("✅ 全部成功！")
        logger.info("💡 下一步: python scripts/test/init_mapping_table.py --add-pk")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='批量处理脚本（写入本机 Machine1 数据库）')
    parser.add_argument('--machine', type=str, required=True, 
                       choices=['machine1', 'machine2'], 
                       help='硬盘配置ID（machine1: embeddings_v1+s2orc, machine2: embeddings_v2+s2orc_v2）')
    parser.add_argument('--base-dir', type=str, required=True, help='硬盘数据根目录')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS, 
                       help=f'解压进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--no-resume', action='store_true', help='禁用断点续传')
    
    args = parser.parse_args()
    
    batch_process_machine_mapping(
        machine_id=args.machine,
        base_dir=args.base_dir,
        num_extractors=args.extractors,
        resume=not args.no_resume
    )


if __name__ == '__main__':
    main()
