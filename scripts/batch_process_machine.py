#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量处理脚本 - 自动处理该机器分配的所有文件夹
支持4台机器的分布式并行处理
"""

import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database.config import db_config_v2
from database.config.db_config_v2 import get_db_config
from machine_config import get_machine_config
from scripts.stream_gz_to_db_optimized import process_gz_folder_pipeline, NUM_EXTRACTORS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_folder_flexible(base_path: Path, folder_name: str) -> Path:
    """
    灵活查找文件夹，自动适配连字符(-)和下划线(_)命名
    
    Args:
        base_path: 基础路径
        folder_name: 文件夹名（可能包含连字符或下划线）
    
    Returns:
        找到的文件夹路径，如果都不存在则返回原始路径
    """
    original_path = base_path / folder_name
    if original_path.exists():
        return original_path
    
    alternative_name = folder_name.replace('-', '_') if '-' in folder_name else folder_name.replace('_', '-')
    alternative_path = base_path / alternative_name
    if alternative_path.exists():
        logger.info(f"  → 自动适配文件夹名: {folder_name} → {alternative_name}")
        return alternative_path
    
    return original_path


def batch_process_machine(
    machine_id: str,
    base_dir: str,
    num_extractors: int = NUM_EXTRACTORS,
    resume: bool = True,
    use_upsert: bool = False,
    is_retry: bool = False
):
    """
    批量处理该机器分配的所有文件夹
    
    Args:
        machine_id: 机器ID ('machine1', 'machine2', 'machine3', 'machine0')
        base_dir: S2ORC数据根目录（包含所有子文件夹）
        num_extractors: 解压进程数
        resume: 是否启用断点续传
    """
    # 获取机器配置
    config = get_machine_config(machine_id)
    folders = config['folders']
    tables = config['tables']
    
    logger.info("="*80)
    logger.info(f"BATCH PROCESSING START")
    logger.info("="*80)
    logger.info(f"Machine ID: {machine_id}")
    logger.info(f"Config: {config['description']}")
    logger.info(f"Base dir: {base_dir}")
    logger.info(f"Folders to process: {len(folders)}")
    for folder, table in zip(folders, tables):
        logger.info(f"  - {folder} -> {table}")
    logger.info(f"Extractors: {num_extractors}")
    logger.info(f"Resume: {'enabled' if resume else 'disabled'}")
    logger.info("="*80)
    logger.info("")
    
    base_path = Path(base_dir)
    if not base_path.exists():
        logger.error(f"ERROR: Base directory not found: {base_dir}")
        sys.exit(1)
    
    overall_start = time.time()
    total_processed = 0
    success_count = 0
    failed_folders = []
    
    for i, (folder_name, table_name) in enumerate(zip(folders, tables), 1):
        folder_path = find_folder_flexible(base_path, folder_name)
        
        logger.info("")
        logger.info("="*80)
        logger.info(f"[{i}/{len(folders)}] Processing folder: {folder_name}")
        logger.info(f"Target table: {table_name}")
        logger.info("="*80)
        logger.info("")
        
        if not folder_path.exists():
            logger.warning(f"WARNING: Folder not found, skipping: {folder_path}")
            failed_folders.append(f"{folder_name} (not found)")
            continue
        
        try:
            # 处理该文件夹（主键字段根据表名自动确定）
            process_gz_folder_pipeline(
                folder_path=str(folder_path),
                table_name=table_name,
                use_upsert=use_upsert,
                num_extractors=num_extractors,
                resume=resume,
                reset_progress=False,
                is_retry=is_retry
            )
            
            success_count += 1
            logger.info(f"[{i}/{len(folders)}] {folder_name} - DONE\n")
            
        except KeyboardInterrupt:
            logger.warning("\nInterrupted by user (progress saved)")
            logger.info(f"Completed: {success_count}/{len(folders)}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"ERROR [{i}/{len(folders)}] {folder_name} failed: {e}")
            failed_folders.append(f"{folder_name} ({str(e)})")
            # 继续处理下一个文件夹
            continue
    
    # 总结
    elapsed = time.time() - overall_start
    
    logger.info("")
    logger.info("="*80)
    logger.info("BATCH PROCESSING COMPLETED")
    logger.info("="*80)
    logger.info(f"Machine ID: {machine_id}")
    logger.info(f"Total folders: {len(folders)}")
    logger.info(f"Success: {success_count}")
    logger.info(f"Failed: {len(failed_folders)}")
    logger.info(f"Total time: {elapsed/3600:.2f} hours")
    
    if failed_folders:
        logger.warning("")
        logger.warning("Failed folders:")
        for folder in failed_folders:
            logger.warning(f"  - {folder}")
    
    logger.info("="*80)
    logger.info("")
    
    if success_count == len(folders):
        logger.info("SUCCESS: All folders processed!")
        
        # 检查是否有失败文件需要重试
        if not is_retry:
            failed_dir = Path("D:\\lzs_download\\faild_file_downlaod")
            
            # 安全检查重试目录，支持多种文件夹命名
            has_retry_files = False
            retry_folders_found = []
            try:
                if failed_dir.exists():
                    for folder_name in folders:
                        retry_folder = find_folder_flexible(failed_dir, folder_name)
                        if retry_folder.exists() and any(retry_folder.glob("*.gz")):
                            has_retry_files = True
                            retry_folders_found.append(retry_folder.name)
            except Exception as e:
                logger.warning(f"检查重试目录时出错: {e}")
            
            if has_retry_files:
                logger.info("")
                logger.info("="*80)
                logger.info("检测到失败文件重下载目录，开始重试...")
                logger.info(f"重试目录: {failed_dir}")
                logger.info(f"找到文件夹: {', '.join(retry_folders_found)}")
                logger.info("="*80)
                
                try:
                    batch_process_machine(
                        machine_id=machine_id,
                        base_dir=str(failed_dir),
                        num_extractors=num_extractors,
                        resume=resume,
                        use_upsert=use_upsert,
                        is_retry=True
                    )
                    logger.info("")
                    logger.info("="*80)
                    logger.info("失败文件重试完成！")
                    logger.info("="*80)
                except Exception as e:
                    logger.error(f"失败文件重试出错: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                logger.info("Next step: Wait for other machines, then merge data")
        else:
            logger.info("重试流程完成！")
    else:
        logger.warning("WARNING: Some folders failed, check logs")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='批量处理该机器分配的所有文件夹',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
机器配置：
  machine1: embeddings-specter_v1, s2orc
  machine2: embeddings-specter_v2, s2orc_v2
  machine3: abstracts, authors, papers, publication-venues, tldrs, citations
  machine0: paper-ids

示例：
  # 正常导入
  python scripts/batch_process_machine.py --machine machine1 --base-dir "E:\\2025-09-30"
  python scripts/batch_process_machine.py --machine machine2 --base-dir "E:\\2025-09-30"
  python scripts/batch_process_machine.py --machine machine3 --base-dir "E:\\2025-09-30"
  python scripts/batch_process_machine.py --machine machine0 --base-dir "E:\\2025-09-30"
  
  # 重试失败文件（使用 --retry 标志）
  python scripts/batch_process_machine.py --machine machine3 --base-dir "D:\\lzs_download\\faild_file_downlaod" --retry
  
  # 自定义解压进程数
  python scripts/batch_process_machine.py --machine machine1 --base-dir "E:\\data" --extractors 12
        """
    )
    
    parser.add_argument('--machine', type=str, required=True,
                       choices=['machine1', 'machine2', 'machine3', 'machine0'],
                       help='机器ID')
    parser.add_argument('--base-dir', type=str, required=True,
                       help='S2ORC数据根目录（包含所有子文件夹）')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS,
                       help=f'解压进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--no-resume', action='store_true',
                       help='禁用断点续传（从头开始）')
    parser.add_argument('--upsert', action='store_true',
                       help='使用UPSERT模式（处理重复数据）')
    parser.add_argument('--retry', action='store_true',
                       help='重试失败文件模式（不排除已失败的文件）')
    
    args = parser.parse_args()
    
    # 根据机器ID更新数据库配置
    db_config = get_db_config(args.machine)
    db_config_v2.DB_CONFIG.update(db_config)
    logger.info(f"Machine: {args.machine}, Database: {db_config_v2.DB_CONFIG['database']}\n")
    
    batch_process_machine(
        machine_id=args.machine,
        base_dir=args.base_dir,
        num_extractors=args.extractors,
        resume=not args.no_resume,
        use_upsert=args.upsert,
        is_retry=args.retry
    )


if __name__ == '__main__':
    main()

