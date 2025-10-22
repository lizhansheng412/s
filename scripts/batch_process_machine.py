#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量处理脚本 - 自动处理该机器分配的所有文件夹
支持3台机器的分布式并行处理
"""

import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from machine_config import get_machine_config, FOLDER_TO_TABLE_MAP
from scripts.stream_gz_to_db_optimized import process_gz_folder_pipeline, NUM_EXTRACTORS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def batch_process_machine(
    machine_id: str,
    base_dir: str,
    num_extractors: int = NUM_EXTRACTORS,
    resume: bool = True,
    use_upsert: bool = False  # 默认使用INSERT模式（更快）
):
    """
    批量处理该机器分配的所有文件夹
    
    Args:
        machine_id: 机器ID ('machine1', 'machine2', 'machine3')
        base_dir: S2ORC数据根目录（包含所有子文件夹）
        num_extractors: 解压进程数
        resume: 是否启用断点续传
    """
    # 获取机器配置
    config = get_machine_config(machine_id)
    folders = config['folders']
    tables = config['tables']
    
    logger.info("="*80)
    logger.info(f"🚀 批量处理启动")
    logger.info("="*80)
    logger.info(f"机器ID: {machine_id}")
    logger.info(f"配置: {config['description']}")
    logger.info(f"数据根目录: {base_dir}")
    logger.info(f"待处理文件夹: {len(folders)}")
    for folder, table in zip(folders, tables):
        logger.info(f"  - {folder} → {table}")
    logger.info(f"解压进程数: {num_extractors}")
    logger.info(f"断点续传: {'启用' if resume else '禁用'}")
    logger.info("="*80)
    logger.info("")
    
    base_path = Path(base_dir)
    if not base_path.exists():
        logger.error(f"❌ 数据根目录不存在: {base_dir}")
        sys.exit(1)
    
    overall_start = time.time()
    total_processed = 0
    success_count = 0
    failed_folders = []
    
    for i, (folder_name, table_name) in enumerate(zip(folders, tables), 1):
        folder_path = base_path / folder_name
        
        logger.info("")
        logger.info("="*80)
        logger.info(f"📁 [{i}/{len(folders)}] 处理文件夹: {folder_name}")
        logger.info(f"目标表: {table_name}")
        logger.info("="*80)
        logger.info("")
        
        if not folder_path.exists():
            logger.warning(f"⚠️  文件夹不存在，跳过: {folder_path}")
            failed_folders.append(f"{folder_name} (不存在)")
            continue
        
        try:
            # 处理该文件夹（主键字段根据表名自动确定）
            process_gz_folder_pipeline(
                folder_path=str(folder_path),
                table_name=table_name,
                use_upsert=use_upsert,
                num_extractors=num_extractors,
                resume=resume,
                reset_progress=False
            )
            
            success_count += 1
            logger.info(f"✅ [{i}/{len(folders)}] {folder_name} 处理完成\n")
            
        except KeyboardInterrupt:
            logger.warning("\n⚠️  用户中断（进度已保存）")
            logger.info(f"已完成: {success_count}/{len(folders)}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ [{i}/{len(folders)}] {folder_name} 处理失败: {e}")
            failed_folders.append(f"{folder_name} ({str(e)})")
            # 继续处理下一个文件夹
            continue
    
    # 总结
    elapsed = time.time() - overall_start
    
    logger.info("")
    logger.info("="*80)
    logger.info("🏁 批量处理完成")
    logger.info("="*80)
    logger.info(f"机器ID: {machine_id}")
    logger.info(f"总文件夹数: {len(folders)}")
    logger.info(f"成功: {success_count}")
    logger.info(f"失败: {len(failed_folders)}")
    logger.info(f"总耗时: {elapsed/3600:.2f} 小时")
    
    if failed_folders:
        logger.warning("")
        logger.warning("⚠️  以下文件夹处理失败:")
        for folder in failed_folders:
            logger.warning(f"  - {folder}")
    
    logger.info("="*80)
    logger.info("")
    
    if success_count == len(folders):
        logger.info("✅ 所有文件夹处理成功！")
        logger.info("下一步：等待其他机器完成，然后进行数据合并")
    else:
        logger.warning("⚠️  部分文件夹处理失败，请检查日志")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='批量处理该机器分配的所有文件夹',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
机器配置：
  machine1: embeddings-specter_v1, s2orc
  machine2: embeddings-specter_v2, s2orc_v2
  machine3: abstracts, authors, citations, paper_ids, papers, publication_venues, tldrs

示例：
  # 电脑1：处理分配的文件夹
  python scripts/batch_process_machine.py --machine machine1 --base-dir "E:\\S2ORC_Data"
  
  # 电脑2
  python scripts/batch_process_machine.py --machine machine2 --base-dir "F:\\S2ORC_Data"
  
  # 电脑3
  python scripts/batch_process_machine.py --machine machine3 --base-dir "D:\\S2ORC_Data"
  
  # 自定义解压进程数（根据CPU核心数）
  python scripts/batch_process_machine.py --machine machine1 --base-dir "E:\\data" --extractors 12
        """
    )
    
    parser.add_argument('--machine', type=str, required=True,
                       choices=['machine1', 'machine2', 'machine3'],
                       help='机器ID')
    parser.add_argument('--base-dir', type=str, required=True,
                       help='S2ORC数据根目录（包含所有子文件夹）')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS,
                       help=f'解压进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--no-resume', action='store_true',
                       help='禁用断点续传（从头开始）')
    parser.add_argument('--upsert', action='store_true',
                       help='使用UPSERT模式（处理重复数据）')
    
    args = parser.parse_args()
    
    batch_process_machine(
        machine_id=args.machine,
        base_dir=args.base_dir,
        num_extractors=args.extractors,
        resume=not args.no_resume,
        use_upsert=args.upsert
    )


if __name__ == '__main__':
    main()

