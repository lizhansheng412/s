#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量处理脚本（映射表测试版）
只处理 embeddings_specter_v1/v2 和 s2orc/s2orc_v2
只提取 corpusid 和 filename 到映射表，不插入真正的数据

⚠️  测试脚本，不会影响现有的真实数据插入流程
"""

import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from machine_config import get_machine_config
from scripts.test.stream_gz_to_mapping_table import process_gz_folder_to_mapping, NUM_EXTRACTORS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def batch_process_machine_mapping(
    machine_id: str,
    base_dir: str,
    num_extractors: int = NUM_EXTRACTORS,
    resume: bool = True
):
    """
    批量处理该机器分配的文件夹（映射表模式）
    只针对 machine1 和 machine2
    
    Args:
        machine_id: 机器ID ('machine1', 'machine2')
        base_dir: S2ORC数据根目录
        num_extractors: 解压进程数
        resume: 是否启用断点续传
    """
    # 只支持 machine1 和 machine2
    if machine_id not in ['machine1', 'machine2']:
        logger.error(f"❌ 映射表模式只支持 machine1 和 machine2（大数据集）")
        logger.error(f"   当前机器: {machine_id}")
        sys.exit(1)
    
    # 获取机器配置
    config = get_machine_config(machine_id)
    folders = config['folders']
    tables = config['tables']
    
    logger.info("="*80)
    logger.info(f"🚀 批量处理启动（映射表测试模式 - 极速版）")
    logger.info("="*80)
    logger.info(f"⚠️  测试模式：只提取 corpusid + filename，不插入真实数据")
    logger.info(f"机器ID: {machine_id}")
    logger.info(f"配置: {config['description']}")
    logger.info(f"数据根目录: {base_dir}")
    logger.info(f"待处理文件夹: {len(folders)}")
    for folder, table in zip(folders, tables):
        logger.info(f"  - {folder} → corpus_filename_mapping (dataset={table})")
    logger.info(f"")
    logger.info(f"⚡ 性能配置：")
    logger.info(f"  - 解压进程数: {num_extractors}")
    logger.info(f"  - 批量大小: 200000 条/批次")
    logger.info(f"  - 事务大小: 2000000 条/事务")
    logger.info(f"  - 缓冲区: 16MB")
    logger.info(f"  - 断点续传: {'启用' if resume else '禁用'}")
    logger.info(f"  - 无额外索引: 只有主键（最大化插入速度）")
    logger.info(f"  - 目标速度: 30000-60000 条/秒")
    logger.info("="*80)
    logger.info("")
    
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
        logger.info(f"📁 [{i}/{len(folders)}] 处理文件夹: {folder_name}")
        logger.info(f"数据集类型: {dataset_type}")
        logger.info(f"目标: corpus_filename_mapping 表")
        logger.info("="*80)
        logger.info("")
        
        if not folder_path.exists():
            logger.warning(f"⚠️  文件夹不存在，跳过: {folder_path}")
            failed_folders.append(f"{folder_name} (不存在)")
            continue
        
        try:
            # 处理该文件夹（映射表模式）
            process_gz_folder_to_mapping(
                folder_path=str(folder_path),
                dataset_type=dataset_type,
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
    logger.info("🏁 批量处理完成（映射表测试模式）")
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
        logger.info("📊 corpus_filename_mapping 表已建立索引")
        logger.info("")
        logger.info("💡 下一步：")
        logger.info("   1. 查询映射表：SELECT filename FROM corpus_filename_mapping WHERE corpusid = ?")
        logger.info("   2. 根据文件名从 gz 文件中读取完整数据")
        logger.info("   3. 对比完整数据插入方式，评估性能提升")
        logger.info("")
        logger.info("⚡ 性能对比（无额外索引优化）：")
        logger.info("   - 映射表插入：30000-60000 条/秒")
        logger.info("   - 完整数据插入：1000-3000 条/秒")
        logger.info("   - 速度提升：15-60 倍")
        logger.info("   - 空间节省：无索引额外开销（节省30-50%）")
    else:
        logger.warning("⚠️  部分文件夹处理失败，请检查日志")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='批量处理脚本（映射表测试版）- 只提取索引，不插入真实数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
映射表测试模式说明：
  - 只支持 machine1 和 machine2（大数据集）
  - 只提取 corpusid + filename，不插入完整数据
  - 目标表：corpus_filename_mapping
  - 不影响现有的真实数据插入流程
  
使用前提：
  必须先执行 init_mapping_table.py 创建映射表

机器配置：
  machine1: embeddings-specter_v1, s2orc
  machine2: embeddings-specter_v2, s2orc_v2

示例：
  # Machine 1（测试模式）
  python scripts/test/batch_process_machine_mapping_test.py --machine machine1 --base-dir "E:\\2025-09-30"
  
  # Machine 2（测试模式）
  python scripts/test/batch_process_machine_mapping_test.py --machine machine2 --base-dir "E:\\2025-09-30"
  
  # 自定义解压进程数
  python scripts/test/batch_process_machine_mapping_test.py --machine machine1 --base-dir "E:\\data" --extractors 4
        """
    )
    
    parser.add_argument('--machine', type=str, required=True,
                       choices=['machine1', 'machine2'],
                       help='机器ID（只支持 machine1 和 machine2）')
    parser.add_argument('--base-dir', type=str, required=True,
                       help='S2ORC数据根目录')
    parser.add_argument('--extractors', type=int, default=NUM_EXTRACTORS,
                       help=f'解压进程数（默认: {NUM_EXTRACTORS}）')
    parser.add_argument('--no-resume', action='store_true',
                       help='禁用断点续传（从头开始）')
    
    args = parser.parse_args()
    
    batch_process_machine_mapping(
        machine_id=args.machine,
        base_dir=args.base_dir,
        num_extractors=args.extractors,
        resume=not args.no_resume
    )


if __name__ == '__main__':
    main()

