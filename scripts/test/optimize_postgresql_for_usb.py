#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL USB 硬盘优化配置脚本
自动修改 postgresql.conf 以优化 USB 硬盘写入性能
"""

import sys
import shutil
from pathlib import Path
from datetime import datetime


def backup_config(config_path: Path):
    """备份配置文件"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = config_path.parent / f"postgresql.conf.backup_{timestamp}"
    shutil.copy2(config_path, backup_path)
    print(f"✅ 配置文件已备份: {backup_path}")
    return backup_path


def optimize_config(config_path: Path, mode: str = 'import'):
    """
    优化 PostgreSQL 配置
    
    Args:
        config_path: postgresql.conf 路径
        mode: 'import' 导入模式（高速） 或 'safe' 安全模式（恢复）
    """
    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        sys.exit(1)
    
    # 备份原始配置
    backup_config(config_path)
    
    # 读取配置
    with open(config_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # 配置映射
    if mode == 'import':
        config_map = {
            'fsync': 'off',
            'synchronous_commit': 'off',
            'full_page_writes': 'off',
            'wal_level': 'minimal',
            'wal_buffers': '128MB',
            'max_wal_size': '10GB',
            'checkpoint_timeout': '1h',
            'checkpoint_completion_target': '0.9',
            'shared_buffers': '8GB',
            'work_mem': '2GB',
            'maintenance_work_mem': '4GB',
            'effective_cache_size': '16GB',
        }
        print("🚀 应用导入模式配置（高速，不安全）...")
    else:
        config_map = {
            'fsync': 'on',
            'synchronous_commit': 'on',
            'full_page_writes': 'on',
            'wal_level': 'replica',
        }
        print("🔒 应用安全模式配置（恢复默认）...")
    
    # 修改配置
    new_lines = []
    updated_keys = set()
    
    for line in lines:
        stripped = line.strip()
        
        # 跳过注释和空行
        if not stripped or stripped.startswith('#'):
            new_lines.append(line)
            continue
        
        # 解析配置项
        if '=' in stripped:
            key = stripped.split('=')[0].strip()
            if key in config_map:
                # 更新配置
                new_lines.append(f"{key} = {config_map[key]}\n")
                updated_keys.add(key)
                continue
        
        new_lines.append(line)
    
    # 添加未找到的配置项
    if mode == 'import':
        new_lines.append("\n# === USB 硬盘优化配置（自动添加） ===\n")
        for key, value in config_map.items():
            if key not in updated_keys:
                new_lines.append(f"{key} = {value}\n")
                updated_keys.add(key)
    
    # 写入配置
    with open(config_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    
    print(f"✅ 已更新 {len(updated_keys)} 个配置项:")
    for key in sorted(updated_keys):
        print(f"   - {key} = {config_map[key]}")
    
    print("\n⚠️  请重启 PostgreSQL 以使配置生效：")
    print("   Windows: net stop postgresql-x64-13 && net start postgresql-x64-13")
    print("   Linux:   sudo systemctl restart postgresql")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='PostgreSQL USB 硬盘优化配置',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  # 导入模式（高速，不安全）
  python scripts/test/optimize_postgresql_for_usb.py --config "C:\\PostgreSQL\\data\\postgresql.conf" --mode import
  
  # 安全模式（恢复默认）
  python scripts/test/optimize_postgresql_for_usb.py --config "C:\\PostgreSQL\\data\\postgresql.conf" --mode safe
        """
    )
    
    parser.add_argument('--config', type=str, required=True,
                       help='postgresql.conf 文件路径')
    parser.add_argument('--mode', type=str, choices=['import', 'safe'], default='import',
                       help='模式：import=导入优化，safe=恢复安全配置')
    
    args = parser.parse_args()
    
    config_path = Path(args.config)
    optimize_config(config_path, args.mode)


if __name__ == '__main__':
    main()

