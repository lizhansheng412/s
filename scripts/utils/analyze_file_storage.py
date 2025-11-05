#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析文件在磁盘上的物理存储顺序
"""

import subprocess
import time
from pathlib import Path
from datetime import datetime
import csv

# =============================================================================
# 配置
# =============================================================================

TARGET_DIR = Path(r"E:\final_delivery")
SAMPLE_SIZE = None  # None=全部文件，或指定采样数量（如100）【首次运行建议用小样本】
OUTPUT_DIR = Path(__file__).parent.parent.parent / "logs" / "storage_analysis"

# =============================================================================
# 核心功能
# =============================================================================

def get_file_lcn(file_path: Path) -> tuple[int | None, str]:
    """获取文件的起始LCN（物理簇号）"""
    try:
        result = subprocess.run(
            ["fsutil", "file", "queryextents", str(file_path)],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        # 解析输出：VCN: 0xXXX LCN: 0xYYY Length: 0xZZZ
        for line in result.stdout.splitlines():
            if "VCN:" in line and "LCN:" in line:
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "LCN:" and i + 1 < len(parts):
                        lcn_hex = parts[i + 1]
                        return int(lcn_hex, 16), ""
        
        return None, f"No LCN found: {result.stderr[:50]}"
    
    except subprocess.TimeoutExpired:
        return None, "Timeout"
    except Exception as e:
        return None, str(e)[:50]


def calculate_rank_correlation(list1, list2):
    """计算Spearman秩相关系数"""
    rank1 = {item['name']: idx for idx, item in enumerate(list1)}
    rank2 = {item['name']: idx for idx, item in enumerate(list2)}
    
    n = len(list1)
    sum_d_squared = sum((rank1[name] - rank2[name]) ** 2 for name in rank1)
    
    return 1 - (6 * sum_d_squared) / (n * (n**2 - 1))


# =============================================================================
# 主流程
# =============================================================================

def main():
    print("=" * 80)
    print("文件存储顺序分析工具（简化版）")
    print("=" * 80)
    print(f"目标目录: {TARGET_DIR}")
    print(f"样本大小: {'全部' if SAMPLE_SIZE is None else SAMPLE_SIZE}")
    print()
    print("⚠️  需要管理员权限运行 PowerShell")
    print()
    
    # 测试fsutil是否可用
    print("【测试 fsutil 命令】")
    test_result = subprocess.run(["fsutil", "fsinfo", "drives"], 
                                capture_output=True, text=True)
    if test_result.returncode != 0:
        print("✗ fsutil 命令不可用或无管理员权限")
        print(f"  错误: {test_result.stderr}")
        return
    print("✓ fsutil 命令可用")
    print()
    
    input("按 Enter 开始...")
    print()
    
    start_time = time.time()
    
    # 步骤1: 扫描文件
    print("【步骤1】扫描文件...")
    all_files = sorted(TARGET_DIR.glob("*.jsonl"))
    
    if SAMPLE_SIZE and SAMPLE_SIZE < len(all_files):
        import random
        all_files = random.sample(all_files, SAMPLE_SIZE)
    
    print(f"✓ 找到 {len(all_files)} 个文件")
    print()
    
    # 步骤2: 获取文件信息
    print("【步骤2】获取文件信息...")
    file_data = []
    success = 0
    failed = 0
    
    for idx, fpath in enumerate(all_files, 1):
        stat = fpath.stat()
        lcn, error = get_file_lcn(fpath)
        
        file_data.append({
            'name': fpath.name,
            'size': stat.st_size,
            'mtime': stat.st_mtime,
            'ctime': stat.st_ctime,
            'lcn': lcn,
            'error': error
        })
        
        if lcn is not None:
            success += 1
        else:
            failed += 1
            if failed <= 3:  # 只显示前3个错误
                print(f"  失败示例 {failed}: {fpath.name} - {error}")
        
        if idx % 100 == 0 or idx == len(all_files):
            print(f"\r进度: {idx}/{len(all_files)} | 成功: {success} | 失败: {failed}", 
                  end='', flush=True)
    
    print()
    print()
    
    if success == 0:
        print("✗ 没有成功获取任何文件的LCN")
        print()
        print("可能的原因：")
        print("  1. 未以管理员权限运行")
        print("  2. E盘不是NTFS文件系统")
        print("  3. fsutil版本不支持")
        print()
        print("请尝试：右键 PowerShell → '以管理员身份运行'")
        return
    
    # 步骤3: 分析
    print("【步骤3】分析存储顺序...")
    valid = [f for f in file_data if f['lcn'] is not None]
    
    by_name = sorted(valid, key=lambda x: x['name'])
    by_mtime = sorted(valid, key=lambda x: x['mtime'])
    by_lcn = sorted(valid, key=lambda x: x['lcn'])
    
    corr_name = calculate_rank_correlation(by_name, by_lcn)
    corr_time = calculate_rank_correlation(by_mtime, by_lcn)
    
    print(f"  有效样本: {len(valid)}/{len(file_data)}")
    print(f"  文件名顺序 vs 物理位置: {corr_name:+.4f}")
    print(f"  修改时间   vs 物理位置: {corr_time:+.4f}")
    print()
    
    # 连续性分析
    lcn_gaps = [by_lcn[i+1]['lcn'] - by_lcn[i]['lcn'] 
                for i in range(len(by_lcn)-1)]
    avg_gap = sum(lcn_gaps) / len(lcn_gaps) if lcn_gaps else 0
    
    avg_size_clusters = (sum(f['size'] for f in valid) / len(valid)) / 4096
    
    print("【结论】")
    best_corr = max(corr_name, corr_time)
    
    if best_corr > 0.7:
        print(f"✓ 文件较连续（相关性 {best_corr:.2f}）")
        print(f"  → 批量复制可能有效，建议使用批量模式")
    elif best_corr > 0.3:
        print(f"○ 文件部分连续（相关性 {best_corr:.2f}）")
        print(f"  → 批量复制可能有小幅提升，建议实测")
    else:
        print(f"✗ 文件高度分散（相关性 {best_corr:.2f}）")
        print(f"  → 批量复制不会减少寻道，建议用流水线模式")
    
    print(f"  平均LCN间隔: {avg_gap:.0f} 簇")
    print(f"  平均文件大小: {avg_size_clusters:.0f} 簇")
    print()
    
    # 步骤4: 保存报告
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    csv_file = OUTPUT_DIR / f"analysis_{timestamp}.csv"
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'size', 'mtime', 'ctime', 'lcn', 'error'])
        writer.writeheader()
        writer.writerows(file_data)
    
    print(f"✓ 报告已保存: {csv_file}")
    print(f"  耗时: {time.time() - start_time:.1f}秒")
    print()


if __name__ == "__main__":
    main()
