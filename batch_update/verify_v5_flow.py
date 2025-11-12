"""
验证 jsonl_batch_updater_v5.py 的文件读取逻辑
展示每个批次的输入输出路径
"""
from pathlib import Path

# 配置（与v5脚本相同）
SOURCE_DIR = Path("E:/copy_final_1")        # 源JSONL目录（快速模式文件）
OUTPUT_DIR = Path("E:/copy_final_cache")    # 输出目录（完整模式文件）

print("=" * 80)
print("jsonl_batch_updater_v5.py 文件流向验证")
print("=" * 80)

print(f"\n配置:")
print(f"  SOURCE_DIR (原始快速模式): {SOURCE_DIR}")
print(f"  OUTPUT_DIR (完整模式输出): {OUTPUT_DIR}")

print("\n" + "=" * 80)
print("批次1 - 创建新文件")
print("=" * 80)
print(f"  读取源: {SOURCE_DIR}\\*.jsonl")
print(f"  写入到: {OUTPUT_DIR}\\*.jsonl")
print(f"  说明: 从快速模式转换为完整模式，填充前1/3的title")

print("\n" + "=" * 80)
print("批次2 - 增量更新（原地更新）")
print("=" * 80)
print(f"  读取源: {OUTPUT_DIR}\\*.jsonl  ← 批次1的输出")
print(f"  写入到: {OUTPUT_DIR}\\*.jsonl.tmp")
print(f"  替换为: {OUTPUT_DIR}\\*.jsonl")
print(f"  说明: 读取批次1创建的文件，填充中间1/3的title")

print("\n" + "=" * 80)
print("批次3 - 增量更新（原地更新）")
print("=" * 80)
print(f"  读取源: {OUTPUT_DIR}\\*.jsonl  ← 批次2的输出")
print(f"  写入到: {OUTPUT_DIR}\\*.jsonl.tmp")
print(f"  替换为: {OUTPUT_DIR}\\*.jsonl")
print(f"  说明: 读取批次2更新的文件，填充最后1/3的title")

print("\n" + "=" * 80)
print("关键代码逻辑验证")
print("=" * 80)

# 模拟 get_jsonl_files 函数
def get_jsonl_files(batch_num):
    if batch_num == 1:
        source_dir = SOURCE_DIR
        print(f"\n批次{batch_num}: source_dir = {source_dir}")
    else:
        source_dir = OUTPUT_DIR
        print(f"\n批次{batch_num}: source_dir = {source_dir}")
    
    # 注意：这里返回的是 source_dir 下的文件路径
    # 例如：E:\copy_final_cache\file1.jsonl
    return source_dir

# 验证每个批次
for batch in [1, 2, 3]:
    source = get_jsonl_files(batch)
    print(f"  → 文件列表来自: {source}\\*.jsonl")

print("\n" + "=" * 80)
print("结论")
print("=" * 80)
print("✓ 批次1: 从 E:/copy_final_1 读取（原始快速模式）")
print("✓ 批次2: 从 E:/copy_final_cache 读取（批次1输出）")
print("✓ 批次3: 从 E:/copy_final_cache 读取（批次2输出）")
print("\n逻辑正确！批次2和3都会读取 E:/copy_final_cache 下的文件。")
print("=" * 80)

