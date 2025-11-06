"""
测试gz文件内容，检查为什么某些文件导入0条记录
"""
import gzip
import orjson
from pathlib import Path

def check_gz_file(gz_file_path, max_lines=10):
    """
    检查gz文件的内容结构
    
    Args:
        gz_file_path: gz文件路径
        max_lines: 最多检查的行数
    """
    gz_path = Path(gz_file_path)
    
    if not gz_path.exists():
        print(f"❌ 文件不存在: {gz_path}")
        return
    
    print(f"\n{'='*80}")
    print(f"检查文件: {gz_path.name}")
    print(f"{'='*80}")
    
    total_lines = 0
    valid_records = 0
    missing_corpusid = 0
    missing_content = 0
    parse_errors = 0
    sample_keys = set()
    
    try:
        with gzip.open(gz_path, 'rt', encoding='utf-8', errors='replace') as f:
            for i, line in enumerate(f):
                total_lines += 1
                
                # 只详细显示前几条
                if i < 3:
                    print(f"\n--- 记录 {i+1} ---")
                
                try:
                    data = orjson.loads(line.strip())
                    
                    # 收集所有字段名
                    sample_keys.update(data.keys())
                    
                    has_corpusid = 'corpusid' in data and data.get('corpusid') is not None
                    has_content = 'content' in data and data.get('content') is not None
                    
                    if i < 3:
                        print(f"字段: {list(data.keys())}")
                        print(f"有 corpusid: {has_corpusid}")
                        if has_corpusid:
                            print(f"  corpusid 值: {data.get('corpusid')}")
                        print(f"有 content: {has_content}")
                        if has_content:
                            content = data.get('content')
                            if isinstance(content, dict):
                                print(f"  content 类型: dict, 字段数: {len(content)}")
                                print(f"  content 字段: {list(content.keys())[:10]}...")  # 只显示前10个
                            else:
                                print(f"  content 类型: {type(content).__name__}")
                    
                    # 统计
                    if has_corpusid and has_content:
                        valid_records += 1
                    else:
                        if not has_corpusid:
                            missing_corpusid += 1
                        if not has_content:
                            missing_content += 1
                            
                except Exception as e:
                    parse_errors += 1
                    if i < 3:
                        print(f"解析失败: {e}")
                        print(f"原始内容: {line[:200]}...")
                
                # 限制检查行数
                if i >= max_lines - 1:
                    print(f"\n... (已检查前 {max_lines} 行，继续统计)")
                    # 继续统计，但不再显示详情
                    for remaining_line in f:
                        total_lines += 1
                        try:
                            data = orjson.loads(remaining_line.strip())
                            sample_keys.update(data.keys())
                            
                            has_corpusid = 'corpusid' in data and data.get('corpusid') is not None
                            has_content = 'content' in data and data.get('content') is not None
                            
                            if has_corpusid and has_content:
                                valid_records += 1
                            else:
                                if not has_corpusid:
                                    missing_corpusid += 1
                                if not has_content:
                                    missing_content += 1
                        except:
                            parse_errors += 1
                    break
        
        # 统计结果
        print(f"\n{'='*80}")
        print(f"统计结果:")
        print(f"  总行数: {total_lines}")
        print(f"  有效记录（有corpusid和content）: {valid_records}")
        print(f"  缺少 corpusid: {missing_corpusid}")
        print(f"  缺少 content: {missing_content}")
        print(f"  解析错误: {parse_errors}")
        print(f"\n所有出现的字段名:")
        print(f"  {sorted(sample_keys)}")
        print(f"{'='*80}")
        
        # 结论
        if valid_records == 0:
            print(f"\n⚠️  结论: 该文件没有有效记录，所以导入0条是正常的")
            if missing_content > 0:
                print(f"   主要原因: 缺少 content 字段")
            if missing_corpusid > 0:
                print(f"   主要原因: 缺少 corpusid 字段")
        else:
            print(f"\n✓ 该文件有 {valid_records} 条有效记录")
        
    except Exception as e:
        print(f"\n❌ 读取文件失败: {e}")


def check_multiple_files(gz_files):
    """
    检查多个gz文件
    
    Args:
        gz_files: gz文件路径列表
    """
    for gz_file in gz_files:
        check_gz_file(gz_file, max_lines=10)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="检查gz文件内容结构")
    parser.add_argument("files", nargs="+", help="要检查的gz文件路径")
    parser.add_argument("--max-lines", type=int, default=10, help="详细检查的最大行数（默认10）")
    
    args = parser.parse_args()
    
    for file_path in args.files:
        check_gz_file(file_path, max_lines=args.max_lines)



