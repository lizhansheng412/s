"""
Final Delivery - 数据提取与导出

核心脚本：
- init_table.py: 创建表、去重、建主键
- extract_corpusid.py: 从 gz 文件提取 corpusid
- rebuild_sorted_table_v2.py: 按 corpusid 排序重建表（性能优化）
- export_final_delivery.py: 导出数据到 JSONL 文件

完整工作流程：
1. init_table.py - 创建无约束表
2. extract_corpusid.py - 提取并插入数据
3. init_table.py --finalize - 去重、建主键
4. rebuild_sorted_table_v2.py --yes - 重建排序表（提升性能 5-10倍）
5. export_final_delivery.py - 导出到 E:\final_delivery

日志目录：
- logs/final_delivery_progress/ - 进度日志
- logs/final_delivery_failed/ - 失败日志
"""
