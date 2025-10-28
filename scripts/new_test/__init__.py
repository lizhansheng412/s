"""
新测试脚本模块 - corpusid 到 gz 文件名映射

脚本说明：
- disk_config.py: 硬盘配置管理（m1/m2/m3）
- init_corpusid_mapping.py: 创建/维护 corpus_new_bigdataset 表
- stream_gz_to_mapping.py: 提取 corpusid 并映射到 gz 文件名
- batch_process_mapping.py: 批量处理（支持 m1/m2/m3）

处理流程：
1. init_corpusid_mapping.py - 创建无约束表（极速导入模式）
2. batch_process_mapping.py - 批量 COPY 插入数据
3. init_corpusid_mapping.py --finalize - 去重、建主键、优化

日志文件夹：logs/corpusid_mapping_progress/ 和 logs/corpusid_mapping_failed/
"""

