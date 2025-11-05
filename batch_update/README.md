# 1. 首次使用：创建表
python batch_update/init_temp_table.py
python batch_update/init_temp_table.py --init-log-table

# 2. 导入数据（必须指定数据集类型）
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc

# 3. 中断后继续运行（自动跳过已处理文件）
python batch_update/import_gz_to_temp_fast.py D:\gz_temp\s2orc --dataset s2orc

# 3.1 导入数据完成后立即创建索引
python batch_update/init_temp_table.py --create-indexes

# 4. 清空临时表但保留导入记录
python batch_update/init_temp_table.py --truncate

# 5. 如需重新处理所有文件
python batch_update/init_temp_table.py --clear-log

# 7.多进程模式
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc

# 8.多进程 + 删除文件
python batch_update/import_gz_to_temp.py D:\gz_temp\s2orc --dataset s2orc --delete-gz