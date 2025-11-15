# 机器配置信息

## 设备配置详情

### machine0
- **用户名**: dev
- **电脑密码**: Grained2025！
- **设备码**: 755101830
- **秘钥**: Grained0
- **外接硬盘**: disk0
- **存储数据集**: s2orc, s2orc_v2
- **数据库**: PostgreSQL数据表（不包括数据库驱动程序只有可迁移的数据库表）

### machine2  
- **用户名**: Grained.AI
- **电脑密码**: 无
- **设备码**: 1376569781
- **秘钥**: Grained2
- **外接硬盘**: disk2
- **存储数据集**: papers, abstracts, tldrs, citations, authors, paper_ids
- **数据库**: PostgreSQL数据表（不包括数据库驱动程序只有可迁移的数据库表）

### machine3
- **用户名**: ice
- **电脑密码**: 950726
- **设备码**: 待补充
- **秘钥**: 待补充
- **外接硬盘**: disk3
- **存储数据集**: embeddings-specter_v1, embeddings-specter_v2
- **数据库**: PostgreSQL数据表（不包括数据库驱动程序只有可迁移的数据库表）

---

## Step One 执行步骤

### 前置条件
当以下任一机器的所有数据集gz文件下载完成时，执行Step One启动对应的PostgreSQL服务：
- machine0的外置硬盘disk0中的s2orc、s2orc_v2数据集
- machine2内置SSD中的papers、abstracts、tldrs、citations、authors、paper_ids数据集  
- machine3的外置硬盘disk3中的embeddings-specter_v1、embeddings-specter_v2数据集

#### machine0  
- **服务名**: machine0
- **启动命令**: `"D:\person_data\postgresql\bin\pg_ctl.exe" runservice -N "machine0" -D "E:\postgresql_data" -w -o "-p 54300"`
- **作用**: 连接外置硬盘disk0中的postgresql数据目录中的数据

#### machine3（这个机器需要创建新的postgresql服务，Vivian拿走的电脑，创建时注意目录名）
**创建PostgreSQL服务**（以管理员身份运行PowerShell7）：
```powershell
& "D:\person_data\postgresql\bin\pg_ctl.exe" register -N "machine3" -D "E:\postgresql_data" -o "-p 54330" -w
```

**初始化数据目录**：
```bash
initdb -D E:\postgresql_data
```
创建完服务后同样启动服务：
- **服务名**: machine3
- **启动命令**: `"D:\person_data\postgresql\bin\pg_ctl.exe" runservice -N "machine3" -D "E:\postgresql_data" -w -o "-p 54330"`
- **作用**: 连接外置硬盘disk3中的postgresql数据目录中的数据

### 参数说明
- **register**: 创建Windows服务
- **-N**: 指定服务名
- **-D**: 指定数据目录
- **-o**: 向Postgres传递参数（可设置端口、监听地址等）
- **-w**: 等待创建完成
---
