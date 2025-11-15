# Step One 执行说明

## 脚本执行顺序

按照文件编号依次执行以下脚本：

### 1. 构建完整的 corpusid 表
```bash
python 1_build_full_corpusid_table.py
```
**功能**: 解压 paper-ids 数据集，提取 corpusid 字段并批量插入数据库

---

### 2. 构建 papers/abstracts/tldrs 分区表
```bash
python 2_build_papers_abstracts_tldrs_table.py
```
**功能**: 创建分区表，导入 papers、abstracts、tldrs 数据集并建立索引

---

### 3. 构建 authors 和 publication_venues 表
```bash
python 3_authors_publication-venues.py
```
**功能**: 创建 authors 分区表和 publication_venues 普通表，导入数据并建立索引

---

### 4. 构建 citations 和 references 表
```bash
python 4_citations_reference.py
```
**功能**: 创建 citation_raw 表，导入引用数据，构造 references 和 citations 数组表

**执行阶段**:
- 阶段 0: 创建 citation_raw 表
- 阶段 1: 导入 gz 文件
- 阶段 2: 创建索引
- 阶段 3: 构造 temp_references
- 阶段 4: 构造 temp_citations
- 阶段 5: 全部执行

---

### 5. 构建 corpusid_mapping_title 表
```bash
python 5_build_corpusid_title.py
```
**功能**: 从 papers 数据集提取 corpusid 和 title 字段，建立映射表

**执行阶段**:
- 阶段 0: 创建表
- 阶段 1: 导入 gz 文件
- 阶段 2: 创建主键索引
- 阶段 3: 全部执行

---

### 6. 构建 s2orc 和 embeddings 表
```bash
python 6_build_s2orc_embedding_table.py
```
**功能**: 创建 s2orc、s2orc_v2、embeddings_specter_v1、embeddings_specter_v2 分区表

**数据集选择**:
- 1: s2orc (machine0, 1500GB)
- 2: s2orc_v2 (machine0, 650GB)
- 3: embeddings_specter_v1 (machine3, 2200GB)
- 4: embeddings_specter_v2 (machine3, 2200GB)

**执行阶段**:
- 阶段 0: 创建分区表
- 阶段 1: 导入 gz 文件
- 阶段 2: 创建索引
- 阶段 3: 全部执行

---

## 注意事项

1. 所有脚本支持交互式选择执行阶段
2. 脚本 4、5、6 支持分阶段执行，可根据需要选择特定阶段
3. 确保数据文件路径配置正确
4. 执行前确认数据库连接配置 (machine_db_config.py)
