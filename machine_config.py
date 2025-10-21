"""
机器配置 - 定义每台电脑处理哪些文件夹/表
"""

# 3台机器的表分配方案
MACHINE_CONFIGS = {
    'machine1': {
        'folders': ['embeddings-specter_v1', 's2orc'],
        'tables': ['embeddings_specter_v1', 's2orc'],
        'description': '电脑1：处理 embeddings-specter_v1 和 s2orc'
    },
    'machine2': {
        'folders': ['embeddings-specter_v2', 's2orc_v2'],
        'tables': ['embeddings_specter_v2', 's2orc_v2'],
        'description': '电脑2：处理 embeddings-specter_v2 和 s2orc_v2'
    },
    'machine3': {
        'folders': ['abstracts', 'authors', 'citations', 'paper-ids', 'papers', 'publication-venues', 'tldrs'],
        'tables': ['abstracts', 'authors', 'citations', 'paper_ids', 'papers', 'publication_venues', 'tldrs'],
        'description': '电脑3：处理 abstracts, authors, citations, paper_ids, papers, publication_venues, tldrs'
    }
}

# 文件夹名 -> 表名 映射（处理文件夹名和表名的差异）
FOLDER_TO_TABLE_MAP = {
    'embeddings-specter_v1': 'embeddings_specter_v1',
    'embeddings-specter_v2': 'embeddings_specter_v2',
    's2orc': 's2orc',
    's2orc_v2': 's2orc_v2',
    'abstracts': 'abstracts',
    'authors': 'authors',
    'citations': 'citations',
    'paper-ids': 'paper_ids',
    'papers': 'papers',
    'publication-venues': 'publication_venues',
    'tldrs': 'tldrs'
}

def get_machine_config(machine_id: str) -> dict:
    """
    获取指定机器的配置
    
    Args:
        machine_id: 机器ID ('machine1', 'machine2', 'machine3')
    
    Returns:
        机器配置字典
    """
    if machine_id not in MACHINE_CONFIGS:
        raise ValueError(f"无效的机器ID: {machine_id}。有效值: {list(MACHINE_CONFIGS.keys())}")
    
    return MACHINE_CONFIGS[machine_id]


def get_all_machines():
    """获取所有机器ID列表"""
    return list(MACHINE_CONFIGS.keys())

