"""
Machine Configuration - Define folders/tables for each machine
"""

MACHINE_CONFIGS = {
    'machine1': {
        'folders': ['embeddings-specter_v1', 's2orc'],
        'tables': ['embeddings_specter_v1', 's2orc'],
        'description': 'Machine 1: Process embeddings-specter_v1 and s2orc'
    },
    'machine2': {
        'folders': ['embeddings-specter_v2', 's2orc_v2'],
        'tables': ['embeddings_specter_v2', 's2orc_v2'],
        'description': 'Machine 2: Process embeddings-specter_v2 and s2orc_v2'
    },
    'machine3': {
        'folders': ['abstracts', 'authors', 'citations', 'paper-ids', 'papers', 'publication-venues', 'tldrs'],
        'tables': ['abstracts', 'authors', 'citations', 'paper_ids', 'papers', 'publication_venues', 'tldrs'],
        'description': 'Machine 3: Process abstracts, authors, citations, paper_ids, papers, publication_venues, tldrs'
    }
}

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
    Get machine configuration
    
    Args:
        machine_id: Machine ID ('machine1', 'machine2', 'machine3')
    
    Returns:
        Machine configuration dict
    """
    if machine_id not in MACHINE_CONFIGS:
        raise ValueError(f"Invalid machine ID: {machine_id}. Valid values: {list(MACHINE_CONFIGS.keys())}")
    
    return MACHINE_CONFIGS[machine_id]


def get_all_machines():
    """Get all machine IDs"""
    return list(MACHINE_CONFIGS.keys())
