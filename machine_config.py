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
        'folders': ['abstracts', 'authors', 'papers', 'publication-venues', 'tldrs', 'citations'],
        'tables': ['abstracts', 'authors', 'papers', 'publication_venues', 'tldrs', 'citations'],
        'description': 'Machine 3: Process abstracts, authors, papers, publication_venues, tldrs, citations (citations last due to slow processing)'
    },
    'machine4': {
        'folders': ['paper-ids'],
        'tables': ['paper_ids'],
        'description': 'Machine 4: Process paper-ids'
    }
}

def get_machine_config(machine_id: str) -> dict:
    """
    Get machine configuration
    
    Args:
        machine_id: Machine ID ('machine1', 'machine2', 'machine3', 'machine4')
    
    Returns:
        Machine configuration dict
    """
    if machine_id not in MACHINE_CONFIGS:
        raise ValueError(f"Invalid machine ID: {machine_id}. Valid values: {list(MACHINE_CONFIGS.keys())}")
    
    return MACHINE_CONFIGS[machine_id]
