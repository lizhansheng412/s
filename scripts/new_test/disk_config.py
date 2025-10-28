#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
硬盘配置管理 - 映射硬盘ID到文件夹和字段
"""

# 硬盘配置字典
DISK_CONFIGS = {
    'm1': {
        'description': 'Machine1 硬盘 - embeddings_v1 + s2orc',
        'folders': ['embeddings-specter_v1', 's2orc'],
        'fields': ['embeddings_specter_v1', 's2orc']
    },
    'm2': {
        'description': 'Machine2 硬盘 - embeddings_v2 + s2orc_v2',
        'folders': ['embeddings-specter_v2', 's2orc_v2'],
        'fields': ['embeddings_specter_v2', 's2orc_v2']
    },
    'm3': {
        'description': 'Machine3 硬盘 - citations',
        'folders': ['citations'],
        'fields': ['citation']
    }
}

# 所有支持的字段
ALL_FIELDS = ['embeddings_specter_v1', 'embeddings_specter_v2', 's2orc', 's2orc_v2', 'citation']


def get_disk_config(disk_id: str) -> dict:
    """获取硬盘配置"""
    if disk_id not in DISK_CONFIGS:
        raise ValueError(f"不支持的硬盘ID: {disk_id}，支持: {list(DISK_CONFIGS.keys())}")
    return DISK_CONFIGS[disk_id]


def get_field_for_folder(folder_name: str) -> str:
    """根据文件夹名获取对应的表字段名"""
    for disk_id, config in DISK_CONFIGS.items():
        for folder, field in zip(config['folders'], config['fields']):
            if folder == folder_name:
                return field
    raise ValueError(f"未找到文件夹 {folder_name} 对应的字段")

