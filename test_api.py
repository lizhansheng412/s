#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Semantic Scholar API 连接
用于验证 API 是否正常工作
"""

import requests
import json

# 尝试导入 API Key 配置
try:
    from config import API_KEY
except ImportError:
    API_KEY = None


def test_api_connection():
    """测试 API 连接"""
    print("测试 Semantic Scholar API 连接")
    
    # 测试端点
    api_endpoint = "https://api.semanticscholar.org/graph/v1/paper/batch"
    
    # 检查 API Key
    if API_KEY:
        print(f"\n✓ 使用 API Key: {API_KEY[:10]}...（已部分隐藏）")
    else:
        print("\n⚠ 未配置 API Key，使用公共访问（速率限制较低）")
        print("⚠ 建议申请免费 API Key: https://www.semanticscholar.org/product/api#api-key-form")
    
    # 测试数据：使用一些已知的 corpusId
    test_ids = [
        "CorpusId:1234567",  # 示例 ID
        "CorpusId:7654321"   # 示例 ID
    ]
    
    # 请求字段
    fields = "title,abstract,year,citationCount"
    
    print(f"\n测试端点: {api_endpoint}")
    print(f"测试 ID: {test_ids}")
    print(f"请求字段: {fields}\n")
    
    try:
        # 准备请求头
        headers = {'Content-Type': 'application/json'}
        if API_KEY:
            headers['x-api-key'] = API_KEY
        
        # 发送请求
        print("\n发送 API 请求...")
        response = requests.post(
            api_endpoint,
            headers=headers,
            json={'ids': test_ids},
            params={'fields': fields},
            timeout=30
        )
        
        print(f"响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            print("✓ API 连接成功！\n")
            
            # 解析响应
            data = response.json()
            print(f"返回数据数量: {len(data)}")
            
            # 显示第一条数据（如果有）
            if data and data[0]:
                print("\n示例响应数据:")
                print(json.dumps(data[0], indent=2, ensure_ascii=False))
            else:
                print("\n注意: 测试 ID 可能不存在，返回空数据")
                print("这是正常的，说明 API 连接工作正常")
            
            print("\n✓ API 工作正常，可以开始处理数据")
            return True
        else:
            print(f"✗ API 返回错误状态码: {response.status_code}")
            print(f"错误信息: {response.text}")
            
            if response.status_code == 429:
                print("\n⚠ 速率限制 (429 Too Many Requests)")
                print("\n解决方案:")
                print("1. 申请免费 API Key: https://www.semanticscholar.org/product/api#api-key-form")
                print("2. 等待一段时间（10-60分钟）后再试")
                print("3. 将 API Key 填写到 config.py 的 API_KEY 配置中")
            
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ 网络请求失败: {e}")
        print("\n请检查:")
        print("1. 网络连接是否正常")
        print("2. 是否可以访问 semanticscholar.org")
        print("3. 防火墙或代理设置")
        return False
    
    except Exception as e:
        print(f"✗ 发生错误: {e}")
        return False


def main():
    """主函数"""
    success = test_api_connection()
    
    print("\n" + "=" * 60)
    if success:
        print("测试完成 - API 工作正常")
    else:
        print("测试失败 - 请检查错误信息")
    print("=" * 60)


if __name__ == "__main__":
    main()

