#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S2ORC 数据集批量增强处理脚本
从 GZ 文件中提取论文数据，调用 Semantic Scholar API 获取详细信息，并保存增强后的数据
"""

import gzip
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple
import requests
from tqdm import tqdm

# 导入用户配置
try:
    from config import (
        INPUT_DIR as CONFIG_INPUT_DIR,
        API_KEY,
        OUTPUT_PREFIX,
        OUTPUT_EXTENSION,
        LOG_FILE,
        LOG_LEVEL
    )
except ImportError:
    # 如果没有配置文件，使用默认值
    CONFIG_INPUT_DIR = r"E:\machine_win01\2025-09-22"
    API_KEY = None
    OUTPUT_PREFIX = "enhanced_"
    OUTPUT_EXTENSION = ".jsonl"
    LOG_FILE = "s2orc_processing.log"
    LOG_LEVEL = "INFO"


# ==================== API 配置（固定配置，无需修改）====================

# Semantic Scholar API 端点
API_ENDPOINT = "https://api.semanticscholar.org/graph/v1/paper/batch"

# 批量查询大小（API 限制最大 500）
BATCH_SIZE = 500

# API 请求超时时间（秒）
API_TIMEOUT = 60

# 批次之间的延迟时间（秒），避免 API 限流
BATCH_DELAY = 3  # 增加到 3 秒以避免速率限制

# API 重试配置
MAX_RETRIES = 3  # 最大重试次数
RETRY_DELAY = 10  # 重试延迟（秒）

# 需要从 API 获取的字段列表
API_FIELDS = (
    "url,title,abstract,venue,publicationVenue,year,referenceCount,"
    "citationCount,influentialCitationCount,isOpenAccess,openAccessPdf,"
    "fieldsOfStudy,s2FieldsOfStudy,publicationTypes,publicationDate,"
    "journal,citationStyles,authors,citations,references,embedding,tldr"
)

# API 响应大小限制
MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10 MB

# ==================== 日志配置 ====================

# 配置日志
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class S2ORCProcessor:
    """S2ORC 数据处理器"""
    
    def __init__(self, input_dir: str):
        """
        初始化处理器
        
        Args:
            input_dir: 包含 GZ 文件的目录路径
        """
        self.input_dir = Path(input_dir)
        if not self.input_dir.exists():
            raise ValueError(f"输入目录不存在: {input_dir}")
        
        logger.info(f"初始化 S2ORC 处理器，输入目录: {self.input_dir}")
    
    def scan_gz_files(self) -> List[Path]:
        """
        扫描目录中的所有 GZ 文件
        
        Returns:
            GZ 文件路径列表
        """
        gz_files = list(self.input_dir.glob("*.gz"))
        logger.info(f"找到 {len(gz_files)} 个 GZ 文件")
        return sorted(gz_files)
    
    def read_gz_file(self, gz_path: Path) -> List[Dict[str, Any]]:
        """
        读取并解压 GZ 文件，返回论文数据列表
        
        Args:
            gz_path: GZ 文件路径
            
        Returns:
            论文数据列表（每个元素是一个 JSON 对象）
        """
        decompress_start = time.time()
        papers = []
        try:
            with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        paper = json.loads(line)
                        papers.append(paper)
                    except json.JSONDecodeError as e:
                        logger.error(f"文件 {gz_path.name} 第 {line_num} 行 JSON 解析错误: {e}")
                        raise
            
            decompress_time = time.time() - decompress_start
            logger.info(f"成功读取 {gz_path.name}，共 {len(papers)} 篇论文")
            logger.info(f"解压耗时: {decompress_time:.2f} 秒")
            return papers
        
        except Exception as e:
            logger.error(f"读取文件 {gz_path.name} 失败: {e}")
            raise
    
    def extract_corpus_ids(self, papers: List[Dict[str, Any]]) -> List[str]:
        """
        从论文数据中提取 corpusId 并格式化
        
        Args:
            papers: 论文数据列表
            
        Returns:
            格式化的 corpusId 列表（格式：CorpusId:<数字>）
        """
        corpus_ids = []
        for i, paper in enumerate(papers):
            if 'corpusId' in paper and paper['corpusId']:
                corpus_id = paper['corpusId']
                # 确保格式为 CorpusId:<数字>
                formatted_id = f"CorpusId:{corpus_id}"
                corpus_ids.append(formatted_id)
            else:
                logger.warning(f"论文索引 {i} 缺少 corpusId 字段")
                corpus_ids.append(None)
        
        valid_count = sum(1 for cid in corpus_ids if cid is not None)
        logger.info(f"提取到 {valid_count} 个有效的 corpusId")
        return corpus_ids
    
    def fetch_paper_details_batch(
        self, 
        corpus_ids: List[str],
        batch_size: int = BATCH_SIZE
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量调用 Semantic Scholar API 获取论文详细信息
        
        Args:
            corpus_ids: corpusId 列表
            batch_size: 每批次的大小
            
        Returns:
            字典，key 为 corpusId（数字部分），value 为论文详细信息
        """
        # 过滤掉 None 值
        valid_ids = [cid for cid in corpus_ids if cid is not None]
        
        if not valid_ids:
            logger.warning("没有有效的 corpusId 需要查询")
            return {}
        
        all_details = {}
        total_batches = (len(valid_ids) + batch_size - 1) // batch_size
        
        logger.info(f"开始批量查询，共 {len(valid_ids)} 个 ID，分 {total_batches} 批次")
        
        with tqdm(total=len(valid_ids), desc="API 查询进度", unit="篇") as pbar:
            for batch_idx in range(0, len(valid_ids), batch_size):
                batch_ids = valid_ids[batch_idx:batch_idx + batch_size]
                current_batch = (batch_idx // batch_size) + 1
                
                logger.info(f"处理批次 {current_batch}/{total_batches}，包含 {len(batch_ids)} 个 ID")
                
                try:
                    details = self._fetch_single_batch(batch_ids)
                    all_details.update(details)
                    pbar.update(len(batch_ids))
                    
                    # 避免 API 限流，添加短暂延迟
                    time.sleep(BATCH_DELAY)
                    
                except Exception as e:
                    logger.error(f"批次 {current_batch} API 调用失败: {e}")
                    logger.error(f"失败的批次 ID: {batch_ids[:5]}... (显示前5个)")
                    raise
        
        logger.info(f"API 查询完成，成功获取 {len(all_details)} 篇论文的详细信息")
        return all_details
    
    def _fetch_single_batch(self, corpus_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        调用一次 API 获取单批次论文详细信息（支持重试）
        
        Args:
            corpus_ids: corpusId 列表（格式：CorpusId:<数字>）
            
        Returns:
            字典，key 为 corpusId（数字部分），value 为论文详细信息
        """
        headers = {
            'Content-Type': 'application/json'
        }
        
        # 如果有 API Key，添加到请求头
        if API_KEY:
            headers['x-api-key'] = API_KEY
        
        payload = {
            'ids': corpus_ids
        }
        
        params = {
            'fields': API_FIELDS
        }
        
        # 重试机制
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(
                    API_ENDPOINT,
                    headers=headers,
                    json=payload,
                    params=params,
                    timeout=API_TIMEOUT
                )
            
                # 检查响应状态
                if response.status_code == 429:
                    # 速率限制错误
                    if attempt < MAX_RETRIES - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        logger.warning(f"遇到速率限制 (429)，等待 {wait_time} 秒后重试... (尝试 {attempt + 1}/{MAX_RETRIES})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"API 速率限制 (429)，已达到最大重试次数")
                        logger.error(f"建议: 1) 申请 API Key: https://www.semanticscholar.org/product/api#api-key-form")
                        logger.error(f"      2) 等待一段时间后再试")
                        raise Exception(f"API 速率限制，请申请 API Key 或稍后重试")
                
                elif response.status_code != 200:
                    logger.error(f"API 返回错误状态码: {response.status_code}")
                    logger.error(f"响应内容: {response.text}")
                    raise Exception(f"API 请求失败，状态码: {response.status_code}, 消息: {response.text}")
            
                # 检查响应大小
                response_size = len(response.content)
                if response_size > MAX_RESPONSE_SIZE:
                    logger.warning(f"响应大小 ({response_size} bytes) 超过 10 MB 限制")
                
                # 解析响应
                papers_data = response.json()
                
                # 构建结果字典，使用 corpusId 作为 key
                result = {}
                for paper in papers_data:
                    if paper and 'corpusId' in paper and paper['corpusId']:
                        corpus_id = str(paper['corpusId'])
                        result[corpus_id] = paper
                
                return result
            
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"网络请求错误: {e}，{RETRY_DELAY} 秒后重试...")
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    logger.error(f"网络请求错误: {e}")
                    raise
            except json.JSONDecodeError as e:
                logger.error(f"API 响应 JSON 解析错误: {e}")
                raise
    
    def merge_paper_data(
        self, 
        original_papers: List[Dict[str, Any]], 
        api_details: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        合并原始论文数据和 API 返回的详细信息
        
        Args:
            original_papers: 原始论文数据列表
            api_details: API 返回的详细信息字典
            
        Returns:
            合并后的论文数据列表
        """
        enhanced_papers = []
        
        for paper in tqdm(original_papers, desc="合并数据", unit="篇"):
            enhanced_paper = paper.copy()
            
            # 获取对应的 API 详细信息
            if 'corpusId' in paper and paper['corpusId']:
                corpus_id = str(paper['corpusId'])
                
                if corpus_id in api_details:
                    api_data = api_details[corpus_id]
                    
                    # 将 API 返回的字段添加到原始数据中（不覆盖已存在的字段）
                    for key, value in api_data.items():
                        if key not in enhanced_paper:
                            enhanced_paper[key] = value
                        elif enhanced_paper[key] is None and value is not None:
                            # 如果原始字段为 None，用 API 数据填充
                            enhanced_paper[key] = value
                else:
                    logger.warning(f"corpusId {corpus_id} 未在 API 响应中找到")
            
            enhanced_papers.append(enhanced_paper)
        
        logger.info(f"数据合并完成，共 {len(enhanced_papers)} 篇论文")
        return enhanced_papers
    
    def save_enhanced_jsonl(
        self, 
        papers: List[Dict[str, Any]], 
        output_path: Path
    ) -> None:
        """
        保存增强后的论文数据为 JSONL 格式
        
        Args:
            papers: 论文数据列表
            output_path: 输出文件路径
        """
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                for paper in papers:
                    json_line = json.dumps(paper, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            logger.info(f"成功保存增强数据到: {output_path}")
            logger.info(f"文件大小: {output_path.stat().st_size / (1024*1024):.2f} MB")
        
        except Exception as e:
            logger.error(f"保存文件失败: {e}")
            raise
    
    def process_single_file(self, gz_path: Path, file_index: int, total_files: int) -> bool:
        """
        处理单个 GZ 文件
        
        Args:
            gz_path: GZ 文件路径
            file_index: 当前文件索引（从 1 开始）
            total_files: 总文件数
            
        Returns:
            是否处理成功
        """
        start_time = time.time()
        logger.info(f"\n{'='*60}")
        logger.info(f"处理文件 {file_index}/{total_files}: {gz_path.name}")
        logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        
        try:
            # 1. 读取并解压 GZ 文件
            logger.info("步骤 1/5: 解压 GZ 文件...")
            papers = self.read_gz_file(gz_path)
            
            if not papers:
                logger.warning(f"文件 {gz_path.name} 中没有论文数据，跳过处理")
                return False
        
        except Exception as e:
            logger.error(f"⚠ 解压文件失败: {e}")
            logger.warning(f"⚠ 跳过损坏的文件: {gz_path.name}")
            return False
        
        # 2. 提取 corpusId
        logger.info("步骤 2/5: 提取 corpusId...")
        corpus_ids = self.extract_corpus_ids(papers)
        
        # 3. 批量查询 API
        logger.info("步骤 3/5: 调用 Semantic Scholar API...")
        api_details = self.fetch_paper_details_batch(corpus_ids)
        
        # 4. 合并数据
        logger.info("步骤 4/5: 合并论文数据...")
        enhanced_papers = self.merge_paper_data(papers, api_details)
        
        # 5. 保存结果
        logger.info("步骤 5/5: 保存增强数据...")
        # 生成输出文件名：enhanced_<原文件名>.jsonl
        original_name = gz_path.stem  # 去除 .gz 后缀
        output_filename = f"{OUTPUT_PREFIX}{original_name}{OUTPUT_EXTENSION}"
        output_path = gz_path.parent / output_filename
        
        self.save_enhanced_jsonl(enhanced_papers, output_path)
        
        # 计算处理时间
        elapsed_time = time.time() - start_time
        logger.info(f"✓ 文件 {gz_path.name} 处理完成！")
        logger.info(f"耗时: {elapsed_time:.2f} 秒 ({elapsed_time/60:.2f} 分钟)")
        return True
    
    def process_all_files(self) -> None:
        """
        处理所有 GZ 文件
        """
        # 扫描所有 GZ 文件
        gz_files = self.scan_gz_files()
        
        if not gz_files:
            logger.warning("未找到 GZ 文件，退出处理")
            return
        
        total_files = len(gz_files)
        total_start_time = time.time()
        logger.info(f"\n开始处理 {total_files} 个 GZ 文件...")
        logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # 统计处理结果
        success_count = 0
        failed_count = 0
        skipped_files = []
        
        # 逐个处理文件
        for idx, gz_path in enumerate(gz_files, 1):
            try:
                success = self.process_single_file(gz_path, idx, total_files)
                if success:
                    success_count += 1
                else:
                    failed_count += 1
                    skipped_files.append(gz_path.name)
            except Exception as e:
                # API 错误等严重错误才停止
                if "API" in str(e) or "速率限制" in str(e):
                    logger.error(f"遇到 API 错误: {e}")
                    logger.error("停止处理以避免进一步的 API 限制")
                    raise
                else:
                    # 其他错误跳过文件
                    logger.error(f"⚠ 处理文件 {gz_path.name} 时发生错误: {e}")
                    logger.warning(f"⚠ 跳过该文件，继续处理下一个")
                    failed_count += 1
                    skipped_files.append(gz_path.name)
        
        # 计算总耗时
        total_elapsed = time.time() - total_start_time
        logger.info(f"\n{'='*60}")
        logger.info(f"批量处理完成！")
        logger.info(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"总耗时: {total_elapsed:.2f} 秒 ({total_elapsed/60:.2f} 分钟)")
        logger.info(f"{'='*60}")
        logger.info(f"处理统计:")
        logger.info(f"  ✓ 成功处理: {success_count} 个文件")
        logger.info(f"  ✗ 跳过失败: {failed_count} 个文件")
        logger.info(f"  📊 总文件数: {total_files} 个")
        
        if success_count > 0:
            avg_time = total_elapsed / total_files
            logger.info(f"  ⏱ 平均每个文件: {avg_time:.2f} 秒")
        
        if skipped_files:
            logger.warning(f"\n跳过的文件列表:")
            for filename in skipped_files:
                logger.warning(f"  - {filename}")
        
        logger.info(f"{'='*60}")


def main():
    """主函数"""
    try:
        # 检查 API Key 配置
        if API_KEY:
            logger.info("✓ 使用 API Key 进行请求（更高速率限制）")
        else:
            logger.warning("⚠ 未配置 API Key，使用公共访问（速率限制较低）")
        
        # 创建处理器（使用配置文件中的目录）
        processor = S2ORCProcessor(CONFIG_INPUT_DIR)
        
        # 处理所有文件
        processor.process_all_files()
        
        logger.info("\n✓ 处理成功完成！")
        
    except KeyboardInterrupt:
        logger.warning("\n用户中断处理")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ 处理失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

