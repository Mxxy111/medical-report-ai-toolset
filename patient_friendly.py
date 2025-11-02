"""将医学报告转换为患者友好的简洁语言。"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import AppConfig, ensure_api_key
from .silicon_client import SiliconClient

# 复用 sync_runner 中的 Record 和相关函数
from .sync_runner import Record, RateLimiter, read_csv_records


@dataclass
class PatientFriendlyResult:
	"""患者友好化结果。"""
	id_value: str
	original_text: str  # 原始报告文本
	simplified_text: str  # 简化后的患者友好文本
	error: Optional[str] = None
	
	def to_dict(self) -> Dict[str, Any]:
		return {
			"id": self.id_value,
			"original_text": self.original_text,
			"simplified_text": self.simplified_text,
			"error": self.error,
		}


# 默认的 system prompt（可以自定义）
DEFAULT_SYSTEM_PROMPT = """你是一名专业的医学翻译助手，任务是将专业的医学影像报告转换为患者容易理解的简洁语言。

要求：
1. 使用通俗易懂的语言，避免专业术语（如必须使用，请简单解释）
2. 保持关键信息的准确性
3. 语言简洁明了，适合普通患者阅读
4. 保持友好的语气
5. 如果报告中有严重异常，请温和但清晰地说明

输出格式：
直接输出简化后的文本，不要添加额外的说明或格式标记。
"""


async def _call_with_retry(
	client: SiliconClient,
	messages: List[Dict[str, Any]],
	config: AppConfig,
	ratelimiter: RateLimiter,
) -> Dict[str, Any]:
	"""调用 API 并重试。"""
	max_retries = config.rate_limit.max_retries
	backoff = config.rate_limit.retry_backoff
	
	for attempt in range(max_retries + 1):
		await ratelimiter.wait()
		try:
			return await asyncio.to_thread(
				client.chat_completion,
				messages=messages,
				temperature=config.response_temperature,
			)
		except Exception as exc:
			error_str = str(exc)
			is_429 = "429" in error_str or "RESOURCE_EXHAUSTED" in error_str or "quota" in error_str.lower()
			
			if attempt >= max_retries:
				raise exc
			
			# 429 错误：延迟更长时间
			if is_429:
				delay = backoff ** attempt * 10
				print(f"⚠️  配额超限，等待 {delay:.1f} 秒后重试（尝试 {attempt + 1}/{max_retries + 1}）...")
			else:
				delay = backoff ** attempt
			
			await asyncio.sleep(delay)


async def process_records(
	records: List[Record],
	client: SiliconClient,
	config: AppConfig,
	system_prompt: str = DEFAULT_SYSTEM_PROMPT,
) -> List[Tuple[Record, Optional[PatientFriendlyResult]]]:
	"""处理记录，转换为患者友好语言。"""
	results: List[Tuple[Record, Optional[PatientFriendlyResult]]] = []
	sem = asyncio.Semaphore(config.rate_limit.concurrency)
	ratelimiter = RateLimiter(config.rate_limit.rpm)
	
	async def worker(record: Record) -> Tuple[Record, Optional[PatientFriendlyResult]]:
		messages = [
			{"role": "system", "content": system_prompt},
			{"role": "user", "content": record.text},
		]
		try:
			async with sem:
				resp = await _call_with_retry(client, messages, config, ratelimiter)
			
			txt = resp["choices"][0]["message"]["content"]
			result = PatientFriendlyResult(
				id_value=record.id_value,
				original_text=record.text,
				simplified_text=txt.strip(),
			)
			return record, result
		except Exception as exc:
			return record, PatientFriendlyResult(
				id_value=record.id_value,
				original_text=record.text,
				simplified_text="",
				error=str(exc),
			)
	
	tasks = [worker(rec) for rec in records]
	for chunk in asyncio.as_completed(tasks):
		result = await chunk
		results.append(result)
	
	return results


def write_outputs(
	results: List[Tuple[Record, Optional[PatientFriendlyResult]]],
	*,
	output_dir: Path,
) -> None:
	"""写入结果到 CSV 和 JSONL。"""
	output_dir.mkdir(parents=True, exist_ok=True)
	jsonl_path = output_dir / "patient_friendly.jsonl"
	csv_path = output_dir / "patient_friendly.csv"
	
	fieldnames = ["id", "original_text", "simplified_text", "error"]
	
	with jsonl_path.open("w", encoding="utf-8") as fj, csv_path.open(
		"w", encoding="utf-8", newline=""
	) as fc:
		writer = csv.DictWriter(fc, fieldnames=fieldnames)
		writer.writeheader()
		
		for record, result in results:
			if result:
				row_dict = result.to_dict()
				json_line = {
					"id": result.id_value,
					"original_text": result.original_text,
					"simplified_text": result.simplified_text,
					"error": result.error,
				}
				fj.write(json.dumps(json_line, ensure_ascii=False) + "\n")
			else:
				row_dict = {
					"id": record.id_value,
					"original_text": record.text,
					"simplified_text": "",
					"error": "处理失败",
				}
			writer.writerow(row_dict)


def run_patient_friendly(
	*,
	input_path: Path,
	config: Optional[AppConfig] = None,
	text_col: Optional[str] = None,
	text_cols: Optional[str] = None,
	id_col: Optional[str] = None,
	limit: Optional[int] = None,
	output_dir: Optional[Path] = None,
	system_prompt: Optional[str] = None,
) -> List[Tuple[Record, Optional[PatientFriendlyResult]]]:
	"""运行患者友好化转换。"""
	app_config = config or AppConfig.from_env()
	records = read_csv_records(
		input_path,
		app_config,
		text_col=text_col,
		text_cols=text_cols,
		id_col=id_col,
		limit=limit,
	)
	
	if not records:
		raise RuntimeError("未读取到任何记录")
	
	print(f"📤 准备处理 {len(records)} 条记录")
	
	client = SiliconClient(app_config)
	prompt = system_prompt or DEFAULT_SYSTEM_PROMPT
	
	results = asyncio.run(process_records(records, client, app_config, prompt))
	
	output_path = output_dir or Path(app_config.output_dir)
	write_outputs(results, output_dir=output_path)
	
	succeeded = sum(1 for _, r in results if r and not r.error)
	print(f"✅ 处理完成：成功 {succeeded}/{len(results)} 条，结果保存至 {output_path}")
	
	return results

