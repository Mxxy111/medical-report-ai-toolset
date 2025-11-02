"""同步方式逐条调用 SiliconFlow 接口并抽取特征。"""

from __future__ import annotations

import asyncio
import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .config import AppConfig
from .prompt import build_system_prompt
from .schema import ExtractionResult, flatten_for_csv, normalise_extraction
from .silicon_client import SiliconClient


@dataclass
class Record:
	id_value: str
	text: str
	row_data: Dict[str, Any]


def _auto_detect_column(
	columns: Iterable[str],
	candidates: Iterable[str],
	keywords: Tuple[str, ...],
) -> Optional[str]:
	col_list = list(columns)
	for cand in candidates:
		if cand in col_list:
			return cand
	lower_map = {col.lower(): col for col in col_list}
	for col_lower, original in lower_map.items():
		if all(keyword in col_lower for keyword in keywords):
			return original
	return None


def read_csv_records(
	path: Path,
	config: AppConfig,
	*,
	text_col: Optional[str] = None,
	text_cols: Optional[str] = None,  # 新增：多列模式
	id_col: Optional[str] = None,
	limit: Optional[int] = None,
	encoding: Optional[str] = None,
) -> List[Record]:
	if not path.exists():
		raise FileNotFoundError(path)
	encoding = encoding or config.csv.default_encoding
	with open(path, "r", encoding=encoding, newline="") as fh:
		reader = csv.DictReader(fh)
		if reader.fieldnames is None:
			raise ValueError("CSV 未包含表头")
		columns = reader.fieldnames
		
		# 处理多列模式
		text_columns: List[str] = []
		if text_cols:
			# 多列模式：用逗号分隔
			text_columns = [col.strip() for col in text_cols.split(",") if col.strip()]
			# 验证所有列都存在
			missing = [col for col in text_columns if col not in columns]
			if missing:
				raise ValueError(f"指定的列不存在：{missing}。可用列：{list(columns)}")
		elif text_col:
			# 单列模式
			if text_col not in columns:
				raise ValueError(f"列 '{text_col}' 不存在。可用列：{list(columns)}")
			text_columns = [text_col]
		else:
			# 自动检测模式（只检测单列）
			detected = _auto_detect_column(
				columns,
				config.csv.text_column_candidates,
				("查", "见"),
			)
			if not detected:
				raise ValueError("未找到\"检查所见\"列，请使用 --text-col 或 --text-cols 指定")
			text_columns = [detected]
		
		# 处理 ID 列
		id_column = id_col or _auto_detect_column(
			columns,
			config.csv.id_column_candidates,
			("号",),
		)
		if not id_column:
			id_column = columns[0]

		records: List[Record] = []
		for row in reader:
			# 合并多列文本
			text_parts: List[str] = []
			for col in text_columns:
				value = (row.get(col) or "").strip()
				if value:
					# 为每列添加标签，便于 AI 理解
					if len(text_columns) > 1:
						text_parts.append(f"[{col}]\n{value}")
					else:
						text_parts.append(value)
			
			text = "\n\n".join(text_parts) if text_parts else ""
			if not text:
				continue
			if len(text) > config.csv.max_chars:
				text = text[: config.csv.max_chars]
			records.append(
				Record(
					id_value=str(row.get(id_column, "")).strip() or f"row_{len(records)+1}",
					text=text,
					row_data=row,
				)
			)
			if limit is not None and len(records) >= limit:
				break
		return records


class RateLimiter:
	def __init__(self, rpm: int) -> None:
		self.interval = 60.0 / max(rpm, 1)
		self._lock = asyncio.Lock()
		self._next_slot = time.monotonic()

	async def wait(self) -> None:
		async with self._lock:
			now = time.monotonic()
			if now < self._next_slot:
				await asyncio.sleep(self._next_slot - now)
			self._next_slot = max(now, self._next_slot) + self.interval


def _extract_429_retry_delay(error_msg: str) -> Optional[float]:
	"""从 429 错误消息中提取建议的延迟时间（秒）。"""
	import re
	# 匹配 "Please retry in 38.36s" 或 "retryDelay": "44s" 等
	patterns = [
		r'retry in ([\d.]+)\s*s',
		r'retryDelay["\']?\s*:\s*["\']?(\d+)',
		r'retryDelay["\']?\s*:\s*["\']?([\d.]+)\s*s',
	]
	for pattern in patterns:
		match = re.search(pattern, error_msg, re.IGNORECASE)
		if match:
			try:
				return float(match.group(1))
			except (ValueError, IndexError):
				continue
	return None


async def _call_with_retry(
	client: SiliconClient,
	messages: List[Dict[str, Any]],
	config: AppConfig,
	ratelimiter: RateLimiter,
) -> Dict[str, Any]:
	max_retries = config.rate_limit.max_retries
	backoff = config.rate_limit.retry_backoff
	for attempt in range(max_retries + 1):
		await ratelimiter.wait()
		try:
			return await asyncio.to_thread(
				client.chat_completion,
				messages=messages,
				response_format={"type": "json_object"},
			)
		except Exception as exc:  # pylint: disable=broad-except
			error_str = str(exc)
			is_429 = "429" in error_str or "RESOURCE_EXHAUSTED" in error_str or "quota" in error_str.lower()
			
			if attempt >= max_retries:
				raise exc
			
			# 429 错误：使用 API 建议的延迟，或指数退避，取较大值
			if is_429:
				suggested_delay = _extract_429_retry_delay(error_str)
				if suggested_delay:
					# API 建议的延迟 + 一些缓冲（避免边界情况）
					delay = min(suggested_delay + 2.0, 120.0)  # 最多等待 2 分钟
					print(f"⚠️  配额超限，等待 {delay:.1f} 秒后重试（尝试 {attempt + 1}/{max_retries + 1}）...")
				else:
					# 没有找到建议延迟，使用指数退避
					delay = backoff ** attempt * 10  # 429 错误延迟更长
					print(f"⚠️  配额超限，等待 {delay:.1f} 秒后重试（尝试 {attempt + 1}/{max_retries + 1}）...")
			else:
				# 其他错误：正常指数退避
				delay = backoff ** attempt
			
			await asyncio.sleep(delay)


async def process_records(
	records: List[Record],
	client: SiliconClient,
	config: AppConfig,
) -> List[Tuple[Record, Optional[ExtractionResult], Optional[str]]]:
	reluts: List[Tuple[Record, Optional[ExtractionResult], Optional[str]]] = []
	sem = asyncio.Semaphore(config.rate_limit.concurrency)
	ratelimiter = RateLimiter(config.rate_limit.rpm)
	system_prompt = build_system_prompt(config.template_id)

	async def worker(record: Record) -> Tuple[Record, Optional[ExtractionResult], Optional[str]]:
		messages = [
			{"role": "system", "content": system_prompt},
			{"role": "user", "content": record.text},
		]
		try:
			async with sem:
				resp = await _call_with_retry(client, messages, config, ratelimiter)
			txt = resp["choices"][0]["message"]["content"]
			data = json.loads(txt)
			data.setdefault("id_value", record.id_value)
			result = normalise_extraction(data, config.template_id)
			result.id_value = record.id_value
			return record, result, None
		except Exception as exc:  # pylint: disable=broad-except
			return record, None, str(exc)

	tasks = [worker(rec) for rec in records]
	for chunk in asyncio.as_completed(tasks):
		result = await chunk
		reluts.append(result)
	return reluts


def write_outputs(
	results: List[Tuple[Record, Optional[ExtractionResult], Optional[str]]],
	*,
	output_dir: Path,
	config: AppConfig,
) -> None:
	output_dir.mkdir(parents=True, exist_ok=True)
	jsonl_path = output_dir / "rcc_extractions.jsonl"
	csv_path = output_dir / "rcc_extractions.csv"
	
	# 动态获取字段名（从模板）
	from .templates import get_template
	template = get_template(config.template_id)
	fieldnames = ["id"] + [f.name for f in template.fields] + ["error"]

	with jsonl_path.open("w", encoding="utf-8") as fj, csv_path.open(
		"w", encoding="utf-8", newline=""
	) as fc:
		writer = csv.DictWriter(fc, fieldnames=fieldnames)
		writer.writeheader()
		for record, extraction, error in results:
			row_dict: Dict[str, Any] = {
				"id": record.id_value,
				"error": error or "",
			}
			payload: Dict[str, Any]
			if extraction:
				payload = extraction.to_dict()
				json_line = {
					"id": record.id_value,
					"raw_text": record.text,
					"extraction": payload,
					"error": error,
				}
				fj.write(json.dumps(json_line, ensure_ascii=False) + "\n")
				flat = flatten_for_csv(extraction)
				row_dict.update(flat)
			else:
				json_line = {
					"id": record.id_value,
					"raw_text": record.text,
					"extraction": None,
					"error": error,
				}
				fj.write(json.dumps(json_line, ensure_ascii=False) + "\n")
			writer.writerow(row_dict)


def run_sync(
	*,
	input_path: Path,
	config: Optional[AppConfig] = None,
	text_col: Optional[str] = None,
	text_cols: Optional[str] = None,  # 新增：多列模式
	id_col: Optional[str] = None,
	limit: Optional[int] = None,
	output_dir: Optional[Path] = None,
) -> List[Tuple[Record, Optional[ExtractionResult], Optional[str]]]:
	app_config = config or AppConfig.from_env()
	records = read_csv_records(
		input_path,
		app_config,
		text_col=text_col,
		text_cols=text_cols,
		id_col=id_col,
		limit=limit,
	)
	client = SiliconClient(app_config)
	results = asyncio.run(process_records(records, client, app_config))
	write_outputs(results, output_dir=(output_dir or Path(app_config.output_dir)), config=app_config)
	return results


