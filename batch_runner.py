"""SiliconFlow Batch 工作流：生成 JSONL、提交任务并合并结果。"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .config import AppConfig
from .prompt import build_system_prompt
from .schema import ExtractionResult, flatten_for_csv, normalise_extraction
from .silicon_client import SiliconClient
from .sync_runner import Record, read_csv_records, write_outputs


def build_batch_payload(record: Record, config: AppConfig, system_prompt: str, *, custom_id: str | None = None) -> Dict[str, object]:
	return {
		"custom_id": custom_id or record.id_value,
		"method": "POST",
		"url": "/v1/chat/completions",
		"body": {
			"model": config.model,
			"messages": [
				{"role": "system", "content": system_prompt},
				{"role": "user", "content": record.text},
			],
			"temperature": config.response_temperature,
			"response_format": {"type": "json_object"},
		},
	}


def write_request_jsonl(
	records: List[Record],
	output_dir: Path,
	config: AppConfig,
	filename: str = "batch_requests.jsonl",
) -> Tuple[Path, Dict[str, Record]]:
	output_dir.mkdir(parents=True, exist_ok=True)
	path = output_dir / filename
	system_prompt = build_system_prompt(config.template_id)
	# Batch 要求 custom_id 全局唯一；为重复 ID 增加序号后缀
	seen_counts: dict[str, int] = {}
	id_map: Dict[str, Record] = {}
	with path.open("w", encoding="utf-8") as fh:
		for record in records:
			base_id = record.id_value or "row"
			count = seen_counts.get(base_id, 0)
			seen_counts[base_id] = count + 1
			unique_id = base_id if count == 0 else f"{base_id}__{count+1}"
			id_map[unique_id] = record
			payload = build_batch_payload(record, config, system_prompt, custom_id=unique_id)
			fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
	return path, id_map


def poll_batch(client: SiliconClient, batch_id: str, interval: float = 10.0) -> Dict[str, object]:
	"""轮询批量任务状态，直到完成或失败。"""
	poll_count = 0
	while True:
		status = client.retrieve_batch(batch_id)
		state = status.get("status")
		poll_count += 1
		
		if state == "completed":
			print(f"✅ 批量任务完成！")
			return status
		elif state in {"failed", "cancelled"}:
			print(f"❌ 批量任务状态：{state}")
			return status
		else:
			# 显示进度（每 3 次轮询显示一次，避免刷屏）
			if poll_count % 3 == 1:
				print(f"⏳ 批量任务处理中... (状态: {state}, 已轮询: {poll_count} 次)")
		time.sleep(interval)



def parse_batch_results(
	records: List[Record],
	id_map: Dict[str, Record],
	result_bytes: bytes,
	template_id: str = "rcc",
) -> List[Tuple[Record, Optional[ExtractionResult], Optional[str]]]:
	results_dict: Dict[str, Tuple[Record, Optional[ExtractionResult], Optional[str]]] = {}
	lines = result_bytes.decode("utf-8").splitlines()
	for line in lines:
		if not line.strip():
			continue
		entry = json.loads(line)
		custom_id = entry.get("custom_id")
		record = id_map.get(custom_id)
		if not record:
			continue
		response = entry.get("response")
		error = entry.get("error")
		if response and not error:
			try:
				output = response["body"]["choices"][0]["message"]["content"]
				data = json.loads(output)
				data.setdefault("id_value", record.id_value)
				extraction = normalise_extraction(data, template_id)
				extraction.id_value = record.id_value
				results_dict[record.id_value] = (record, extraction, None)
			except Exception as exc:  # pylint: disable=broad-except
				results_dict[record.id_value] = (record, None, str(exc))
		else:
			error_msg = str(error) if error else "未知错误"
			results_dict[record.id_value] = (record, None, error_msg)

	ordered: List[Tuple[Record, Optional[ExtractionResult], Optional[str]]] = []
	for record in records:
		ordered.append(
			results_dict.get(record.id_value, (record, None, "批量任务无返回"))
		)
	return ordered


def run_batch(
	*,
	input_path: Path,
	config: Optional[AppConfig] = None,
	text_col: Optional[str] = None,
	text_cols: Optional[str] = None,  # 新增：多列模式
	id_col: Optional[str] = None,
	limit: Optional[int] = None,
	output_dir: Optional[Path] = None,
	request_dir: Optional[Path] = None,
	poll_interval: float = 15.0,
) -> Dict[str, object]:
	app_config = config or AppConfig.from_env()
	provider = app_config.get_provider()
	
	# 检查批量推理支持
	if provider not in {"siliconflow", "aliyun", "openai"}:
		raise RuntimeError(
			f"批量推理模式当前仅支持 siliconflow、aliyun、openai。"
			f"当前供应商：{provider or 'custom'}。"
			f"请使用 --mode sync 进行同步模式处理。"
		)
	
	records = read_csv_records(
		input_path,
		app_config,
		text_col=text_col,
		text_cols=text_cols,
		id_col=id_col,
		limit=limit,
	)
	
	if not records:
		raise RuntimeError("未读取到任何记录，请检查 CSV 文件和列名配置")
	
	print(f"📤 准备批量处理 {len(records)} 条记录，供应商：{provider or 'custom'}")
	
	client = SiliconClient(app_config)
	request_dir = request_dir or Path("inputs")
	request_path, id_map = write_request_jsonl(records, request_dir, app_config)
	
	print(f"📁 已生成批量请求文件：{request_path}")
	print(f"⬆️  正在上传文件到 {provider or 'API'}...")
	
	file_info = client.upload_jsonl(path=str(request_path))
	
	# 兼容不同返回结构，确保拿到文件ID
	file_id = (
		(file_info.get("id") if isinstance(file_info, dict) else None)
		or (file_info.get("data", {}).get("id") if isinstance(file_info, dict) else None)
	)
	if not file_id:
		raise RuntimeError(f"文件上传成功但未获取到文件ID，返回：{file_info}")
	
	print(f"✅ 文件上传成功，ID：{file_id}")
	print(f"🚀 正在创建批量任务...")
	
	# 根据供应商调整批量任务创建参数
	if provider == "aliyun":
		# 阿里云百炼：直接使用模型参数，不需要 extra_body
		batch = client.create_batch(
			input_file_id=file_id,
			extra_body=None,  # 阿里云在 body 中已经指定了 model
		)
	else:
		# SiliconFlow/OpenAI：使用 extra_body 覆盖模型
		batch = client.create_batch(
			input_file_id=file_id,
			extra_body={"replace": {"model": app_config.model}},
		)
	batch_id = batch["id"]
	print(f"📋 批量任务 ID：{batch_id}")
	print(f"⏱️  开始轮询任务状态（间隔：{poll_interval} 秒）...")
	
	status = poll_batch(client, batch_id, interval=poll_interval)
	
	if status.get("status") != "completed":
		return {"batch": batch, "status": status, "results": None}
	
	print(f"📥 正在下载结果文件...")
	output_file_id = status.get("output_file_id")
	if not output_file_id:
		for attempt in range(6):
			time.sleep(2.0 * (attempt + 1))
			refreshed = client.retrieve_batch(batch_id)
			output_file_id = refreshed.get("output_file_id")
			if output_file_id:
				status = refreshed
				break
	if not output_file_id:
		raise RuntimeError("批量任务已完成但尚未生成结果文件，请稍后重试")
	
	# 下载结果：即便 completed，也可能短暂 404，增加重试
	for attempt in range(8):
		try:
			result_bytes = client.download_file(output_file_id)
			break
		except Exception:
			time.sleep(2.0 * (attempt + 1))
	else:
		raise RuntimeError("结果文件暂不可用，请稍后重试下载")
	
	print(f"✅ 结果文件下载成功，正在解析...")
	parsed = parse_batch_results(records, id_map, result_bytes, template_id=app_config.template_id)
	
	output_path = output_dir or Path(app_config.output_dir)
	write_outputs(parsed, output_dir=output_path, config=app_config)
	
	succeeded = sum(1 for _, extraction, err in parsed if extraction and not err)
	print(f"✨ 批量处理完成：成功 {succeeded}/{len(parsed)} 条，结果保存至 {output_path}")
	
	return {"batch": batch, "status": status, "results": parsed}


