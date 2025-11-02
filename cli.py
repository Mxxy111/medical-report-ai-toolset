"""命令行入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import Optional

from .batch_runner import run_batch
from .config import AppConfig
from .sync_runner import read_csv_records, run_sync


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		prog="rcc-extract",
		description="肾癌核医学报告结构化特征抽取工具",
	)
	parser.add_argument(
		"--mode",
		choices=["sync", "batch"],
		default="sync",
		help="执行模式：sync 逐条请求；batch 批量任务",
	)
	parser.add_argument("--input", required=True, help="输入 CSV 路径")
	parser.add_argument(
		"--text-col",
		help="文本列名（单列模式）。与 --text-cols 二选一",
	)
	parser.add_argument(
		"--text-cols",
		help="文本列名（多列模式，用逗号分隔，如'列1,列2,列3'）。多列会自动合并成一个文本发送给 AI",
	)
	parser.add_argument("--id-col", help="病例 ID 列名")
	parser.add_argument(
		"--template",
		choices=["rcc", "lung_cancer", "generic"],
		help="提取模板 (rcc=肾癌/lung_cancer=肺癌/generic=通用)",
	)
	parser.add_argument(
		"--provider",
		choices=["siliconflow", "aliyun", "openai", "deepseek", "qianduoduo", "custom"],
		help="API 供应商 (siliconflow/aliyun/openai/deepseek/qianduoduo/custom)",
	)
	parser.add_argument("--api-url", help="自定义 API Base URL")
	parser.add_argument("--api-key", help="API Key (也可通过环境变量设置)")
	parser.add_argument("--model", help="模型名称/ID")
	parser.add_argument(
		"--temperature", type=float, help="响应 temperature (默认0)"
	)
	parser.add_argument("--rpm", type=int, help="每分钟请求数限制")
	parser.add_argument("--concurrency", type=int, help="并发请求数")
	parser.add_argument("--timeout", type=float, help="单次请求超时（秒）")
	parser.add_argument("--max-retries", type=int, help="最大重试次数")
	parser.add_argument("--retry-backoff", type=float, help="指数退避基数")
	parser.add_argument("--max-chars", type=int, help="单条文本最大长度")
	parser.add_argument("--limit", type=int, help="仅处理前 N 条记录")
	parser.add_argument("--out", help="输出目录")
	parser.add_argument("--request-dir", help="Batch 请求 JSONL 输出目录")
	parser.add_argument(
		"--poll-interval",
		type=float,
		default=15.0,
		help="Batch 状态轮询间隔 (秒)",
	)
	parser.add_argument(
		"--dry-run",
		action="store_true",
		help="仅解析 CSV 并预览， 不发起 API 调用",
	)
	return parser.parse_args(argv)


def apply_overrides(config: AppConfig, args: argparse.Namespace) -> None:
	if hasattr(args, 'template') and args.template:
		config.template_id = args.template
	if args.api_url:
		config.base_url = args.api_url
	if args.api_key:
		config.api_key = args.api_key
	if args.model:
		config.model = args.model
	if args.temperature is not None:
		config.response_temperature = args.temperature
	if args.rpm is not None:
		config.rate_limit.rpm = args.rpm
	if args.concurrency is not None:
		config.rate_limit.concurrency = args.concurrency
	if args.timeout is not None:
		config.rate_limit.timeout = args.timeout
	if args.max_retries is not None:
		config.rate_limit.max_retries = args.max_retries
	if args.retry_backoff is not None:
		config.rate_limit.retry_backoff = args.retry_backoff
	if args.max_chars is not None:
		config.csv.max_chars = args.max_chars
	if args.out:
		config.output_dir = args.out



def main(argv: Optional[list[str]] = None) -> None:
	args = parse_args(argv)
	
	# 验证参数：text_col 和 text_cols 不能同时使用
	if hasattr(args, 'text_col') and hasattr(args, 'text_cols'):
		if args.text_col and args.text_cols:
			raise ValueError("不能同时使用 --text-col 和 --text-cols，请二选一")
	
	# 根据 provider 参数加载配置
	config = AppConfig.from_env(provider=args.provider if hasattr(args, 'provider') else None)
	apply_overrides(config, args)
	input_path = Path(args.input)
	# 自动生成带时间戳的输出目录：
	# 1) 未指定 --out -> outputs/<输入文件名stem>_<YYYYMMDD_HHMMSS>
	# 2) 指定了 --out，但目录已存在 -> <out>_<YYYYMMDD_HHMMSS>
	# 3) 指定了 --out 且目录不存在 -> 按指定名
	base_out = Path(config.output_dir) if config.output_dir else Path("outputs")
	stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
	if args.out:
		candidate = base_out
		if candidate.exists():
			output_dir = candidate.parent / f"{candidate.name}_{stamp}"
		else:
			output_dir = candidate
	else:
		stem = input_path.stem
		output_dir = Path("outputs") / f"{stem}_{stamp}"
	if args.dry_run:
		records = read_csv_records(
			input_path,
			config,
			text_col=args.text_col,
			text_cols=args.text_cols if hasattr(args, 'text_cols') else None,
			id_col=args.id_col,
			limit=args.limit,
		)
		preview = [
			{"id": record.id_value, "text": record.text[:120]} for record in records[:5]
		]
		print(
			json.dumps(
				{
					"total_records": len(records),
					"preview": preview,
					"output_dir": str(output_dir),
				},
				ensure_ascii=False,
				indent=2,
			)
		)
		return

	if args.mode == "sync":
		results = run_sync(
			input_path=input_path,
			config=config,
			text_col=args.text_col,
			text_cols=args.text_cols if hasattr(args, 'text_cols') else None,
			id_col=args.id_col,
			limit=args.limit,
			output_dir=output_dir,
		)
		print(f"同步模式完成，共处理 {len(results)} 条记录，结果输出至 {output_dir}")
	else:
		batch_info = run_batch(
			input_path=input_path,
			config=config,
			text_col=args.text_col,
			text_cols=args.text_cols if hasattr(args, 'text_cols') else None,
			id_col=args.id_col,
			limit=args.limit,
			output_dir=output_dir,
			request_dir=Path(args.request_dir) if args.request_dir else None,
			poll_interval=args.poll_interval,
		)
		results = batch_info.get("results")
		if results is None:
			print(
				json.dumps(
					{
						"batch": batch_info.get("batch"),
						"status": batch_info.get("status"),
					},
					ensure_ascii=False,
					indent=2,
				)
			)
		else:
			total = len(results)
			succeeded = sum(1 for _, extraction, err in results if extraction and not err)
			errors = [
				{"id": rec.id_value, "error": err}
				for rec, extraction, err in results
				if err or not extraction
			]
			print(
				json.dumps(
					{
						"batch_id": batch_info.get("batch", {}).get("id"),
						"status": batch_info.get("status", {}).get("status"),
						"total": total,
						"succeeded": succeeded,
						"failed": total - succeeded,
						"output_dir": str(output_dir),
						"errors": errors[:20],
					},
					ensure_ascii=False,
					indent=2,
				)
			)


if __name__ == "__main__":
	main()


