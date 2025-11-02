"""患者友好化 CLI 入口。"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from rcc_extract.config import AppConfig
from rcc_extract.patient_friendly import run_patient_friendly


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		prog="patient-friendly",
		description="将医学报告转换为患者友好的简洁语言",
		formatter_class=argparse.RawDescriptionHelpFormatter,
		epilog="""
示例：
  # 基本用法
  python patient_friendly_cli.py --input "工作簿2.csv" --text-col "检查所见" --id-col "门诊号/住院号"
  
  # 使用阿里云 API
  python patient_friendly_cli.py --provider aliyun --model "qwen-plus" --input "数据.csv" --text-col "检查所见"
  
  # 自定义 system prompt
  python patient_friendly_cli.py --input "数据.csv" --text-col "检查所见" --system-prompt "你是一名医学翻译助手..."
		""",
	)
	
	parser.add_argument("--input", required=True, help="输入 CSV 文件路径")
	parser.add_argument(
		"--output",
		help="输出目录（默认：patient_friendly_outputs/输入文件名_时间戳）",
	)
	parser.add_argument(
		"--text-col",
		help="文本列名（单列模式）。与 --text-cols 二选一",
	)
	parser.add_argument(
		"--text-cols",
		help="文本列名（多列模式，用逗号分隔）",
	)
	parser.add_argument("--id-col", help="ID 列名（默认：自动检测）")
	parser.add_argument(
		"--provider",
		choices=["siliconflow", "aliyun", "openai", "deepseek", "qianduoduo", "custom"],
		help="API 供应商",
	)
	parser.add_argument("--api-url", help="自定义 API Base URL")
	parser.add_argument("--api-key", help="API Key")
	parser.add_argument("--model", help="模型名称/ID")
	parser.add_argument("--temperature", type=float, help="响应 temperature（默认：0.3，更自然）")
	parser.add_argument("--rpm", type=int, help="每分钟请求数限制")
	parser.add_argument("--concurrency", type=int, help="并发请求数")
	parser.add_argument("--timeout", type=float, help="单次请求超时（秒）")
	parser.add_argument("--max-retries", type=int, help="最大重试次数")
	parser.add_argument(
		"--system-prompt",
		help="自定义 system prompt（可选，使用默认提示词）",
	)
	parser.add_argument(
		"--system-prompt-file",
		help="从文件读取 system prompt",
	)
	parser.add_argument("--limit", type=int, help="仅处理前 N 条记录")
	parser.add_argument(
		"--encoding",
		default="utf-8-sig",
		help="CSV 文件编码（默认：utf-8-sig）",
	)
	
	return parser.parse_args(argv)


def apply_overrides(config: AppConfig, args: argparse.Namespace) -> None:
	"""应用命令行参数覆盖配置。"""
	if args.api_url:
		config.base_url = args.api_url
	if args.api_key:
		config.api_key = args.api_key
	if args.model:
		config.model = args.model
	if args.temperature is not None:
		config.response_temperature = args.temperature
	else:
		# 患者友好化默认使用稍高的 temperature，让输出更自然
		config.response_temperature = 0.3
	if args.rpm is not None:
		config.rate_limit.rpm = args.rpm
	if args.concurrency is not None:
		config.rate_limit.concurrency = args.concurrency
	if args.timeout is not None:
		config.rate_limit.timeout = args.timeout
	if args.max_retries is not None:
		config.rate_limit.max_retries = args.max_retries


def main(argv: Optional[list[str]] = None) -> None:
	args = parse_args(argv)
	
	# 验证参数
	if args.text_col and args.text_cols:
		raise ValueError("不能同时使用 --text-col 和 --text-cols，请二选一")
	
	if not args.text_col and not args.text_cols:
		# 尝试自动检测
		pass
	
	if args.system_prompt and args.system_prompt_file:
		raise ValueError("不能同时使用 --system-prompt 和 --system-prompt-file")
	
	# 加载配置
	config = AppConfig.from_env(provider=args.provider if args.provider else None)
	apply_overrides(config, args)
	
	# 处理 system prompt
	system_prompt = None
	if args.system_prompt_file:
		prompt_path = Path(args.system_prompt_file)
		if not prompt_path.exists():
			raise FileNotFoundError(f"提示词文件不存在：{prompt_path}")
		system_prompt = prompt_path.read_text(encoding="utf-8").strip()
	elif args.system_prompt:
		system_prompt = args.system_prompt
	
	# 确定输出目录
	input_path = Path(args.input)
	if args.output:
		output_dir = Path(args.output)
		if output_dir.exists():
			# 如果已存在，添加时间戳
			timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
			output_dir = Path(str(output_dir) + f"_{timestamp}")
	else:
		# 默认输出目录
		timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
		output_dir = Path("patient_friendly_outputs") / f"{input_path.stem}_{timestamp}"
	
	# 运行转换
	results = run_patient_friendly(
		input_path=input_path,
		config=config,
		text_col=args.text_col,
		text_cols=args.text_cols,
		id_col=args.id_col,
		limit=args.limit,
		output_dir=output_dir,
		system_prompt=system_prompt,
	)
	
	print(f"\n✨ 完成！结果已保存至：{output_dir}")
	print(f"   - CSV: {output_dir / 'patient_friendly.csv'}")
	print(f"   - JSONL: {output_dir / 'patient_friendly.jsonl'}")


if __name__ == "__main__":
	main()

