"""将 CSV 文件转换为微调数据集格式（JSONL）。

简单版本：直接从 CSV 列生成 user 和 assistant 内容。
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import List, Optional


def merge_columns(row: dict, columns: List[str], add_label: bool = True) -> str:
	"""合并多列内容。"""
	if not columns:
		return ""
	
	parts: List[str] = []
	for col in columns:
		value = (row.get(col, "") or "").strip()
		if value:
			if len(columns) > 1 and add_label:
				parts.append(f"[{col}]\n{value}")
			else:
				parts.append(value)
	
	return "\n\n".join(parts)


def convert_csv_to_jsonl(
	input_csv: Path,
	output_jsonl: Path,
	*,
	system_content: str,
	user_columns: List[str],
	assistant_columns: Optional[List[str]] = None,
	encoding: str = "utf-8-sig",
) -> None:
	"""将 CSV 转换为微调数据集 JSONL 格式。"""
	
	if not input_csv.exists():
		raise FileNotFoundError(f"文件不存在：{input_csv}")
	
	print(f"📖 读取 CSV 文件：{input_csv}")
	
	with open(input_csv, "r", encoding=encoding, newline="") as fh:
		reader = csv.DictReader(fh)
		if reader.fieldnames is None:
			raise ValueError("CSV 未包含表头")
		
		columns = list(reader.fieldnames)
		
		# 验证列是否存在
		missing_user = [col for col in user_columns if col not in columns]
		if missing_user:
			raise ValueError(f"User 列不存在：{missing_user}。可用列：{columns}")
		
		if assistant_columns:
			missing_assistant = [col for col in assistant_columns if col not in columns]
			if missing_assistant:
				raise ValueError(
					f"Assistant 列不存在：{missing_assistant}。可用列：{columns}"
				)
		
		# 转换并写入 JSONL
		print(f"📝 正在转换并写入：{output_jsonl}")
		count = 0
		
		with open(output_jsonl, "w", encoding="utf-8") as out_fh:
			for row in reader:
				# 合并 user 列
				user_content = merge_columns(row, user_columns, add_label=True)
				
				if not user_content.strip():
					continue  # 跳过空的 user 内容
				
				# 构建 messages
				messages: List[dict] = [
					{"role": "system", "content": system_content},
					{"role": "user", "content": user_content},
				]
				
				# 添加 assistant（如果有）
				if assistant_columns:
					assistant_content = merge_columns(
						row, assistant_columns, add_label=False
					)
					if assistant_content.strip():
						messages.append({"role": "assistant", "content": assistant_content})
				
				# 写入 JSONL
				line = json.dumps({"messages": messages}, ensure_ascii=False)
				out_fh.write(line + "\n")
				count += 1
		
		print(f"✅ 转换完成！共生成 {count} 条记录")
		print(f"   - 输出文件：{output_jsonl}")


def main():
	parser = argparse.ArgumentParser(
		description="将 CSV 文件转换为微调数据集格式（JSONL）",
		formatter_class=argparse.RawDescriptionHelpFormatter,
		epilog="""
示例：
  # 基本用法：system + user（无 assistant）
  python convert_to_finetune.py \\
    --input data.csv \\
    --system "你是一名医学影像分析助手" \\
    --user-col "检查所见"
  
  # 包含 assistant（有监督微调）
  python convert_to_finetune.py \\
    --input data.csv \\
    --system "你是一名医学影像分析助手" \\
    --user-col "检查所见" \\
    --assistant-col "提取结果"
  
  # 多列 user（合并）
  python convert_to_finetune.py \\
    --input data.csv \\
    --system "你是一名医学影像分析助手" \\
    --user-cols "检查所见,检查结论,诊断建议"
  
  # 多列 assistant（合并）
  python convert_to_finetune.py \\
    --input data.csv \\
    --system "你是一名医学影像分析助手" \\
    --user-col "检查所见" \\
    --assistant-cols "字段1,字段2,字段3"
		""",
	)
	
	parser.add_argument("--input", required=True, help="输入 CSV 文件路径")
	parser.add_argument(
		"--output", help="输出 JSONL 文件路径（默认：输入文件名.jsonl）"
	)
	parser.add_argument(
		"--system",
		required=True,
		help="System prompt 内容（A）",
	)
	parser.add_argument(
		"--user-col",
		help="User 内容列名（单列，B）。与 --user-cols 二选一",
	)
	parser.add_argument(
		"--user-cols",
		help="User 内容列名（多列，逗号分隔，B）。与 --user-col 二选一",
	)
	parser.add_argument(
		"--assistant-col",
		help="Assistant 内容列名（单列，C）。与 --assistant-cols 二选一",
	)
	parser.add_argument(
		"--assistant-cols",
		help="Assistant 内容列名（多列，逗号分隔，C）。与 --assistant-cols 二选一",
	)
	parser.add_argument(
		"--encoding",
		default="utf-8-sig",
		help="CSV 文件编码（默认：utf-8-sig）",
	)
	
	args = parser.parse_args()
	
	# 验证参数
	if args.user_col and args.user_cols:
		raise ValueError("不能同时使用 --user-col 和 --user-cols，请二选一")
	
	if not args.user_col and not args.user_cols:
		raise ValueError("必须指定 --user-col 或 --user-cols")
	
	if args.assistant_col and args.assistant_cols:
		raise ValueError(
			"不能同时使用 --assistant-col 和 --assistant-cols，请二选一"
		)
	
	# 确定列名
	if args.user_cols:
		user_columns = [col.strip() for col in args.user_cols.split(",") if col.strip()]
	else:
		user_columns = [args.user_col]
	
	if args.assistant_cols:
		assistant_columns = [
			col.strip() for col in args.assistant_cols.split(",") if col.strip()
		]
	elif args.assistant_col:
		assistant_columns = [args.assistant_col]
	else:
		assistant_columns = None
	
	# 确定输出路径
	input_path = Path(args.input)
	if args.output:
		output_path = Path(args.output)
	else:
		output_path = input_path.with_suffix(".jsonl")
	
	# 转换
	convert_csv_to_jsonl(
		input_path,
		output_path,
		system_content=args.system,
		user_columns=user_columns,
		assistant_columns=assistant_columns,
		encoding=args.encoding,
	)


if __name__ == "__main__":
	main()
