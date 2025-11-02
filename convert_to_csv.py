import os
import sys


def convert_excel_to_csv(excel_path: str, csv_path: str) -> None:
	"""Convert the first sheet of an Excel workbook to CSV (UTF-8 with BOM)."""
	try:
		import pandas as pd
	except ImportError as exc:
		raise SystemExit(
			"需要安装 pandas：pip install pandas openpyxl"
		) from exc

	if not os.path.isfile(excel_path):
		raise FileNotFoundError(f"未找到文件: {excel_path}")

	# 读取第一个工作表
	df = pd.read_excel(excel_path, sheet_name=0, engine="openpyxl")

	# 导出 CSV（带 BOM，便于在 Excel/记事本下直接显示中文）
	df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def read_csv_preview(csv_path: str, max_rows: int = 10) -> str:
	"""Read the CSV and return a short preview string."""
	try:
		import pandas as pd
	except ImportError as exc:
		raise SystemExit(
			"需要安装 pandas：pip install pandas"
		) from exc

	if not os.path.isfile(csv_path):
		raise FileNotFoundError(f"未找到 CSV 文件: {csv_path}")

	df = pd.read_csv(csv_path, encoding="utf-8-sig")
	# 预览前 max_rows 行
	preview = df.head(max_rows)
	return preview.to_string(index=False)


def main() -> None:
	# 默认与脚本同目录下的工作簿名
	default_excel = "工作簿2.xlsx"
	excel_path = sys.argv[1] if len(sys.argv) > 1 else default_excel
	csv_path = os.path.splitext(excel_path)[0] + ".csv"

	convert_excel_to_csv(excel_path, csv_path)
	print(f"已导出 CSV: {csv_path}")

	preview_text = read_csv_preview(csv_path, max_rows=10)
	print("CSV 前10行预览:")
	print(preview_text)


if __name__ == "__main__":
	main()


