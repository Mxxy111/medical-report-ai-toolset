"""System prompt 构造（支持模板化）。"""

from __future__ import annotations

from typing import Optional

from .templates import get_template


def build_system_prompt(template_id: str = "rcc") -> str:
	"""构建要求模型输出严格 JSON 的系统提示词。
	
	Args:
		template_id: 模板 ID (rcc/lung_cancer/generic)
	"""
	template = get_template(template_id)
	return template.build_prompt()


