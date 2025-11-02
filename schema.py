"""定义提取结果的 schema 与校验逻辑（支持模板化）。"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Optional, Any

from .templates import get_template


@dataclass
class ExtractionResult:
	"""结构化特征结果（动态字段）。"""
	id_value: str = ""
	_data: Dict[str, Any] = None  # 存储动态字段
	
	def __post_init__(self):
		if self._data is None:
			self._data = {}
	
	def __setitem__(self, key: str, value: Any):
		self._data[key] = value
	
	def __getitem__(self, key: str) -> Any:
		return self._data.get(key)
	
	def get(self, key: str, default=None) -> Any:
		return self._data.get(key, default)
	
	def to_dict(self) -> Dict[str, object]:
		result = {"id": self.id_value}
		result.update(self._data)
		return result


def _clean_str(value: object) -> Optional[str]:
	if isinstance(value, str):
		text = value.strip()
		return text or None
	return None


def normalise_extraction(raw: Dict[str, object], template_id: str = "rcc") -> ExtractionResult:
	"""将模型返回的原始 JSON 规范化为 ExtractionResult。
	
	Args:
		raw: 原始 JSON 数据
		template_id: 模板 ID，用于确定字段列表
	"""
	result = ExtractionResult()
	
	# 处理 ID
	if isinstance(raw.get("id_value"), str):
		result.id_value = raw["id_value"].strip()
	elif isinstance(raw.get("id"), str):
		result.id_value = raw["id"].strip()
	
	# 根据模板动态处理所有字段
	template = get_template(template_id)
	for field in template.fields:
		value = raw.get(field.name)
		if value is not None:
			if isinstance(value, str):
				cleaned = _clean_str(value)
				result[field.name] = cleaned if cleaned else ""
			else:
				result[field.name] = value
		else:
			result[field.name] = ""
	
	return result


def flatten_for_csv(result: ExtractionResult) -> Dict[str, object]:
	"""扁平化结构，便于写入 CSV。"""
	return result._data.copy()


