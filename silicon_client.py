"""SiliconFlow OpenAI 兼容客户端封装。"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from openai import OpenAI

from .config import AppConfig, ensure_api_key


def _to_dict(obj: Any) -> Dict[str, Any]:
	"""兼容不同 openai 版本的响应转字典。"""
	if hasattr(obj, "model_dump"):
		return obj.model_dump()  # type: ignore[return-value]
	if hasattr(obj, "to_dict_recursive"):
		return obj.to_dict_recursive()  # type: ignore[return-value]
	if hasattr(obj, "to_dict"):
		return obj.to_dict()  # type: ignore[return-value]
	return dict(obj) if isinstance(obj, dict) else {"data": obj}


class SiliconClient:
	"""封装 OpenAI Chat Completions 接口，支持多个 API 供应商。"""

	def __init__(self, config: Optional[AppConfig] = None) -> None:
		self.config = config or AppConfig.from_env()
		self._client = OpenAI(
			api_key=ensure_api_key(self.config),
			base_url=self.config.base_url,
		)

	def chat_completion(
		self,
		messages: Iterable[Dict[str, Any]],
		model: Optional[str] = None,
		temperature: Optional[float] = None,
		response_format: Optional[Dict[str, Any]] = None,
		timeout: Optional[float] = None,
	) -> Dict[str, Any]:
		"""发送 Chat Completions 请求并返回原始响应字典。"""
		model_name = model or self.config.model
		temp = (
			temperature
			if temperature is not None
			else self.config.response_temperature
		)
		resp = self._client.chat.completions.create(
			model=model_name,
			messages=list(messages),
			temperature=temp,
			response_format=response_format,
			timeout=timeout or self.config.rate_limit.timeout,
		)
		return _to_dict(resp)

	def create_batch(
		self,
		*,
		input_file_id: str,
		endpoint: str = "/v1/chat/completions",
		completion_window: str = "24h",
		extra_body: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""创建 SiliconFlow Batch 任务。"""
		resp = self._client.batches.create(
			input_file_id=input_file_id,
			endpoint=endpoint,
			completion_window=completion_window,
			extra_body=extra_body,
		)
		return _to_dict(resp)

	def retrieve_batch(self, batch_id: str) -> Dict[str, Any]:
		resp = self._client.batches.retrieve(batch_id)
		return _to_dict(resp)

	def cancel_batch(self, batch_id: str) -> Dict[str, Any]:
		resp = self._client.batches.cancel(batch_id)
		return _to_dict(resp)

	def download_file(self, file_id: str) -> bytes:
		return self._client.files.content(file_id)

	def upload_jsonl(self, *, path: str, purpose: str = "batch") -> Dict[str, Any]:
		with open(path, "rb") as fh:
			resp = self._client.files.create(
				file=fh,
				purpose=purpose,
			)
		return _to_dict(resp)


