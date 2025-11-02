"""全局配置与默认参数。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Optional


DEFAULT_BASE_URL = "https://api.siliconflow.cn/v1"
DEFAULT_MODEL = "Qwen/Qwen3-Omni-30B-A3B-Instruct"
DEFAULT_OUTPUT_DIR = "outputs"

# 预设的 API 供应商配置
API_PROVIDERS = {
	"siliconflow": {
		"base_url": "https://api.siliconflow.cn/v1",
		"default_model": "Qwen/Qwen3-Omni-30B-A3B-Instruct",
		"api_key_env": "SILICONFLOW_API_KEY",
	},
	"aliyun": {
		"base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
		"default_model": "qwen-plus",
		"api_key_env": "ALIYUN_API_KEY",
	},
	"openai": {
		"base_url": "https://api.openai.com/v1",
		"default_model": "gpt-4",
		"api_key_env": "OPENAI_API_KEY",
	},
	"deepseek": {
		"base_url": "https://api.deepseek.com/v1",
		"default_model": "deepseek-chat",
		"api_key_env": "DEEPSEEK_API_KEY",
	},
	"qianduoduo": {
		"base_url": "https://api2.aigcbest.top/v1",
		"default_model": "gpt-4.1-mini",
		"api_key_env": "QIANDUODUO_API_KEY",
	},
}


@dataclass
class RateLimitConfig:
	"""速率限制配置。"""
	rpm: int = 30  # 每分钟请求数
	concurrency: int = 5  # 并发数
	timeout: float = 60.0  # 单次请求超时（秒）
	max_retries: int = 3
	retry_backoff: float = 2.0  # 指数退避基数


@dataclass
class CsvConfig:
	"""CSV 输入相关配置。"""
	text_column_candidates: List[str] = field(
		default_factory=lambda: ["检查所见", "所见", "检查所见描述"]
	)
	id_column_candidates: List[str] = field(
		default_factory=lambda: ["门诊号/住院号", "住院号", "病历号", "病人ID"]
	)
	default_encoding: str = "utf-8-sig"
	max_chars: int = 10_000  # 单条文本最大长度


@dataclass
class AppConfig:
	"""应用顶层配置。"""
	base_url: str = DEFAULT_BASE_URL
	model: str = DEFAULT_MODEL
	output_dir: str = DEFAULT_OUTPUT_DIR
	response_temperature: float = 0.0
	api_key: str = ""  # API密钥
	template_id: str = "rcc"  # 提取模板 ID (rcc/lung_cancer/generic)
	rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
	csv: CsvConfig = field(default_factory=CsvConfig)

	@staticmethod
	def from_env(provider: str = None) -> "AppConfig":
		"""根据环境变量和供应商覆盖默认配置。
		
		Args:
			provider: API 供应商名称 (siliconflow/aliyun/openai/deepseek/custom)
		"""
		config = AppConfig()
		
		# 如果指定了供应商，使用预设配置
		if provider and provider in API_PROVIDERS:
			provider_config = API_PROVIDERS[provider]
			config.base_url = provider_config["base_url"]
			config.model = provider_config["default_model"]
			config.api_key = os.getenv(provider_config["api_key_env"], "")
		else:
			# 默认使用 SiliconFlow 或环境变量
			config.base_url = os.getenv("API_BASE_URL", os.getenv("SILICONFLOW_BASE_URL", config.base_url))
			config.model = os.getenv("API_MODEL", os.getenv("SILICONFLOW_MODEL", config.model))
			config.api_key = os.getenv("API_KEY", os.getenv("SILICONFLOW_API_KEY", ""))
		
		config.output_dir = os.getenv("RCC_OUTPUT_DIR", config.output_dir)
		config.template_id = os.getenv("EXTRACTION_TEMPLATE", config.template_id)
		config.response_temperature = float(
			os.getenv("RCC_TEMPERATURE", config.response_temperature)
		)
		rate = config.rate_limit
		rate.rpm = int(os.getenv("RCC_RPM", rate.rpm))
		rate.concurrency = int(os.getenv("RCC_CONCURRENCY", rate.concurrency))
		rate.timeout = float(os.getenv("RCC_TIMEOUT", rate.timeout))
		rate.max_retries = int(os.getenv("RCC_MAX_RETRIES", rate.max_retries))
		rate.retry_backoff = float(os.getenv("RCC_RETRY_BACKOFF", rate.retry_backoff))
		csv_cfg = config.csv
		csv_cfg.max_chars = int(os.getenv("RCC_MAX_CHARS", csv_cfg.max_chars))
		csv_cfg.default_encoding = os.getenv(
			"RCC_CSV_ENCODING", csv_cfg.default_encoding
		)
		return config
	
	def get_provider(self) -> Optional[str]:
		"""根据 base_url 识别当前使用的 API 供应商。
		
		Returns:
			供应商名称 (siliconflow/aliyun/openai/deepseek) 或 None（自定义）
		"""
		for provider, provider_config in API_PROVIDERS.items():
			if self.base_url == provider_config["base_url"]:
				return provider
		return None


def ensure_api_key(config: AppConfig = None) -> str:
	"""读取 API key，若缺失则抛出错误。
	
	优先使用 config.api_key，然后尝试环境变量。
	"""
	if config and config.api_key:
		return config.api_key
	
	# 尝试多个环境变量
	api_key = (
		os.getenv("API_KEY")
		or os.getenv("SILICONFLOW_API_KEY")
		or os.getenv("ALIYUN_API_KEY")
		or os.getenv("OPENAI_API_KEY")
		or os.getenv("DEEPSEEK_API_KEY")
		or os.getenv("QIANDUODUO_API_KEY")
	)
	if not api_key:
		raise RuntimeError(
			"未设置 API Key。请设置环境变量：API_KEY 或 SILICONFLOW_API_KEY 或 ALIYUN_API_KEY 等"
		)
	return api_key


