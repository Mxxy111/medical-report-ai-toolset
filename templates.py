"""提取模板配置，支持不同疾病/场景的定制化提取。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class FieldConfig:
	"""单个字段的配置。"""
	name: str  # 字段名（英文）
	display_name: str  # 显示名称（中文）
	description: str  # 字段说明
	field_type: str = "string"  # 字段类型：string, enum, number, date
	enum_values: Optional[List[str]] = None  # 如果是枚举类型，可选值列表
	required: bool = False  # 是否必填
	example: str = ""  # 示例值


@dataclass
class ExtractionTemplate:
	"""提取模板：定义特定疾病/场景的提取目标。"""
	template_id: str  # 模板 ID
	template_name: str  # 模板名称
	description: str  # 模板描述
	fields: List[FieldConfig] = field(default_factory=list)  # 字段列表
	system_prompt_template: str = ""  # 系统提示词模板
	
	def to_json_schema(self) -> Dict[str, any]:
		"""生成 JSON Schema 描述。"""
		properties = {}
		required = []
		
		for f in self.fields:
			prop = {"type": "string", "description": f.description}
			if f.enum_values:
				prop["enum"] = f.enum_values
			if f.example:
				prop["example"] = f.example
			properties[f.name] = prop
			if f.required:
				required.append(f.name)
		
		return {
			"type": "object",
			"properties": properties,
			"required": required if required else None,
		}
	
	def build_prompt(self) -> str:
		"""根据字段配置生成系统提示词。"""
		if self.system_prompt_template:
			return self.system_prompt_template
		
		# 自动生成提示词
		field_descriptions = []
		for f in self.fields:
			desc = f'    "{f.name}": {f.description}'
			if f.enum_values:
				desc += f'，可选值：{"|".join(f.enum_values)}'
			if f.example:
				desc += f'，示例：{f.example}'
			field_descriptions.append(desc)
		
		schema_str = ",\n".join(field_descriptions)
		
		return f"""你是一名医学影像报告分析助手，任务是从"{self.template_name}"的检查报告中提取结构化信息。

- 严格输出 JSON，对象结构如下：
  {{
{schema_str}
  }}
- 若原文未提及，填空字符串 "" 或 null。
- 仅提取文本中明确出现的信息，不要推测。
- 输出必须是单个 JSON 对象，无额外注释、无 Markdown。
"""


# ================== 预设模板 ==================

# 模板1: 肾癌核医学报告（当前使用）
RCC_TEMPLATE = ExtractionTemplate(
	template_id="rcc_nuclear_medicine",
	template_name="肾癌核医学报告",
	description="从 PET/CT 或骨显像报告中提取肾癌相关结构化特征",
	fields=[
		FieldConfig(
			name="modality",
			display_name="检查方式",
			description="检查类型：PETCT|骨显像|SPECT/CT|TOC|其他|unknown",
			field_type="enum",
			enum_values=["PETCT", "骨显像", "SPECT/CT", "TOC", "其他", "unknown"],
			required=True,
			example="PETCT"
		),
		FieldConfig(
			name="exam_overview",
			display_name="检查基础信息",
			description="检查类型与示踪剂，如'PET/CT，F-18-FDG'",
			example="PET/CT、F-18-FDG"
		),
		FieldConfig(
			name="renal_status",
			display_name="肾脏核心状态",
			description="双肾及术后状态，如'左肾缺如，右肾正常'",
			example="左肾缺如，右肾正常"
		),
		FieldConfig(
			name="radioactive_findings",
			display_name="放射性异常核心",
			description="代谢增高的关键部位 + SUVmax/大小，如'右肾上腺（略高，3.9）、枕骨（增高，4.4）'",
			example="右肾上腺（略高，3.9）、枕骨（增高，4.4）"
		),
		FieldConfig(
			name="metastasis_summary",
			display_name="肾癌相关转移/累及",
			description="疑似转移/侵袭（骨、肺、肝、肾上腺、淋巴结等），如'骨（枕骨破坏）、肺（类结节）'",
			example="骨（枕骨破坏）、肺（类结节）"
		),
		FieldConfig(
			name="surgery_history",
			display_name="关键手术史",
			description="相关手术或重要既往史，如'左肾癌术、脾缺如、胆囊缺如'",
			example="左肾癌术、脾缺如、胆囊缺如"
		),
		FieldConfig(
			name="notes",
			display_name="补充说明",
			description="无法分类的重要信息或不确定性",
			example=""
		),
	],
	system_prompt_template="""你是一名核医学影像科医生助手，任务是从 PET/CT 或骨显像报告的"检查所见"描述中提取肾癌相关结构化特征。
- 严格输出 JSON，对象顶层结构如下：
  {
    "modality": "PETCT|骨显像|SPECT/CT|TOC|其他|unknown",
    "exam_overview": 字符串或 null,
    "renal_status": 字符串或 null,
    "radioactive_findings": 字符串或 null,
    "metastasis_summary": 字符串或 null,
    "surgery_history": 字符串或 null,
    "notes": 字符串，可为空
  }
- 所有枚举值必须为小写指定值。
- 若原文未提及，务必填 "unknown"；数值不可解析时填 null，并在 notes 说明。
- 模块提取要点：
  1) "检查基础信息" -> `exam_overview`：概括检查类型与示踪剂（如"PET/CT，F-18-FDG"）。
  2) "肾脏核心状态" -> `renal_status`：总结双肾及术后状态（缺如、术后、占位大小等），保留关键尺寸/部位。
  3) "放射性异常核心" -> `radioactive_findings`：列出代谢增高的关键部位 + SUVmax/大小（以逗号分隔）。
  4) "肾癌相关转移/累及" -> `metastasis_summary`：聚焦与肾癌相关的疑似转移/侵袭（骨、肺、肝、肾上腺、淋巴结等），用简短中文分号分隔。
  5) "关键手术史" -> `surgery_history`：提炼文本中出现的相关手术或重要既往史（肾切除、肝转移手术等）。
- `modality` 可根据关键词判断：PET/CT -> "PETCT"，骨显像/MDP -> "骨显像"，SPECT/CT -> "SPECT/CT"，TOC 相关 -> "TOC"，无法判断 -> "unknown"。
- `notes` 用于补充无法分类的重要信息，或指出文本缺失。
- 输出必须是单个 JSON 对象，无额外注释、无 Markdown。
"""
)


# 模板2: 肺癌 CT 报告（示例）
LUNG_CANCER_TEMPLATE = ExtractionTemplate(
	template_id="lung_cancer_ct",
	template_name="肺癌 CT 报告",
	description="从胸部 CT 报告中提取肺癌相关结构化特征",
	fields=[
		FieldConfig(
			name="exam_type",
			display_name="检查类型",
			description="CT 类型",
			field_type="enum",
			enum_values=["平扫", "增强", "平扫+增强", "unknown"],
			required=True
		),
		FieldConfig(
			name="tumor_location",
			display_name="肿瘤位置",
			description="肺癌病灶位置，如'右肺上叶'、'左肺下叶'",
			example="右肺上叶"
		),
		FieldConfig(
			name="tumor_size",
			display_name="肿瘤大小",
			description="肿瘤最大径，如'3.5×2.8cm'",
			example="3.5×2.8cm"
		),
		FieldConfig(
			name="tumor_characteristics",
			display_name="肿瘤特征",
			description="肿瘤形态、边界、密度等特征",
			example="分叶状，边界不清，部分毛刺征"
		),
		FieldConfig(
			name="lymph_node_status",
			display_name="淋巴结状态",
			description="纵隔/肺门淋巴结情况",
			field_type="enum",
			enum_values=["阴性", "阳性", "可疑", "unknown"],
			example="阳性"
		),
		FieldConfig(
			name="lymph_node_details",
			display_name="淋巴结详情",
			description="阳性淋巴结的位置、大小",
			example="纵隔4R组，短径1.2cm"
		),
		FieldConfig(
			name="metastasis_status",
			display_name="转移状态",
			description="远处转移情况",
			field_type="enum",
			enum_values=["阴性", "阳性", "可疑", "unknown"],
			example="阴性"
		),
		FieldConfig(
			name="metastasis_sites",
			display_name="转移部位",
			description="转移器官/部位",
			example="肝脏、骨"
		),
		FieldConfig(
			name="pleural_effusion",
			display_name="胸腔积液",
			description="胸腔积液情况",
			field_type="enum",
			enum_values=["无", "少量", "中量", "大量", "unknown"],
			example="少量"
		),
		FieldConfig(
			name="notes",
			display_name="补充说明",
			description="其他重要发现",
			example=""
		),
	]
)


# 模板3: 通用医学报告（极简）
GENERIC_TEMPLATE = ExtractionTemplate(
	template_id="generic_medical",
	template_name="通用医学报告",
	description="通用医学检查报告提取",
	fields=[
		FieldConfig(
			name="exam_type",
			display_name="检查类型",
			description="检查方式/影像类型",
			required=True,
			example="CT、MRI、超声等"
		),
		FieldConfig(
			name="key_findings",
			display_name="关键发现",
			description="最重要的检查发现",
			example="肝脏多发占位"
		),
		FieldConfig(
			name="diagnosis_suggestion",
			display_name="诊断提示",
			description="影像诊断或诊断提示",
			example="符合肝转移表现"
		),
		FieldConfig(
			name="notes",
			display_name="补充说明",
			description="其他重要信息",
			example=""
		),
	]
)


# 模板注册表
TEMPLATES: Dict[str, ExtractionTemplate] = {
	"rcc": RCC_TEMPLATE,
	"lung_cancer": LUNG_CANCER_TEMPLATE,
	"generic": GENERIC_TEMPLATE,
}


def get_template(template_id: str) -> ExtractionTemplate:
	"""获取指定模板。"""
	if template_id not in TEMPLATES:
		raise ValueError(
			f"未找到模板 '{template_id}'。可用模板：{', '.join(TEMPLATES.keys())}"
		)
	return TEMPLATES[template_id]


def list_templates() -> List[str]:
	"""列出所有可用模板。"""
	return list(TEMPLATES.keys())

