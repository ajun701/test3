from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="用户名")
    password: str = Field(..., min_length=6, max_length=128, description="密码")
    register_key: str = Field(..., min_length=8, max_length=256, description="注册密钥")


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="用户名")
    password: str = Field(..., min_length=6, max_length=128, description="密码")


class UserInfoResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    username: str
    is_active: bool
    created_at: datetime
    last_login_at: Optional[datetime] = None


class AuthTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserInfoResponse


class TablePreview(BaseModel):
    total_rows: int = Field(default=0, description="表总行数")
    shown_rows: int = Field(default=0, description="当前返回行数")
    columns: List[str] = Field(default_factory=list, description="列名")
    rows: List[Dict[str, Any]] = Field(default_factory=list, description="预览数据")


class CleanResponse(BaseModel):
    total_rows: int = Field(..., description="解析到的总行数")
    normal_rows: int = Field(..., description="清洗正常行数")
    abnormal_rows: int = Field(..., description="异常待回访行数")
    over_limit_rows: int = Field(default=0, description="金额超12且其他字段正常行数")
    normal_file_url: str = Field(..., description="正常表下载 URL")
    abnormal_file_url: str = Field(..., description="异常表下载 URL")
    over_limit_file_url: Optional[str] = Field(default=None, description="超12单独导出下载 URL")
    report: Dict[str, Any] = Field(default_factory=dict, description="一致性校验报告")
    normal_preview: TablePreview = Field(default_factory=TablePreview, description="正常预览")
    abnormal_preview: TablePreview = Field(default_factory=TablePreview, description="异常预览")
    over_limit_preview: TablePreview = Field(default_factory=TablePreview, description="超12单独导出预览")


class MatchResponse(BaseModel):
    total_rows: int = Field(..., description="步骤二源表总行数")
    inbound_rows: int = Field(..., description="已入库可进入 AI 行数")
    pending_rows: int = Field(..., description="未入库需跟进行数")
    inbound_file_url: str = Field(..., description="已入库表下载 URL")
    pending_file_url: str = Field(..., description="未入库表下载 URL")
    report: Dict[str, Any] = Field(default_factory=dict, description="一致性校验报告")
    inbound_preview: TablePreview = Field(default_factory=TablePreview, description="已入库预览")
    pending_preview: TablePreview = Field(default_factory=TablePreview, description="未入库预览")


class TablePreviewResponse(TablePreview):
    pass


class AITaskCreateRequest(BaseModel):
    model_name: str = Field(default="qwen3-vl-flash", description="模型名称")
    max_images: int = Field(default=4, ge=1, le=10, description="每条记录最大传图数")
    min_interval_sec: float = Field(default=0.8, ge=0.0, description="最小请求间隔秒数")
    max_retries: int = Field(default=4, ge=0, description="最大重试次数")
    backoff_base_sec: float = Field(default=1.0, ge=0.1, description="重试退避基准秒数")
    max_ai_rows: int = Field(default=300, ge=1, description="最大 AI 处理行数")


class AITaskResponse(BaseModel):
    task_id: str
    status: str
    message: str = ""


class AITaskStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    task_id: str
    status: str
    created_at: datetime
    updated_at: datetime
    finished_at: Optional[datetime] = None

    total: int
    processed: int
    pending: int
    ok_rows: Optional[int] = 0
    bad_rows: Optional[int] = 0
    min_interval_sec: Optional[float] = Field(default=None, description="AI 最小请求间隔秒数")

    error_message: Optional[str] = None
    artifacts: List[str] = Field(default_factory=list, description="结果文件下载路径")
    progress_ratio: float = Field(default=0.0, description="处理进度比例 (0-1)")
    alignment_report: Dict[str, Any] = Field(default_factory=dict, description="一致性校验")


class AITaskAlignmentResponse(BaseModel):
    task_id: str
    alignment_report: Dict[str, Any] = Field(default_factory=dict)


class AITaskRowsResponse(BaseModel):
    task_id: str
    total_rows: int
    page: int
    page_size: int
    total_pages: int
    columns: List[str] = Field(default_factory=list)
    rows: List[Dict[str, Any]] = Field(default_factory=list)


class AITaskSnapshotResponse(BaseModel):
    task_id: str
    processed_rows: int
    unprocessed_rows: int
    processed_file_url: str
    unprocessed_file_url: str
    ok_file_url: Optional[str] = None
    bad_file_url: Optional[str] = None


class OperationHistoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    timestamp: datetime
    stage: str
    action: str
    operator: str
    input_rows: int
    output_rows: int
    detail: Dict[str, Any] | Any


class OperationHistoryListResponse(BaseModel):
    total: int
    items: List[OperationHistoryResponse] = Field(default_factory=list)


class ArtifactRecordResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    stage: str
    action: str
    operator: str
    source_file: str
    task_id: str
    input_rows: int
    output_rows: int
    file_url: str
    file_name: str
    payload: Dict[str, Any] | Any = Field(default_factory=dict)


class ArtifactRecordListResponse(BaseModel):
    total: int
    items: List[ArtifactRecordResponse] = Field(default_factory=list)
