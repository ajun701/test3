# app/schemas.py
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime

class TablePreview(BaseModel):
    total_rows: int = Field(default=0, description="表总行数")
    shown_rows: int = Field(default=0, description="当前返回行数")
    columns: List[str] = Field(default_factory=list, description="列名")
    rows: List[Dict[str, Any]] = Field(default_factory=list, description="预览数据")

# =======================
# 步骤一与步骤二：清洗与匹配
# =======================
class CleanResponse(BaseModel):
    total_rows: int = Field(..., description="解析到的总行数")
    normal_rows: int = Field(..., description="清洗正常的行数")
    abnormal_rows: int = Field(..., description="异常需回访的行数")
    normal_file_url: str = Field(..., description="正常表的下载 URL")
    abnormal_file_url: str = Field(..., description="异常表的下载 URL")
    report: Dict[str, Any] = Field(default_factory=dict, description="一致性校验报告")
    normal_preview: TablePreview = Field(default_factory=TablePreview, description="正常表预览")
    abnormal_preview: TablePreview = Field(default_factory=TablePreview, description="异常表预览")

class MatchResponse(BaseModel):
    total_rows: int = Field(..., description="步骤二待匹配总行数")
    inbound_rows: int = Field(..., description="已入库（可进AI）的行数")
    pending_rows: int = Field(..., description="未入库（待跟进）的行数")
    inbound_file_url: str = Field(..., description="已入库表的下载 URL")
    pending_file_url: str = Field(..., description="未入库表的下载 URL")
    report: Dict[str, Any] = Field(default_factory=dict, description="一致性校验报告")
    inbound_preview: TablePreview = Field(default_factory=TablePreview, description="已入库表预览")
    pending_preview: TablePreview = Field(default_factory=TablePreview, description="未入库表预览")

class TablePreviewResponse(TablePreview):
    pass

# =======================
# 步骤三：AI 任务控制
# =======================
class AITaskCreateRequest(BaseModel):
    model_name: str = Field(default="qwen3-vl-flash", description="通义千问模型名称")
    max_images: int = Field(default=4, ge=1, le=10, description="每条记录最多传给 AI 的图片数")
    min_interval_sec: float = Field(default=0.8, ge=0.0, description="最小请求间隔")
    max_retries: int = Field(default=4, ge=0, description="最大重试次数")
    backoff_base_sec: float = Field(default=1.0, ge=0.1, description="退避基准秒数")
    max_ai_rows: int = Field(default=300, ge=1, description="AI最大处理行数（防误点防爆配额）")

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
    
    error_message: Optional[str] = None
    artifacts: List[str] = Field(default_factory=list, description="结果文件下载路径")
    progress_ratio: float = Field(default=0.0, description="进度百分比 (0-1)")
    alignment_report: Dict[str, Any] = Field(default_factory=dict, description="步骤三源数据与处理结果一致性校验")

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

# =======================
# 历史记录
# =======================
class OperationHistoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    timestamp: datetime
    stage: str
    action: str
    operator: str
    input_rows: int
    output_rows: int
    detail: Dict[str, Any]

class OperationHistoryListResponse(BaseModel):
    total: int
    items: List[OperationHistoryResponse] = Field(default_factory=list)
