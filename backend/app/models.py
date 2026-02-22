# app/models.py
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class OperationHistory(Base):
    """
    操作历史记录表（替代原 operation_history.jsonl）
    """
    __tablename__ = "operation_history"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    stage = Column(String(50), nullable=False, index=True)      # e.g., "步骤一清洗", "步骤二入库匹配", "步骤三AI复核"
    action = Column(String(50), nullable=False)                 # e.g., "执行清洗", "创建AI任务"
    operator = Column(String(50), default="system")
    
    # 记录关键输入输出指标
    input_rows = Column(Integer, default=0)
    output_rows = Column(Integer, default=0)
    
    # 存储更详细的差异/报告数据或生成的文件路径数组
    detail = Column(JSON, default=dict)

class AITask(Base):
    """
    AI 任务状态表（替代原 operation_tasks/{task_id}/meta.json）
    """
    __tablename__ = "ai_tasks"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(String(100), unique=True, index=True, nullable=False)
    
    status = Column(String(20), default="pending", index=True)   # pending, running, paused, completed, error
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    
    # 任务元数据
    source_file = Column(String(255), nullable=True)
    input_rows = Column(Integer, default=0)
    total = Column(Integer, default=0)              # 本次计划处理总行数
    next_idx = Column(Integer, default=0)           # 下一个待处理的索引
    
    # AI 配置
    col_amount = Column(String(100), nullable=False)
    col_shot = Column(String(100), nullable=False)
    model_name = Column(String(50), default="qwen-vl-plus")
    max_images = Column(Integer, default=4)
    min_interval_sec = Column(Float, default=0.8)
    max_retries = Column(Integer, default=4)
    backoff_base_sec = Column(Float, default=1.0)
    
    # 执行结果与上下文
    error_message = Column(Text, nullable=True)
    df_work_path = Column(String(255), nullable=False)  # 指向本地持久化的工作 DataFrame 路径 (.pkl)
    source_df_path = Column(String(255), nullable=False) # 指向原始数据的路径
    artifacts = Column(JSON, default=list)              # 产物下载路径列表
