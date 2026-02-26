# app/tasks/celery_app.py
from celery import Celery
from app.core.config import settings

celery_app = Celery(
    "refund_audit_worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["app.tasks.ai_tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Shanghai",
    enable_utc=True,
    worker_prefetch_multiplier=1, # 防止单个 worker 独占大量长耗时任务
)

# 自动发现 tasks 目录下的任务
celery_app.autodiscover_tasks(["app.tasks"])
