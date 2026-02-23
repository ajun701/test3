# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.core.config import settings
from app.api.endpoints import router as api_router
from app.api.auth import router as auth_router
from app.db.bootstrap import init_database

# =======================
# 启动时自动建表
# =======================
init_database()

# =======================
# FastAPI 实例初始化
# =======================
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="叠纸心意旗舰店 - 退运费智能审核系统后端 API",
    version="2.0.0",
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# =======================
# 配置跨域请求 (CORS) - 供 Vue/React 前端调用
# =======================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境请改为前端实际域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =======================
# 注册路由组
# =======================
app.include_router(auth_router, prefix=settings.API_V1_STR)
app.include_router(api_router, prefix=settings.API_V1_STR)

# =======================
# 挂载静态文件目录 (用于对外提供 Artifacts / Excel 下载)
# 前端可通过 /artifacts/... 访问生成的 Excel
# =======================
app.mount("/artifacts", StaticFiles(directory=settings.ARTIFACT_DIR), name="artifacts")

@app.get("/", tags=["Health Check"])
def root():
    return {
        "message": "Welcome to Refund Audit System API",
        "docs_url": "/docs",
        "status": "Running"
    }
