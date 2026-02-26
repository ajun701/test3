from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from app.api.auth_endpoints import router as auth_router
from app.api.endpoints import router as api_router
from app.core.auth import get_current_user
from app.core.config import settings
from app.db.session import engine
from app.models import Base


def ensure_runtime_schema() -> None:
    """Create missing tables and apply lightweight compatibility patches."""

    Base.metadata.create_all(bind=engine)
    if not settings.DATABASE_URL.startswith("sqlite"):
        return

    with engine.begin() as conn:
        cols = conn.execute(text("PRAGMA table_info(ai_tasks)")).fetchall()
        col_names = {str(c[1]) for c in cols}
        if "operator" not in col_names:
            conn.execute(text("ALTER TABLE ai_tasks ADD COLUMN operator VARCHAR(50) DEFAULT 'system'"))


ensure_runtime_schema()

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="退运费智能审核系统后端 API",
    version="2.1.0",
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Open endpoints: register/login/me
app.include_router(auth_router, prefix=settings.API_V1_STR)
# Business endpoints: require valid bearer token
app.include_router(api_router, prefix=settings.API_V1_STR, dependencies=[Depends(get_current_user)])

app.mount("/artifacts", StaticFiles(directory=settings.ARTIFACT_DIR), name="artifacts")


@app.get("/", tags=["Health Check"])
def root():
    return {"message": "Welcome to Refund Audit System API", "docs_url": "/docs", "status": "Running"}
