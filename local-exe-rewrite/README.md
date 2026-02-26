# 本地 EXE 重构版

这个目录是对原项目的本地化重构版本，目标是：

- 不依赖 FastAPI / Redis / Celery / 浏览器
- 单机本地运行（SQLite + 本地文件）
- 后续可打包为 Windows `exe`
- 保持原业务流程与功能模块：登录注册、步骤一清洗、步骤二入库匹配、步骤三AI复核（异步+暂停继续+快照+一致性校验+行级进度）、步骤四历史记录与文件追溯

## 目录结构

- `app/main.py`: 应用入口
- `app/core`: 配置、常量、密码安全
- `app/db`: SQLAlchemy 模型与本地 SQLite 会话
- `app/services`: 清洗/匹配/AI任务/历史/产物/鉴权
- `app/ui`: PySide6 桌面 UI
- `run_local.bat`: 本地开发运行脚本
- `build_exe.bat`: PyInstaller 打包脚本

## 本地运行

1. 进入目录：
   - `cd local-exe-rewrite`
2. 执行：
   - `run_local.bat`

首次会自动创建虚拟环境并安装依赖。

## 打包 EXE

1. 进入目录：
   - `cd local-exe-rewrite`
2. 执行：
   - `build_exe.bat`

产物默认在 `dist/RefundAuditLocal/`。

## 配置说明

支持环境变量：

- `DASHSCOPE_API_KEY`: AI复核默认 API Key
- `DATABASE_URL`: 本地数据库连接（默认 sqlite）
- `REGISTER_SECRET`: 注册密钥

默认数据目录：

- 开发模式：`local-exe-rewrite/data`
- EXE 模式：`exe 同级目录/data`

## 与原项目功能映射

- 原 `POST /clean` -> 本地按钮“执行清洗”
- 原 `POST /match` -> 本地按钮“执行匹配”
- 原 AI 任务接口（start/status/rows/pause/resume/alignment/snapshot）-> 本地 AI 页签对应按钮和轮询
- 原 `history` / `history/files` / `history/export` -> 本地历史页签查询与 CSV 导出

