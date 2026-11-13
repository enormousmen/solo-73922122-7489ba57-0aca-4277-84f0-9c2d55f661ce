"""DEM 拼接 Web 服务:上传/样例 -> 排队 -> gdalwarp 镶嵌 -> COG -> 下载。"""
from __future__ import annotations

import json
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse

from taskmanager import (
    InputError,
    STATUS_DONE,
    TaskCancelled,
    TaskManager,
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR.parent / "data"))
SAMPLES_DIR = Path(os.environ.get("SAMPLES_DIR", DATA_DIR / "samples"))
TASKS_ROOT = DATA_DIR / "tasks"
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "2"))
# 每次 gdalinfo 校验的基线延迟(秒),用于演示输入准备阶段的慢校验;0 表示关闭
BASE_VALIDATE_DELAY = float(os.environ.get("VALIDATE_DELAY", "0"))
MAX_INPUTS = 4
SAMPLE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+\.tiff?$", re.IGNORECASE)

manager: TaskManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager
    TASKS_ROOT.mkdir(parents=True, exist_ok=True)
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    manager = TaskManager(TASKS_ROOT, max_parallel=MAX_PARALLEL)
    manager.cleanup_stale()
    yield


app = FastAPI(title="DEM 拼接服务", lifespan=lifespan)


# ---------- 页面 ----------

@app.get("/", response_class=HTMLResponse)
async def index():
    return (BASE_DIR / "static" / "index.html").read_text(encoding="utf-8")


@app.get("/healthz")
async def healthz():
    return {"ok": True}


# ---------- 样例 ----------

def _load_manifest() -> dict:
    p = SAMPLES_DIR / "manifest.json"
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


@app.get("/api/samples")
async def list_samples():
    manifest = _load_manifest()
    out = []
    if SAMPLES_DIR.is_dir():
        for p in sorted(SAMPLES_DIR.glob("*.tif")):
            out.append(
                {
                    "name": p.name,
                    "size": p.stat().st_size,
                    "description": manifest.get(p.name, ""),
                }
            )
    return out


# ---------- 任务 ----------

@app.get("/api/tasks")
async def list_tasks():
    return [
        t.public()
        for t in sorted(manager.tasks.values(), key=lambda x: x.created_at, reverse=True)
    ]


@app.post("/api/tasks")
async def create_task(
    resolution: float = Form(...),
    plan: str = Form(...),
    slow_validate: bool = Form(False),
    validate_delay: float = Form(0.0),
    files: Optional[List[UploadFile]] = File(default=None),
):
    """plan 为 JSON 数组,按覆盖优先级从低到高排列:
    [{"type":"upload","index":0}, {"type":"sample","name":"dem_A_utm32n.tif"}]

    任务记录创建后立即对任务列表可见,输入保存与 gdalinfo 校验(慢校验)
    期间也可取消。若准备阶段被取消,本请求直接返回该任务的 CANCELLED 结果。
    """
    if not (0 < resolution <= 10000):
        raise HTTPException(400, "目标像元边长必须为大于 0 的数(米)")
    try:
        plan_items = json.loads(plan)
    except json.JSONDecodeError:
        raise HTTPException(400, "plan 不是合法 JSON")
    if not isinstance(plan_items, list) or not (1 <= len(plan_items) <= MAX_INPUTS):
        raise HTTPException(400, f"文件数量须为 1~{MAX_INPUTS}")

    delay = BASE_VALIDATE_DELAY
    if slow_validate:
        delay += max(0.0, validate_delay)

    uploads = list(files or [])
    resolved = []  # (kind, ref, display_name)
    for item in plan_items:
        if not isinstance(item, dict):
            raise HTTPException(400, "plan 元素格式错误")
        kind = item.get("type")
        if kind == "upload":
            idx = item.get("index")
            if not isinstance(idx, int) or not (0 <= idx < len(uploads)):
                raise HTTPException(400, "plan 中的 upload 索引越界")
            up = uploads[idx]
            resolved.append(("upload", up, up.filename or "upload.tif"))
        elif kind == "sample":
            path = _sample_path(item.get("name") or "")
            resolved.append(("sample", path, path.name))
        else:
            raise HTTPException(400, f"未知 plan 类型: {kind}")

    # 先建任务记录并注册:列表立即可见,准备阶段即可被取消
    task = manager.create([d for _, _, d in resolved], resolution)
    try:
        await manager.prepare_inputs(task, resolved, validate_delay=delay)
    except TaskCancelled:
        # 取消记录已保留在列表中(CANCELLED),创建请求返回该取消结果
        return task.public()
    except InputError as e:
        manager.discard(task)  # 普通输入失败才移除记录;终态记录受保护
        raise HTTPException(400, str(e))
    except Exception as e:  # noqa: BLE001 - 兜底,避免 500 且保证暂存被清理
        manager.discard(task)
        raise HTTPException(400, f"保存输入失败: {e}")

    manager.enqueue(task)
    return task.public()


@app.post("/api/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    # 等待执行方在安全点回收校验/GDAL 进程并清理暂存后,才返回取消完成
    t = await manager.cancel(task_id)
    if t is None:
        raise HTTPException(404, "任务不存在")
    return t.public()


@app.get("/api/tasks/{task_id}/download")
async def download(task_id: str):
    t = manager.tasks.get(task_id)
    if t is None:
        raise HTTPException(404, "任务不存在")
    if t.status != STATUS_DONE:
        raise HTTPException(409, "任务未成功完成,不能下载")
    path = t.workdir / "result.tif"
    if not path.is_file():
        raise HTTPException(410, "结果文件已不存在")
    return FileResponse(path, media_type="image/tiff", filename=f"mosaic_{t.id}.tif")


# ---------- 工具 ----------

def _sample_path(name: str) -> Path:
    if not SAMPLE_NAME_RE.match(name):
        raise HTTPException(400, f"非法样例名: {name}")
    p = SAMPLES_DIR / name
    if not p.is_file():
        raise HTTPException(404, f"样例不存在: {name}")
    return p
