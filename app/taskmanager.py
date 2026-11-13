"""后台拼接任务调度。

流水线:MOSAIC(gdalwarp 重投影+镶嵌)-> COG(gdal_translate 生成带概览的 COG)
-> METADATA(gdalinfo 读取范围/尺寸)。全部成功才发布结果。

设计要点:
- 全局信号量限制并行任务数(默认 2),其余任务排队。
- 每个 GDAL 阶段都是独立子进程,放在自己的进程组里(start_new_session),
  取消时按进程组 SIGKILL 并 await 回收,不影响其他任务的进程。
- 阶段启动前、阶段结束后、发布结果前都检查取消标志:
  取消后绝不启动下一阶段,也绝不发布迟到结果。
- 失败/取消后清理该任务的暂存目录;完成的任务保留结果供下载。
- 像素数据全程由 GDAL 流式处理,不把整幅栅格读入内存。
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

STATUS_QUEUED = "QUEUED"
STATUS_RUNNING = "RUNNING"
STATUS_DONE = "DONE"
STATUS_FAILED = "FAILED"
STATUS_CANCELLED = "CANCELLED"
TERMINAL_STATUSES = {STATUS_DONE, STATUS_FAILED, STATUS_CANCELLED}

STAGE_MOSAIC = "MOSAIC"
STAGE_COG = "COG"
STAGE_METADATA = "METADATA"

TARGET_CRS = "EPSG:3857"
DST_NODATA = "-9999"

MAX_TASKS_KEPT = 200  # 内存中保留的最大任务记录数,超出时回收最旧的终态任务


class StageError(Exception):
    """某个 GDAL 阶段执行失败。"""


class TaskCancelled(Exception):
    """任务被取消(在阶段边界或子进程退出后检测到)。"""


@dataclass
class Task:
    id: str
    resolution: float
    filenames: list  # 原始文件名,按覆盖优先级从低到高排列
    workdir: Path
    created_at: float = field(default_factory=time.time)
    status: str = STATUS_QUEUED
    stage: Optional[str] = None
    error: Optional[str] = None
    log_tail: str = ""
    result: Optional[dict] = None
    cancel_requested: bool = False
    proc: Optional[asyncio.subprocess.Process] = None

    @property
    def inputs_dir(self) -> Path:
        return self.workdir / "inputs"

    def public(self) -> dict:
        return {
            "id": self.id,
            "status": self.status,
            "stage": self.stage,
            "error": self.error,
            "resolution": self.resolution,
            "filenames": list(self.filenames),
            "created_at": self.created_at,
            "log_tail": self.log_tail[-400:],
            "result": self.result,
            "download_url": f"/api/tasks/{self.id}/download"
            if self.status == STATUS_DONE
            else None,
        }


class TaskManager:
    def __init__(self, tasks_root: Path, max_parallel: int = 2):
        self.tasks_root = Path(tasks_root)
        self.tasks_root.mkdir(parents=True, exist_ok=True)
        self.sem = asyncio.Semaphore(max_parallel)
        self.tasks: dict = {}
        self._runners: set = set()

    # ---------- 任务生命周期 ----------

    def create(self, filenames, resolution: float) -> Task:
        """建立任务记录与暂存目录(尚未调度)。上传校验失败可调用 discard。"""
        self._gc()
        tid = uuid.uuid4().hex[:12]
        workdir = self.tasks_root / tid
        (workdir / "inputs").mkdir(parents=True)
        t = Task(id=tid, resolution=resolution, filenames=list(filenames), workdir=workdir)
        self.tasks[tid] = t
        return t

    def enqueue(self, t: Task) -> None:
        runner = asyncio.create_task(self._run(t))
        self._runners.add(runner)
        runner.add_done_callback(self._runners.discard)

    def discard(self, t: Task) -> None:
        """任务尚未入队时放弃(如上传校验失败)。"""
        self.tasks.pop(t.id, None)
        shutil.rmtree(t.workdir, ignore_errors=True)

    def cancel(self, task_id: str) -> Optional[Task]:
        """请求取消:置标志、杀当前 GDAL 进程组;排队任务直接终态并清理。"""
        t = self.tasks.get(task_id)
        if t is None or t.status in TERMINAL_STATUSES:
            return t
        t.cancel_requested = True
        self._kill_proc(t)
        if t.status == STATUS_QUEUED:
            self._finalize(t, STATUS_CANCELLED)
        return t

    def cleanup_stale(self) -> None:
        """启动时清空上次运行遗留的暂存目录(内存注册表已重置)。"""
        for child in self.tasks_root.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)

    # ---------- 内部实现 ----------

    def _gc(self) -> None:
        if len(self.tasks) <= MAX_TASKS_KEPT:
            return
        terminal = sorted(
            (t for t in self.tasks.values() if t.status in TERMINAL_STATUSES),
            key=lambda t: t.created_at,
        )
        for t in terminal[: len(self.tasks) - MAX_TASKS_KEPT]:
            self.tasks.pop(t.id, None)
            if t.status != STATUS_DONE:
                shutil.rmtree(t.workdir, ignore_errors=True)

    def _kill_proc(self, t: Task) -> None:
        proc = t.proc
        if proc is None or proc.returncode is not None:
            return
        try:
            # 子进程在独立会话/进程组中,整组 SIGKILL,回收本任务全部 GDAL 进程
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except (ProcessLookupError, PermissionError):
                pass

    def _finalize(self, t: Task, status: str, error: Optional[str] = None) -> None:
        if t.status in TERMINAL_STATUSES:
            return  # 已被取消端点等置为终态,不覆盖
        t.status = status
        t.stage = None
        t.error = error
        if status in (STATUS_FAILED, STATUS_CANCELLED):
            shutil.rmtree(t.workdir, ignore_errors=True)

    async def _run(self, t: Task) -> None:
        async with self.sem:
            if t.status in TERMINAL_STATUSES:
                return  # 排队期间已被取消并清理
            if t.cancel_requested:
                self._finalize(t, STATUS_CANCELLED)
                return
            t.status = STATUS_RUNNING
            try:
                await self._mosaic(t)
                await self._cog(t)
                await self._metadata(t)
            except TaskCancelled:
                self._finalize(t, STATUS_CANCELLED)
            except StageError as e:
                self._finalize(t, STATUS_FAILED, str(e))
            except Exception as e:  # noqa: BLE001 - 兜底,保证任务进入终态
                self._finalize(t, STATUS_FAILED, f"内部错误: {e!r}")
            else:
                # 发布前再检查:取消后绝不发布迟到结果
                if t.cancel_requested or t.status in TERMINAL_STATUSES:
                    self._finalize(t, STATUS_CANCELLED)
                else:
                    self._finalize(t, STATUS_DONE)

    async def _exec(self, t: Task, stage: str, cmd: list) -> str:
        """执行一个 GDAL 阶段。启动前与退出后都检查取消标志。"""
        if t.cancel_requested or t.status in TERMINAL_STATUSES:
            raise TaskCancelled()
        t.stage = stage
        t.log_tail = ""
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(t.workdir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,  # 独立进程组,便于整组终止
            )
        except FileNotFoundError:
            raise StageError(f"找不到 GDAL 工具: {cmd[0]}")
        t.proc = proc
        if t.cancel_requested:
            # 取消恰好发生在进程创建窗口:立即终止,不进入等待
            self._kill_proc(t)
        chunks: list = []
        try:
            assert proc.stdout is not None
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                chunks.append(chunk)
                t.log_tail = (t.log_tail + chunk.decode("utf-8", "replace"))[-800:]
            rc = await proc.wait()  # 回收子进程,避免僵尸
        finally:
            t.proc = None
        if t.cancel_requested or t.status in TERMINAL_STATUSES:
            raise TaskCancelled()
        if rc != 0:
            raise StageError(f"{stage} 阶段失败 (exit {rc}): {t.log_tail[-300:]}")
        return b"".join(chunks).decode("utf-8", "replace")

    async def _mosaic(self, t: Task) -> None:
        inputs = sorted(p for p in t.inputs_dir.iterdir() if p.is_file())
        if not inputs:
            raise StageError("没有输入文件")
        res = f"{t.resolution:g}"
        # 输入按优先级从低到高排列:gdalwarp 依次写入,后文件的有效像元覆盖先文件;
        # 各输入自带的 NoData 会被 gdalwarp 识别为掩膜,不会盖掉已有高程。
        cmd = [
            "gdalwarp",
            "-t_srs", TARGET_CRS,
            "-tr", res, res,
            "-r", "bilinear",
            "-dstnodata", DST_NODATA,
            "-ot", "Float32",
            "-of", "GTiff",
            "-co", "TILED=YES",
            "-co", "COMPRESS=DEFLATE",
            "-wm", "256",
            "--config", "GDAL_CACHEMAX", "256",
            "-multi",
            "-wo", "NUM_THREADS=2",
        ]
        cmd += [str(p) for p in inputs]
        cmd.append(str(t.workdir / "mosaic.tif"))
        await self._exec(t, STAGE_MOSAIC, cmd)

    async def _cog(self, t: Task) -> None:
        cmd = [
            "gdal_translate",
            "-of", "COG",
            "-co", "COMPRESS=DEFLATE",
            "-co", "RESAMPLING=AVERAGE",  # 概览重采样
            "-co", "NUM_THREADS=2",
            "mosaic.tif",
            "result.tif",
        ]
        await self._exec(t, STAGE_COG, cmd)

    async def _metadata(self, t: Task) -> None:
        out = await self._exec(t, STAGE_METADATA, ["gdalinfo", "-json", "result.tif"])
        try:
            info = json.loads(out)
            w, h = info["size"]
            gt = info["geoTransform"]
        except (ValueError, KeyError) as e:
            raise StageError(f"解析 gdalinfo 输出失败: {e}")
        xs = [gt[0], gt[0] + w * gt[1]]
        ys = [gt[3], gt[3] + h * gt[5]]
        t.result = {
            "width": w,
            "height": h,
            "extent": [min(xs), min(ys), max(xs), max(ys)],  # EPSG:3857 米
            "crs": TARGET_CRS,
            "file_size_bytes": (t.workdir / "result.tif").stat().st_size,
        }
