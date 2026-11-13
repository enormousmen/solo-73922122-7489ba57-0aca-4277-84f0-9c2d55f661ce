"""后台拼接任务调度。

流水线:PREPARING(逐输入写盘 + gdalinfo 慢校验)-> MOSAIC(gdalwarp 重投影+镶嵌)
-> COG(gdal_translate 生成带概览的 COG)-> METADATA(gdalinfo 读取范围/尺寸)。
全部成功才发布结果。

设计要点:
- 全局信号量限制并行任务数(默认 2),其余任务排队。
- 任务在 manager.create() 后立即进入注册表并对列表可见,因此输入准备阶段
  (PREPARING)也可以取消。
- 取消的安全点:每个输入写入结束之后、校验进程启动/退出的交接处、每个 GDAL
  阶段启动前与退出后、发布结果前。取消后不再保存后续输入、不启动后续校验或
  拼接阶段,也绝不发布迟到结果。
- 取消语义:cancel() 仅置标志并终止当前子进程,然后等待执行方在安全点
  回收进程、清理暂存并把任务置为 CANCELLED 后才返回;因此调用方看到的
  取消结果一定是终态。重复取消与迟到的成功/失败都不会覆盖或删除取消记录。
- 每个子进程都放在自己的进程组里(start_new_session),取消时按进程组
  SIGKILL 并 await 回收,不影响其他任务的进程。
- 失败/取消后清理该任务的暂存目录;完成的任务保留结果供下载。
- 像素数据全程由 GDAL 流式处理,不把整幅栅格读入内存。
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import sys
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

STAGE_PREPARING = "PREPARING"
STAGE_MOSAIC = "MOSAIC"
STAGE_COG = "COG"
STAGE_METADATA = "METADATA"

TARGET_CRS = "EPSG:3857"
DST_NODATA = "-9999"

MAX_TASKS_KEPT = 200  # 内存中保留的最大任务记录数,超出时回收最旧的终态任务
# cancel() 等待执行方回收进程/清理暂存的最长时间,超时后返回当前状态
CANCEL_WAIT_TIMEOUT = float(os.environ.get("CANCEL_WAIT_TIMEOUT", "30"))
# 单次校验允许的最大额外延迟(秒),防止演示参数把请求拖得过久
MAX_VALIDATE_DELAY = 30.0

SLOW_WRAPPER = Path(__file__).with_name("slow_gdalinfo_wrapper.py")


class StageError(Exception):
    """某个 GDAL 阶段执行失败。"""


class InputError(Exception):
    """输入准备阶段(保存/校验)失败。"""


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
    preparing: bool = False  # 是否处于输入准备(写盘/慢校验)阶段
    proc: Optional[asyncio.subprocess.Process] = None
    # 进入终态时置位,cancel() 据此等待"进程回收 + 暂存清理"全部完成
    terminal_event: asyncio.Event = field(default_factory=asyncio.Event)

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
            "log_tail": log_tail[-400:] if (log_tail := self.log_tail) else "",
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
        """建立任务记录与暂存目录(尚未调度)。

        记录立即进入注册表,任务马上对任务列表可见,因此即使还在上传/校验
        阶段也能被取消。上传校验失败可调用 discard。
        """
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
        """任务尚未入队且非终态时放弃(如上传校验失败)。

        已进入终态(尤其是 CANCELLED)的记录受保护,不会被迟到的保存失败
        从列表中删除。
        """
        if t.status in TERMINAL_STATUSES:
            return
        self.tasks.pop(t.id, None)
        shutil.rmtree(t.workdir, ignore_errors=True)

    async def cancel(self, task_id: str) -> Optional[Task]:
        """请求取消并等待取消完成。

        - 置取消标志,终止本任务当前子进程(独立进程组,不影响其他任务);
        - 纯排队任务直接置 CANCELLED 并清理;
        - PREPARING/RUNNING 任务由执行方在安全点(当前输入写入结束、子进程
          回收之后)清理暂存并置终态,本方法等待该终态后才返回;
        - 已终态任务(含重复取消、迟到结果)原样返回,不覆盖既有记录。
        """
        t = self.tasks.get(task_id)
        if t is None:
            return None
        if t.status in TERMINAL_STATUSES:
            return t
        t.cancel_requested = True
        self._kill_proc(t)
        if t.status == STATUS_QUEUED and not t.preparing:
            # 仅在排队等待并发槽:没有在途写入/子进程,可立即终态并清理
            self._finalize(t, STATUS_CANCELLED)
            return t
        # 输入准备中或拼接运行中:等待执行方完成进程回收与暂存清理
        try:
            await asyncio.wait_for(
                t.terminal_event.wait(), timeout=CANCEL_WAIT_TIMEOUT
            )
        except asyncio.TimeoutError:
            pass
        return t

    def cleanup_stale(self) -> None:
        """启动时清空上次运行遗留的暂存目录(内存注册表已重置)。"""
        for child in self.tasks_root.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)

    # ---------- 输入准备(写盘 + 慢校验) ----------

    async def prepare_inputs(
        self, t: Task, items: list, validate_delay: float = 0.0
    ) -> None:
        """逐输入写盘并做 gdalinfo 校验。

        items 为 (kind, ref, display_name) 列表,顺序即覆盖优先级从低到高:
        kind="upload" 时 ref 是带 async read() 的上传对象;
        kind="sample" 时 ref 是样例文件的源 Path。

        取消安全点:每个输入写入结束后、校验进程启动后与回收后。取消后
        不再保存后续输入、不启动后续校验,也不会入队进入拼接。
        失败或取消都会先终止/回收可能在途的校验进程,再清理暂存。
        """
        t.preparing = True
        t.stage = STAGE_PREPARING
        try:
            for pos, (kind, ref, display) in enumerate(items):
                self._raise_if_cancelled(t)
                dst = t.inputs_dir / f"{pos:02d}.tif"
                if kind == "upload":
                    await self._save_upload(ref, dst)
                else:
                    shutil.copyfile(ref, dst)
                # 当前输入已落盘:此处取消即刻生效,后续输入不再保存
                self._raise_if_cancelled(t)
                await self._validate_geotiff(t, dst, display, validate_delay)
                # 校验进程已回收:交接处再检查,迟到成功不得覆盖取消
                self._raise_if_cancelled(t)
        except TaskCancelled:
            await self._abort_prepare(t)
            raise
        except Exception as e:  # noqa: BLE001 - 统一转换为输入错误
            await self._abort_prepare(t)
            if t.status == STATUS_CANCELLED:
                # 失败与取消竞态:取消记录优先,不得改写为 FAILED
                raise TaskCancelled() from e
            raise InputError(str(e)) from e
        finally:
            t.preparing = False

    async def _abort_prepare(self, t: Task) -> None:
        """终止并回收在途校验进程;按取消/失败清理暂存。"""
        proc = t.proc
        if proc is not None and proc.returncode is None:
            self._kill_proc(t)
            try:
                await proc.wait()  # 回收子进程,避免僵尸
            except (ProcessLookupError, ChildProcessError):
                pass
        t.proc = None
        if t.status not in TERMINAL_STATUSES:
            if t.cancel_requested:
                self._finalize(t, STATUS_CANCELLED)
            else:
                # 普通准备失败:清暂存;注册表记录交由 create 路由 discard 移除
                shutil.rmtree(t.workdir, ignore_errors=True)

    async def _validate_geotiff(
        self, t: Task, path: Path, display: str, delay: float
    ) -> None:
        """启动 gdalinfo 校验(可经慢校验包装器延迟)。

        进程在独立进程组中启动;启动交接处与退出后都检查取消标志,
        取消时随进程组一起被终止并由 prepare_inputs 回收。
        """
        delay = min(MAX_VALIDATE_DELAY, max(0.0, float(delay)))
        if delay > 0:
            cmd = [
                sys.executable or "python3",
                str(SLOW_WRAPPER),
                f"{delay:g}",
                "gdalinfo",
                "-json",
                str(path),
            ]
        else:
            cmd = ["gdalinfo", "-json", str(path)]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(t.workdir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,  # 独立进程组,便于整组终止
            )
        except FileNotFoundError:
            raise InputError(f"找不到校验工具: {cmd[0]}")
        t.proc = proc
        if t.cancel_requested:
            # 取消恰好发生在进程创建窗口:立即终止,不进入 communicate
            self._kill_proc(t)
        out, err = await proc.communicate()  # 等待并回收子进程
        t.proc = None
        if t.cancel_requested or t.status in TERMINAL_STATUSES:
            raise TaskCancelled()
        if proc.returncode != 0:
            err_msg = err.decode("utf-8", "replace")[-200:]
            raise InputError(f"文件 {display}: gdalinfo 无法读取 ({err_msg})")
        try:
            info = json.loads(out)
        except ValueError:
            raise InputError(f"文件 {display}: gdalinfo 输出无法解析")
        bands = info.get("bands") or []
        if len(bands) != 1:
            raise InputError(
                f"文件 {display}: 需要单波段 GeoTIFF,实际 {len(bands)} 波段"
            )
        if not ((info.get("coordinateSystem") or {}).get("wkt")):
            raise InputError(f"文件 {display}: 缺少坐标系(CRS)定义")

    @staticmethod
    async def _save_upload(up, dst: Path) -> None:
        """分块写盘,不把整个文件读入内存。"""
        with open(dst, "wb") as f:
            while True:
                chunk = await up.read(1 << 20)
                if not chunk:
                    break
                f.write(chunk)

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
            # 子进程在独立会话/进程组中,整组 SIGKILL,回收本任务全部子进程
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except (ProcessLookupError, PermissionError):
                pass

    def _raise_if_cancelled(self, t: Task) -> None:
        if t.cancel_requested or t.status in TERMINAL_STATUSES:
            raise TaskCancelled()

    def _finalize(self, t: Task, status: str, error: Optional[str] = None) -> None:
        if t.status in TERMINAL_STATUSES:
            return  # 已被取消端点等置为终态,不覆盖
        t.status = status
        t.stage = None
        t.error = error
        if status in (STATUS_FAILED, STATUS_CANCELLED):
            shutil.rmtree(t.workdir, ignore_errors=True)
        t.terminal_event.set()

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
