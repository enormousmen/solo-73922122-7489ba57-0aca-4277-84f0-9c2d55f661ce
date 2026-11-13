#!/usr/bin/env python3
"""TaskManager 调度与"输入准备阶段取消"测试(无需真实 GDAL / FastAPI)。

在临时目录生成 gdalwarp / gdal_translate / gdalinfo 桩程序并前置到 PATH。
本仓库的运行环境可能禁止向进程发信号(沙箱),因此测试将 os.killpg 替换为
"记录调用 + 写哨兵文件让桩退出"的假实现,以验证编排逻辑;真实的进程组
SIGKILL 在 Docker 容器中生效(见 README 的手动验证步骤)。

覆盖:
- 两任务并行、排队补位、取消其一不影响另一个;
- 输入准备(PREPARING)阶段:慢 gdalinfo 取消、写盘窗口取消、重复取消、
  迟到失败不覆盖取消记录、取消后不保存后续输入/不启动后续校验;
- 取消后暂存清理、记录保留、创建侧返回取消结果;
- 普通校验失败仍按 400 路径 discard。

运行: python3 -I tests/test_taskmanager.py
"""
import asyncio
import json
import os
import signal
import stat
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

import taskmanager as tm  # noqa: E402

GDALWARP = """#!/usr/bin/env python3
import os, sys, time
args = sys.argv[1:]
if any("slow" in a for a in args):
    # 等待只针对本进程(组)的哨兵: cwd 是任务目录
    sentinel = f"KILL-{os.getpid()}"
    deadline = time.time() + 60
    while time.time() < deadline:
        if os.path.exists(sentinel):
            sys.exit(0)
        time.sleep(0.05)
else:
    open(args[-1], "wb").write(b"mosaic")
"""

GDAL_TRANSLATE = """#!/usr/bin/env python3
import os, shutil, sys, time
args = sys.argv[1:]
if os.environ.get("COG_SLOW") == "1":
    sentinel = f"KILL-{os.getpid()}"
    deadline = time.time() + 60
    while time.time() < deadline:
        if os.path.exists(sentinel):
            sys.exit(0)
        time.sleep(0.05)
shutil.copyfile(args[-2], args[-1])
"""

# gdalinfo 桩:GDALINFO_SLOW=1 时等待进程组哨兵(模拟慢校验);
# GDALINFO_SLOW_FAIL=1 时限时等待哨兵,超时则以非零退出(模拟慢校验自然失败);
# GDALINFO_FAIL=1 时立即以非零退出(模拟校验失败)。
GDALINFO = """#!/usr/bin/env python3
import os, sys, time
if os.environ.get("GDALINFO_FAIL") == "1":
    sys.stderr.write("stub gdalinfo failure")
    sys.exit(1)
if os.environ.get("GDALINFO_SLOW") == "1" or os.environ.get("GDALINFO_SLOW_FAIL") == "1":
    sentinel = f"KILL-{os.getpid()}"
    deadline = time.time() + (1.0 if os.environ.get("GDALINFO_SLOW_FAIL") == "1" else 60)
    while time.time() < deadline:
        if os.path.exists(sentinel):
            sys.exit(0)
        time.sleep(0.02)
    if os.environ.get("GDALINFO_SLOW_FAIL") == "1":
        sys.stderr.write("stub slow gdalinfo failure")
        sys.exit(1)
print('{"size":[120,80],"geoTransform":[100.0,2.0,0.0,200.0,0.0,-2.0],'
      '"bands":[{}],"coordinateSystem":{"wkt":"PROJCS[stub]"}}')
"""


def make_stubs(root: Path) -> None:
    for name, body in [
        ("gdalwarp", GDALWARP),
        ("gdal_translate", GDAL_TRANSLATE),
        ("gdalinfo", GDALINFO),
    ]:
        p = root / name
        p.write_text(body)
        p.chmod(p.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class FakeUpload:
    """模拟 FastAPI UploadFile 的异步分块读取。

    read_delay > 0 时每个分块前 await 一次,用于稳定制造"写盘进行中"窗口。
    """

    def __init__(self, data: bytes, chunk: int = 4096, read_delay: float = 0.0):
        self._data = data
        self._chunk = chunk
        self._pos = 0
        self._read_delay = read_delay

    async def read(self, n: int = -1):
        if self._pos >= len(self._data):
            return b""
        if self._read_delay:
            await asyncio.sleep(self._read_delay)
        size = self._chunk if n is None or n < 0 else min(n, self._chunk)
        part = self._data[self._pos:self._pos + size]
        self._pos += len(part)
        return part


async def wait_for(cond, timeout=10.0, what="condition"):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"超时等待: {what}")


async def main():
    tmp = Path(tempfile.mkdtemp(prefix="tm-test-"))
    (tmp / "bin").mkdir()
    make_stubs(tmp / "bin")
    old_path = os.environ.get("PATH", "")
    os.environ["PATH"] = str(tmp / "bin") + os.pathsep + old_path

    # 记录 killpg 调用;并模拟"仅该进程组被终止":在桩的工作目录(任务目录)
    # 写入以被 kill 的 pgid 命名的哨兵文件,对应桩随即退出。
    killpg_calls = []
    real_killpg = os.killpg

    def fake_killpg(pgid, sig):
        killpg_calls.append((pgid, sig))
        tasks_dir = tmp / "tasks"
        if tasks_dir.exists():
            for d in tasks_dir.iterdir():
                if d.is_dir():
                    (d / f"KILL-{pgid}").write_text("1")

    os.killpg = fake_killpg
    try:
        mgr = tm.TaskManager(tmp / "tasks", max_parallel=2)

        def mk(input_name, res=2.0):
            t = mgr.create([input_name], res)
            (t.inputs_dir / input_name).write_bytes(b"x")
            mgr.enqueue(t)
            return t

        def sample_items(names):
            # 用已存在的样例源文件走 sample 分支(shutil.copyfile)
            src_dir = tmp / "src"
            src_dir.mkdir(exist_ok=True)
            items = []
            for nm in names:
                p = src_dir / nm
                p.write_bytes(b"sample-bytes")
                items.append(("sample", p, nm))
            return items

        def upload_items(specs):
            # specs 元素: (文件名, 字节数) 或 (文件名, 字节数, 每块延迟秒)
            items = []
            for spec in specs:
                name, size = spec[0], spec[1]
                delay = spec[2] if len(spec) > 2 else 0.0
                items.append(("upload", FakeUpload(b"x" * size, read_delay=delay), name))
            return items

        # --- 场景 1:两任务并行,取消其一不影响另一;排队任务随后补位 ---
        t1 = mk("00_slow.tif")          # gdalwarp 挂起,等待取消
        t2 = mk("00.tif")
        t3 = mk("00.tif")
        await wait_for(
            lambda: t1.status == tm.STATUS_RUNNING and t2.status == tm.STATUS_RUNNING,
            what="t1/t2 进入 RUNNING",
        )
        assert t3.status == tm.STATUS_QUEUED, f"t3 应排队,实际 {t3.status}"

        await mgr.cancel(t1.id)
        await wait_for(lambda: t1.status == tm.STATUS_CANCELLED, what="t1 变为 CANCELLED")
        assert killpg_calls, "取消运行中任务应调用 killpg 终止进程组"
        pgid, sig = killpg_calls[-1]
        assert sig == signal.SIGKILL, f"应发送 SIGKILL,实际 {sig}"
        assert not t1.workdir.exists(), "取消后暂存目录应被清理"
        assert t1.result is None, "取消任务不得发布结果"

        await wait_for(lambda: t2.status == tm.STATUS_DONE, what="t2 完成")
        await wait_for(lambda: t3.status == tm.STATUS_DONE, what="t3 补位完成")
        assert t2.result["width"] == 120 and t2.result["height"] == 80
        assert t2.result["extent"] == [100.0, 40.0, 340.0, 200.0], t2.result["extent"]
        assert t2.result["crs"] == "EPSG:3857"
        assert (t2.workdir / "result.tif").exists(), "成功任务应保留结果文件"

        # --- 场景 2:取消排队任务,立即终态并清理,不占用并发槽 ---
        os.environ["COG_SLOW"] = "1"    # 后续任务会停在 COG 阶段
        s1 = mk("00.tif")
        s2 = mk("00.tif")
        q = mk("00.tif")
        await wait_for(
            lambda: s1.status == tm.STATUS_RUNNING and s2.status == tm.STATUS_RUNNING,
            what="s1/s2 进入 RUNNING",
        )
        assert q.status == tm.STATUS_QUEUED
        await mgr.cancel(q.id)
        assert q.status == tm.STATUS_CANCELLED, f"排队任务取消后应立即 CANCELLED,实际 {q.status}"
        assert not q.workdir.exists(), "排队任务取消后应清理暂存"

        # --- 场景 3:COG 阶段取消,不得发布结果;取消后槽位释放 ---
        await mgr.cancel(s1.id)
        await wait_for(lambda: s1.status == tm.STATUS_CANCELLED, what="s1 变为 CANCELLED")
        assert s1.result is None and not s1.workdir.exists()
        await mgr.cancel(s2.id)
        await wait_for(lambda: s2.status == tm.STATUS_CANCELLED, what="s2 变为 CANCELLED")
        del os.environ["COG_SLOW"]

        # --- 场景 4:取消后不再启动新任务阶段,任务正常完成 ---
        f1 = mk("00.tif")
        await wait_for(lambda: f1.status == tm.STATUS_DONE, what="f1 完成")
        assert f1.result is not None

        # ============ 输入准备(PREPARING)阶段取消 ============

        # --- 场景 5:慢校验中取消——列表可见、可取消,回收进程并清理暂存 ---
        os.environ["GDALINFO_SLOW"] = "1"
        p1 = mgr.create(["00.tif"], 2.0)
        assert p1.id in mgr.tasks, "创建后任务应立即可见于任务列表"
        pre = asyncio.create_task(
            mgr.prepare_inputs(p1, sample_items(["00.tif"]), validate_delay=0)
        )
        await wait_for(
            lambda: p1.preparing and p1.proc is not None and p1.proc.returncode is None,
            what="p1 进入慢校验且子进程在运行",
        )
        before = len(killpg_calls)
        p1_after = await mgr.cancel(p1.id)   # 必须等待终态才返回
        assert p1_after.status == tm.STATUS_CANCELLED, "cancel 应在清理完成后返回 CANCELLED"
        assert p1.status == tm.STATUS_CANCELLED
        assert len(killpg_calls) == before + 1, "取消应终止在途校验进程组"
        with pytest_raises(tm.TaskCancelled):
            await pre
        # 子进程已被 communicate/wait 回收:无残留运行句柄,暂存已清理
        assert p1.proc is None
        assert not p1.workdir.exists(), "取消完成后暂存目录应已清理"
        assert p1.id in mgr.tasks, "取消记录必须保留在列表中"
        assert p1.result is None
        del os.environ["GDALINFO_SLOW"]

        # --- 场景 6:多输入写入窗口取消——当前输入写完即生效,不保存后续输入 ---
        # 第一个上传分块间带延迟,稳定地让取消落在"当前输入写入中"
        slow_big = ("00.tif", 300_000, 0.02)   # 约 74 个分块,写入持续一段时间
        p2 = mgr.create(["00.tif", "01.tif"], 2.0)
        prepare2 = asyncio.create_task(
            mgr.prepare_inputs(p2, upload_items([slow_big, ("01.tif", 1024)]))
        )
        await wait_for(lambda: p2.preparing, what="p2 进入 PREPARING")
        await asyncio.sleep(0.05)            # 确保第一个输入正在写盘
        # 在第一个输入写盘期间请求取消:cancel 等待当前写入结束
        await mgr.cancel(p2.id)
        with pytest_raises(tm.TaskCancelled):
            await prepare2
        assert p2.status == tm.STATUS_CANCELLED
        assert not (p2.inputs_dir / "01.tif").exists(), "取消后不得保存后续输入"
        assert not p2.workdir.exists()
        assert p2.id in mgr.tasks, "取消记录不得消失"

        # --- 场景 7:重复取消与迟到结果不得覆盖/删除取消记录 ---
        os.environ["GDALINFO_SLOW"] = "1"
        p3 = mgr.create(["00.tif"], 2.0)
        prepare3 = asyncio.create_task(
            mgr.prepare_inputs(p3, sample_items(["00.tif"]))
        )
        await wait_for(
            lambda: p3.proc is not None and p3.proc.returncode is None,
            what="p3 校验进程启动",
        )
        r_a = await mgr.cancel(p3.id)
        # 终态后再次取消:直接原样返回,不覆盖、不删除
        r_b = await mgr.cancel(p3.id)
        assert r_a is r_b is p3
        assert p3.status == tm.STATUS_CANCELLED and p3.id in mgr.tasks
        with pytest_raises(tm.TaskCancelled):
            await prepare3
        # 模拟迟到的成功回调:_finalize 拒绝覆盖终态
        mgr._finalize(p3, tm.STATUS_DONE)
        mgr._finalize(p3, tm.STATUS_FAILED, "late")
        assert p3.status == tm.STATUS_CANCELLED, "迟到成功/失败不得覆盖取消记录"
        mgr.discard(p3)
        assert p3.id in mgr.tasks, "discard 不得删除取消记录"
        del os.environ["GDALINFO_SLOW"]

        # --- 场景 8:迟到的校验失败也不得覆盖取消记录 ---
        # SLOW_FAIL 桩:被取消(收到哨兵)则退出 0;若无人取消则 1 秒后失败退出。
        # 这里取消先到,模拟"失败结果迟到",交接处必须判定为取消而非输入失败。
        os.environ["GDALINFO_SLOW_FAIL"] = "1"
        p4 = mgr.create(["00.tif"], 2.0)
        prepare4 = asyncio.create_task(
            mgr.prepare_inputs(p4, sample_items(["00.tif"]))
        )
        await wait_for(
            lambda: p4.proc is not None and p4.proc.returncode is None,
            what="p4 校验进程启动",
        )
        await mgr.cancel(p4.id)
        with pytest_raises(tm.TaskCancelled):
            await prepare4
        assert p4.status == tm.STATUS_CANCELLED and p4.id in mgr.tasks
        assert not p4.workdir.exists()
        del os.environ["GDALINFO_SLOW_FAIL"]

        # --- 场景 9:取消后不启动后续校验(多输入、逐份慢校验) ---
        os.environ["GDALINFO_SLOW"] = "1"
        p5 = mgr.create(["00.tif", "01.tif"], 2.0)
        prepare5 = asyncio.create_task(
            mgr.prepare_inputs(p5, sample_items(["00.tif", "01.tif"]))
        )
        await wait_for(
            lambda: p5.proc is not None and p5.proc.returncode is None,
            what="p5 第一份校验启动",
        )
        await mgr.cancel(p5.id)
        with pytest_raises(tm.TaskCancelled):
            await prepare5
        assert not (p5.inputs_dir / "01.tif").exists(), "取消后不得保存/校验后续输入"
        assert p5.status == tm.STATUS_CANCELLED and not p5.workdir.exists()
        del os.environ["GDALINFO_SLOW"]

        # --- 场景 10:未取消时校验失败走 InputError(对应路由 400 + discard) ---
        os.environ["GDALINFO_FAIL"] = "1"
        p6 = mgr.create(["00.tif"], 2.0)
        with pytest_raises(tm.InputError):
            await mgr.prepare_inputs(p6, sample_items(["00.tif"]))
        assert p6.status not in tm.TERMINAL_STATUSES
        assert not p6.workdir.exists(), "失败后应清理暂存"
        mgr.discard(p6)
        assert p6.id not in mgr.tasks, "普通失败 discard 后记录移除"
        del os.environ["GDALINFO_FAIL"]

        # --- 场景 11:准备阶段取消不影响其他提交;其他任务照常拼接完成 ---
        os.environ["GDALINFO_SLOW"] = "1"
        p7 = mgr.create(["00.tif"], 2.0)
        prep7 = asyncio.create_task(
            mgr.prepare_inputs(p7, sample_items(["00.tif"]))
        )
        await wait_for(
            lambda: p7.proc is not None and p7.proc.returncode is None,
            what="p7 校验进程启动",
        )
        del os.environ["GDALINFO_SLOW"]   # 让之后的任务校验快速通过
        ok = mgr.create(["00.tif"], 2.0)
        prep_ok = asyncio.create_task(
            mgr.prepare_inputs(ok, sample_items(["00.tif"]))
        )
        await prep_ok
        mgr.enqueue(ok)
        await mgr.cancel(p7.id)
        with pytest_raises(tm.TaskCancelled):
            await prep7
        await wait_for(lambda: ok.status == tm.STATUS_DONE, what="ok 照常完成")
        assert p7.status == tm.STATUS_CANCELLED
        assert ok.status == tm.STATUS_DONE and (ok.workdir / "result.tif").exists()

        print("ALL TASKMANAGER TESTS PASSED")
    finally:
        os.killpg = real_killpg
        os.environ["PATH"] = old_path


class _Raises:
    def __init__(self, exc):
        self.exc = exc

    def __enter__(self):
        return self

    def __exit__(self, et, ev, tb):
        assert et is not None and issubclass(et, self.exc), f"应抛出 {self.exc.__name__}"
        return True


def pytest_raises(exc):
    """轻量 pytest.raises 替身,避免第三方依赖。"""
    return _Raises(exc)


if __name__ == "__main__":
    asyncio.run(main())
