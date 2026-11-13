#!/usr/bin/env python3
"""TaskManager 调度逻辑测试(无需真实 GDAL / FastAPI)。

在临时目录生成 gdalwarp / gdal_translate / gdalinfo 桩程序并前置到 PATH。
本仓库的运行环境可能禁止向进程发信号(沙箱),因此测试将 os.killpg 替换为
"记录调用 + 写哨兵文件让桩退出"的假实现,以验证编排逻辑;真实的进程组
SIGKILL 在 Docker 容器中生效(见 README 的手动验证步骤)。

运行: python3 -I tests/test_taskmanager.py
"""
import asyncio
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

GDALINFO = """#!/usr/bin/env python3
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


async def wait_for(cond, timeout=10.0, what="condition"):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if cond():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"超时等待: {what}")


async def main():
    tmp = Path(tempfile.mkdtemp(prefix="tm-test-"))
    (tmp / "bin").mkdir()
    make_stubs(tmp / "bin")
    os.environ["PATH"] = str(tmp / "bin") + os.pathsep + os.environ["PATH"]

    # 记录 killpg 调用;并模拟"仅该进程组被终止":在桩的工作目录(任务目录)
    # 写入以被 kill 的 pgid 命名的哨兵文件,对应桩随即退出。
    killpg_calls = []
    real_killpg = os.killpg

    def fake_killpg(pgid, sig):
        killpg_calls.append((pgid, sig))
        for d in (tmp / "tasks").iterdir():
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

        # --- 场景 1:两任务并行,取消其一不影响另一;排队任务随后补位 ---
        t1 = mk("00_slow.tif")          # gdalwarp 挂起,等待取消
        t2 = mk("00.tif")
        t3 = mk("00.tif")
        await wait_for(
            lambda: t1.status == tm.STATUS_RUNNING and t2.status == tm.STATUS_RUNNING,
            what="t1/t2 进入 RUNNING",
        )
        assert t3.status == tm.STATUS_QUEUED, f"t3 应排队,实际 {t3.status}"

        mgr.cancel(t1.id)
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
        mgr.cancel(q.id)
        assert q.status == tm.STATUS_CANCELLED, f"排队任务取消后应立即 CANCELLED,实际 {q.status}"
        assert not q.workdir.exists(), "排队任务取消后应清理暂存"

        # --- 场景 3:COG 阶段取消,不得发布结果;取消后槽位释放 ---
        mgr.cancel(s1.id)
        await wait_for(lambda: s1.status == tm.STATUS_CANCELLED, what="s1 变为 CANCELLED")
        assert s1.result is None and not s1.workdir.exists()
        mgr.cancel(s2.id)
        await wait_for(lambda: s2.status == tm.STATUS_CANCELLED, what="s2 变为 CANCELLED")
        del os.environ["COG_SLOW"]

        # --- 场景 4:取消后不再启动新任务阶段,任务正常完成 ---
        f1 = mk("00.tif")
        await wait_for(lambda: f1.status == tm.STATUS_DONE, what="f1 完成")
        assert f1.result is not None

        print("ALL TASKMANAGER TESTS PASSED")
    finally:
        os.killpg = real_killpg


if __name__ == "__main__":
    asyncio.run(main())
