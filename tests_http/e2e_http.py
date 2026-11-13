#!/usr/bin/env python3
"""HTTP 级端到端测试:FastAPI 路由 + TaskManager(无需真实 GDAL)。

用桩 gdalinfo(支持慢/失败)前置到 PATH,通过 TestClient 验证:
- 慢校验期间 POST /cancel 等待取消完成,创建/取消后记录保留为 CANCELLED;
- 重复取消返回同一条 CANCELLED 记录,不报错、不删除;
- 校验失败返回 400,且任务记录被移除(普通失败语义);
- 取消一份提交不影响另一份,后者可正常 DONE 并下载;
- 任务列表在各阶段都能查询。

运行: /tmp/venv/bin/python -I tests_http/e2e_http.py
"""
import asyncio
import json
import os
import signal
import stat
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "app"))

GDALINFO = """#!/usr/bin/env python3
import os, sys, time
mode = os.environ.get("GDALINFO_MODE", "ok")
if mode in ("slow", "slow_fail"):
    sentinel = f"KILL-{os.getpid()}"
    deadline = time.time() + (1.0 if mode == "slow_fail" else 60)
    while time.time() < deadline:
        if os.path.exists(sentinel):
            sys.exit(0)
        time.sleep(0.02)
    if mode == "slow_fail":
        sys.stderr.write("stub failure"); sys.exit(1)
elif mode == "fail":
    sys.stderr.write("stub failure"); sys.exit(1)
print('{"size":[120,80],"geoTransform":[100.0,2.0,0.0,200.0,0.0,-2.0],'
      '"bands":[{}],"coordinateSystem":{"wkt":"PROJCS[stub]"}}')
"""

GDALWARP = """#!/usr/bin/env python3
import sys
open(sys.argv[-1], "wb").write(b"mosaic")
"""
GDAL_TRANSLATE = """#!/usr/bin/env python3
import shutil, sys
shutil.copyfile(sys.argv[-2], sys.argv[-1])
"""


def make_stubs(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    for name, body in [
        ("gdalinfo", GDALINFO),
        ("gdalwarp", GDALWARP),
        ("gdal_translate", GDAL_TRANSLATE),
    ]:
        p = root / name
        p.write_text(body)
        p.chmod(p.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def main():
    tmp = Path(tempfile.mkdtemp(prefix="http-test-"))
    make_stubs(tmp / "stubbin")
    data_dir = tmp / "data"
    (data_dir / "samples").mkdir(parents=True)
    sample = data_dir / "samples" / "dem_A.tif"
    sample.write_bytes(b"sample")
    (data_dir / "samples" / "manifest.json").write_text("{}", encoding="utf-8")

    os.environ["PATH"] = str(tmp / "stubbin") + os.pathsep + os.environ["PATH"]
    os.environ["DATA_DIR"] = str(data_dir)
    os.environ["SAMPLES_DIR"] = str(data_dir / "samples")
    os.environ["MAX_PARALLEL"] = "2"
    os.environ["GDALINFO_MODE"] = "slow"

    # 假 killpg:写哨兵让慢桩退出,模拟整组 SIGKILL
    real_killpg = os.killpg
    killpg = {"count": 0}

    def fake_killpg(pgid, sig):
        killpg["count"] += 1
        for d in (data_dir / "tasks").iterdir():
            if d.is_dir():
                (d / f"KILL-{pgid}").write_text("1")

    os.killpg = fake_killpg

    from fastapi.testclient import TestClient
    import main as web

    with TestClient(web.app) as client:
        # ---------- 1. 慢校验期间取消 ----------
        plan = json.dumps([{"type": "sample", "name": "dem_A.tif"}])
        created = {}
        def submit():
            r = client.post(
                "/api/tasks",
                data={"resolution": "2", "plan": plan,
                      "slow_validate": "true", "validate_delay": "0.3"},
            )
            created["resp"] = (r.status_code, r.json())

        th = threading.Thread(target=submit)
        th.start()

        # 等待任务在列表中可见(PREPARING)
        tid = None
        deadline = time.time() + 10
        while time.time() < deadline:
            arr = client.get("/api/tasks").json()
            if arr:
                tid = arr[0]["id"]
                assert arr[0]["status"] in ("QUEUED", "RUNNING"), arr[0]
                break
            time.sleep(0.02)
        assert tid, "准备阶段任务应在列表可见"

        # 列表可见即可取消:cancel 等待终态
        rc = client.post(f"/api/tasks/{tid}/cancel")
        assert rc.status_code == 200
        body = rc.json()
        assert body["status"] == "CANCELLED", body
        th.join(timeout=10)
        # 创建请求返回的就是该取消结果
        sc, resp = created["resp"]
        assert sc == 200 and resp["status"] == "CANCELLED" and resp["id"] == tid, created

        # 取消记录仍在列表,暂存已清理
        arr = client.get("/api/tasks").json()
        assert any(x["id"] == tid and x["status"] == "CANCELLED" for x in arr)
        assert not (data_dir / "tasks" / tid).exists()

        # 重复取消:返回同一条记录,不报错不删除
        rc2 = client.post(f"/api/tasks/{tid}/cancel")
        assert rc2.status_code == 200 and rc2.json()["status"] == "CANCELLED"
        assert any(x["id"] == tid for x in client.get("/api/tasks").json())

        # 不存在的任务取消 -> 404
        assert client.post("/api/tasks/nonexistent/cancel").status_code == 404

        # ---------- 1b. 多文件上传,慢校验中取消:不保存后续输入 ----------
        os.environ["GDALINFO_MODE"] = "slow"
        plan_up = json.dumps([
            {"type": "upload", "index": 0},
            {"type": "upload", "index": 1},
        ])
        up_created = {}
        def submit_upload():
            rr = client.post(
                "/api/tasks",
                data={"resolution": "2", "plan": plan_up,
                      "slow_validate": "true", "validate_delay": "0.4"},
                files=[("files", ("a.tif", b"AAAA", "image/tiff")),
                       ("files", ("b.tif", b"BBBB", "image/tiff"))],
            )
            up_created["resp"] = (rr.status_code, rr.json())
        thu = threading.Thread(target=submit_upload)
        thu.start()
        up_id = None
        deadline = time.time() + 10
        while time.time() < deadline:
            ids = {x["id"] for x in client.get("/api/tasks").json()}
            new_ids = ids - {tid}
            if new_ids:
                up_id = next(iter(new_ids))
                break
            time.sleep(0.02)
        assert up_id, "上传任务应在准备阶段可见"
        rcu = client.post(f"/api/tasks/{up_id}/cancel")
        assert rcu.status_code == 200 and rcu.json()["status"] == "CANCELLED"
        thu.join(timeout=10)
        sc, resp = up_created["resp"]
        assert sc == 200 and resp["status"] == "CANCELLED" and resp["id"] == up_id
        # 两次慢校验取消都必须终止在途的校验进程组(无残留 gdalinfo/包装器)
        assert killpg["count"] >= 2, f"应至少终止两个校验进程组,实际 {killpg['count']}"
        # 取消后暂存整体清理;清理前第一份已写盘、第二份不应写盘
        assert not (data_dir / "tasks" / up_id).exists()
        os.environ["GDALINFO_MODE"] = "ok"

        # ---------- 2. 普通校验失败 -> 400 且记录移除 ----------
        os.environ["GDALINFO_MODE"] = "fail"
        r = client.post(
            "/api/tasks",
            data={"resolution": "2", "plan": plan},
        )
        assert r.status_code == 400, r.text
        remaining = client.get("/api/tasks").json()
        # 失败的新任务被移除;此前的取消记录保持 CANCELLED 且不受影响
        assert all(x["status"] == "CANCELLED" for x in remaining), remaining
        assert {x["id"] for x in remaining} == {tid, up_id}, remaining

        # ---------- 3. 取消不影响另一提交,后者正常 DONE 并可下载 ----------
        # 新提交走快速校验:prepare 在请求内完成后入队,随后跑完三阶段
        os.environ["GDALINFO_MODE"] = "ok"
        r = client.post("/api/tasks", data={"resolution": "2", "plan": plan})
        assert r.status_code == 200
        ok_id = r.json()["id"]
        deadline = time.time() + 10
        status = None
        while time.time() < deadline:
            arr = {x["id"]: x for x in client.get("/api/tasks").json()}
            status = arr[ok_id]["status"]
            if status == "DONE":
                break
            time.sleep(0.05)
        assert status == "DONE", status
        dl = client.get(f"/api/tasks/{ok_id}/download")
        assert dl.status_code == 200 and dl.content == b"mosaic"
        # 被取消任务的下载应被拒绝,且记录仍在
        assert client.get(f"/api/tasks/{tid}/download").status_code == 409
        assert any(x["id"] == tid and x["status"] == "CANCELLED"
                   for x in client.get("/api/tasks").json())

    os.killpg = real_killpg
    print("ALL HTTP E2E TESTS PASSED")


if __name__ == "__main__":
    main()
