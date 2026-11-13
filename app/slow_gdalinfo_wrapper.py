#!/usr/bin/env python3
"""慢校验包装器:先可控延迟,再 exec 真实 gdalinfo。

用途:演示输入准备阶段的慢校验。延迟期间 gdalinfo 尚未启动,进程停留在
本包装器中;它由 TaskManager 以 start_new_session 启动并作为进程组组长,
取消时对该进程组发 SIGKILL,exec 之后同一 PID/进程组的 gdalinfo 也会被
一起终止并回收,不会留下运行中的孤儿 gdalinfo。

用法: slow_gdalinfo_wrapper.py <延迟秒> <gdalinfo 参数...>
例如: slow_gdalinfo_wrapper.py 8 gdalinfo -json /data/tasks/xxx/inputs/00.tif
"""
import os
import sys
import time


def main() -> int:
    if len(sys.argv) < 3:
        sys.stderr.write("usage: slow_gdalinfo_wrapper.py <delay_sec> <cmd> [args...]\n")
        return 2
    try:
        delay = max(0.0, float(sys.argv[1]))
    except ValueError:
        delay = 0.0
    cmd = sys.argv[2:]
    # 可被信号中断(SIGKILL 直接终止);time.sleep 不吞信号
    end = time.monotonic() + delay
    while time.monotonic() < end:
        time.sleep(min(0.1, end - time.monotonic()))
    # 同一进程/进程组映像替换为真实 gdalinfo,保持 PID 与进程组不变
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    sys.exit(main())
