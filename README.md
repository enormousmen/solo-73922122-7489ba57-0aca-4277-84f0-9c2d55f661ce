# DEM 拼接服务(FastAPI + GDAL)

把相邻架次的高程栅格拼成统一底图:网页上传最多 **4 份带坐标系的单波段 GeoTIFF**,
调整覆盖顺序、设置目标像元边长(米,>0),后台统一重投影到 **EPSG:3857** 后镶嵌,
再转换为**带概览的 COG**。全部成功才开放下载,并显示范围与尺寸。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`samplegen` 服务会先生成演示样例(写入 `./data/samples`),完成后 `web` 自动启动。
浏览器中可直接勾选样例、调整顺序、提交拼接、取消任务、下载结果,无需准备数据。

> 若无法拉取 `ghcr.io/osgeo/gdal`,把 Dockerfile 第一行改为
> `osgeo/gdal:ubuntu-small-3.9.2`(Docker Hub)。

## 演示样例(不同坐标系 / 重叠 / NoData)

| 文件 | CRS | 说明 |
|---|---|---|
| `dem_A_utm32n.tif` | EPSG:32632 | 2m,中部一块 NoData 空洞 |
| `dem_B_utm32n.tif` | EPSG:32632 | 与 A 重叠约 200m×500m,西侧 NoData 条带 |
| `dem_C_wgs84.tif` | EPSG:4326 | 经纬度(度),覆盖 A 西南角,含 NoData 空洞 |
| `dem_D_webmerc.tif` | EPSG:3857 | 与 B 东北角重叠,四周一圈 NoData 边缘 |

建议体验顺序:勾选 A、B、C、D 加入列表 → 像元边长 `2` → 开始拼接。
把 B 调到 A 之下/之上各跑一次,可看到**列表靠后的文件有效像元优先**,
而 NoData 不会盖掉已有高程。重新生成样例:`FORCE=1 docker compose run --rm samplegen`。

## 处理流水线

每个任务三个 GDAL 阶段,全部流式处理,**不把整幅栅格读入内存**:

1. **MOSAIC** — `gdalwarp -t_srs EPSG:3857 -tr <res> <res> -r bilinear -dstnodata -9999`
   多输入一次镶嵌:自动计算并集范围;输入按列表顺序依次写入,**后文件有效像元覆盖先文件**;
   各输入自带的 NoData 被识别为掩膜,不参与写入,因此不会盖掉已有高程。
2. **COG** — `gdal_translate -of COG -co COMPRESS=DEFLATE -co RESAMPLING=AVERAGE`,
   COG 驱动自动生成概览。
3. **METADATA** — `gdalinfo -json` 读取尺寸与地理变换,计算 EPSG:3857 范围(米)。

只有三个阶段全部成功,任务才进入 `DONE` 并开放下载。

## 并发、取消与资源回收

- 全局信号量限制 **2 个任务并行**,其余排队(`QUEUED`);取消/失败会释放槽位。
- 每个 GDAL 阶段是独立子进程,置于**独立进程组**(`start_new_session`)。
  取消时对该进程组发 `SIGKILL` 并 `await` 回收,只影响本任务,不影响另一并行任务。
- **阶段交接安全**:阶段启动前、子进程退出后、发布结果前都检查取消标志——
  取消后绝不启动下一阶段,也绝不发布迟到结果。
- 失败或取消后,先终止并回收本任务全部 GDAL 进程,再删除该任务暂存目录;
  成功任务保留 `result.tif` 供下载。服务重启时清空遗留暂存。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 网页 UI |
| GET | `/api/samples` | 样例列表 |
| POST | `/api/tasks` | 创建任务(multipart:`resolution`、`plan` JSON、`files`) |
| GET | `/api/tasks` | 任务列表(状态/阶段/结果/日志尾) |
| POST | `/api/tasks/{id}/cancel` | 取消任务 |
| GET | `/api/tasks/{id}/download` | 下载 COG(仅 `DONE`,否则 409) |

`plan` 示例(按覆盖优先级从低到高):
`[{"type":"upload","index":0},{"type":"sample","name":"dem_B_utm32n.tif"}]`

## 目录结构

```
app/main.py          FastAPI 路由(上传校验、任务 API、下载)
app/taskmanager.py   任务调度:并发限制、三阶段流水线、取消与清理(不依赖 FastAPI)
app/static/index.html 单页 UI
samples/generate_samples.py 样例生成(标准库 + gdal_translate)
tests/test_taskmanager.py  调度逻辑测试(GDAL 桩程序,无需真实 GDAL)
data/                运行期数据(样例 + 每任务暂存目录,Compose 自动创建)
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `/data` | 数据根目录 |
| `SAMPLES_DIR` | `$DATA_DIR/samples` | 样例目录 |
| `MAX_PARALLEL` | `2` | 并行任务数 |

注意:任务注册表在进程内,uvicorn 必须单 worker(镜像默认如此)。

## 验证取消时的进程回收(容器内)

```bash
docker compose up --build -d
# 网页提交一个大任务后立即取消,然后:
docker compose exec web ps aux   # 不应残留 gdalwarp/gdal_translate 进程
ls data/tasks/                   # 被取消任务的目录已清理
```

## 本地运行调度测试(无需 GDAL)

```bash
python3 -I tests/test_taskmanager.py
```

覆盖:两任务并行与排队、取消运行中任务(killpg 参数与清理)、取消排队任务、
COG 阶段取消不发布结果、槽位释放。沙箱环境禁止真实信号时,测试用哨兵文件
模拟进程组终止;真实 `SIGKILL` 在 Docker 容器中生效。
