#!/usr/bin/env python3
"""生成演示用 DEM 样例:不同坐标系、相互重叠、含 NoData。

仅依赖 Python 标准库 + gdal_translate(GDAL CLI):
先写 ESRI ASCII Grid(.asc),再转换为单波段 Float32 GeoTIFF 并设置 CRS/NoData。

用法: python3 generate_samples.py [输出目录]
"""
import json
import math
import os
import subprocess
import sys
from pathlib import Path

NODATA = -9999.0


# ---------- 坐标换算(WGS84,标准公式,足够演示精度) ----------

def utm_to_latlon(easting: float, northing: float, zone: int = 32):
    """UTM 北半球 -> WGS84 经纬度(度)。"""
    a = 6378137.0
    f = 1 / 298.257223563
    e2 = f * (2 - f)
    ep2 = e2 / (1 - e2)
    k0 = 0.9996
    x = easting - 500000.0
    y = northing
    lon0 = math.radians(zone * 6 - 183)
    m = y / k0
    mu = m / (a * (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256))
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
    phi1 = (
        mu
        + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * math.sin(2 * mu)
        + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * math.sin(4 * mu)
        + (151 * e1 ** 3 / 96) * math.sin(6 * mu)
        + (1097 * e1 ** 4 / 512) * math.sin(8 * mu)
    )
    s1, c1 = math.sin(phi1), math.cos(phi1)
    t1 = math.tan(phi1) ** 2
    n1 = a / math.sqrt(1 - e2 * s1 * s1)
    cp = ep2 * c1 * c1
    r1 = a * (1 - e2) / (1 - e2 * s1 * s1) ** 1.5
    d = x / (n1 * k0)
    lat = phi1 - (n1 * math.tan(phi1) / r1) * (
        d ** 2 / 2
        - (5 + 3 * t1 + 10 * cp - 4 * cp ** 2 - 9 * ep2) * d ** 4 / 24
        + (61 + 90 * t1 + 298 * cp + 45 * t1 ** 2 - 252 * ep2 - 3 * cp ** 2) * d ** 6 / 720
    )
    lon = lon0 + (
        d
        - (1 + 2 * t1 + cp) * d ** 3 / 6
        + (5 - 2 * cp + 28 * t1 - 3 * cp ** 2 + 8 * ep2 + 24 * t1 ** 2) * d ** 5 / 120
    ) / c1
    return math.degrees(lat), math.degrees(lon)


def latlon_to_merc(lat: float, lon: float):
    """WGS84 经纬度(度)-> EPSG:3857(米)。"""
    r = 6378137.0
    return r * math.radians(lon), r * math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))


# ---------- 栅格写出 ----------

def write_asc(path: Path, ncols, nrows, xll, yll, cs, value_fn, nodata_mask):
    with open(path, "w", encoding="ascii") as f:
        f.write(f"ncols {ncols}\n")
        f.write(f"nrows {nrows}\n")
        f.write(f"xllcorner {xll!r}\n")
        f.write(f"yllcorner {yll!r}\n")
        f.write(f"cellsize {cs!r}\n")
        f.write(f"NODATA_value {int(NODATA)}\n")
        for r in range(nrows):
            row = [
                str(int(NODATA)) if nodata_mask(r, c) else f"{value_fn(r, c):.2f}"
                for c in range(ncols)
            ]
            f.write(" ".join(row) + "\n")


def main():
    out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "/data/samples")
    out_dir.mkdir(parents=True, exist_ok=True)
    force = os.environ.get("FORCE", "") == "1"

    lat_a, lon_a = utm_to_latlon(500000.0, 5480000.0)      # A 西南角
    lat_b, lon_b = utm_to_latlon(501000.0, 5480700.0)      # B 东北角
    mx_b, my_b = latlon_to_merc(lat_b, lon_b)

    jobs = [
        dict(
            name="dem_A_utm32n.tif", srs="EPSG:32632",
            ncols=300, nrows=300, xll=500000.0, yll=5480000.0, cs=2.0,
            desc="UTM 32N,2m,中部一块 NoData 空洞",
            fn=lambda r, c: 100 + 0.02 * c + 0.01 * r + 5 * math.sin(c / 20),
            mask=lambda r, c: 40 <= r < 80 and 200 <= c < 240,
        ),
        dict(
            name="dem_B_utm32n.tif", srs="EPSG:32632",
            ncols=300, nrows=300, xll=500400.0, yll=5480100.0, cs=2.0,
            desc="UTM 32N,与 A 重叠约 200m×500m,西侧 NoData 条带",
            fn=lambda r, c: 140 + 0.03 * c - 0.02 * r + 8 * math.cos(r / 25),
            mask=lambda r, c: 150 <= r < 190 and c < 50,
        ),
        dict(
            name="dem_C_wgs84.tif", srs="EPSG:4326",
            ncols=350, nrows=250, xll=lon_a - 0.012, yll=lat_a - 0.006, cs=0.0002,
            desc="WGS84 经纬度,覆盖 A 西南角,含 NoData 空洞",
            fn=lambda r, c: 120 + 0.05 * c + 0.03 * r + 4 * math.sin((c + r) / 40),
            mask=lambda r, c: 100 <= r < 130 and 120 <= c < 170,
        ),
        dict(
            name="dem_D_webmerc.tif", srs="EPSG:3857",
            ncols=300, nrows=300, xll=mx_b - 100.0, yll=my_b - 200.0, cs=2.0,
            desc="EPSG:3857,与 B 东北角重叠,四周一圈 NoData 边缘",
            fn=lambda r, c: 160 + 0.04 * c + 6 * math.sin((c + r) / 30),
            mask=lambda r, c: r < 12 or c < 12 or r >= 288 or c >= 288,
        ),
    ]

    manifest = {}
    for j in jobs:
        tif = out_dir / j["name"]
        if tif.exists() and not force:
            print(f"[samples] {j['name']} 已存在,跳过", flush=True)
        else:
            asc = out_dir / (j["name"] + ".asc")
            write_asc(asc, j["ncols"], j["nrows"], j["xll"], j["yll"], j["cs"], j["fn"], j["mask"])
            cmd = [
                "gdal_translate", "-q", "-of", "GTiff",
                "-co", "COMPRESS=DEFLATE",
                "-ot", "Float32",
                "-a_srs", j["srs"],
                "-a_nodata", str(int(NODATA)),
                str(asc), str(tif),
            ]
            subprocess.run(cmd, check=True)
            asc.unlink()
            print(f"[samples] {j['name']} ({j['srs']}) 生成完毕", flush=True)
        manifest[j["name"]] = j["desc"]

    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[samples] 全部样例就绪: {out_dir}", flush=True)


if __name__ == "__main__":
    main()
