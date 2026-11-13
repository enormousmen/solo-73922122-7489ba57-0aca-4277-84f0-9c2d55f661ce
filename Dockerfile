# GDAL 官方镜像(含 gdalwarp / gdal_translate / gdalinfo CLI 与 python3)
# 若无法访问 ghcr.io,可改为 Docker Hub 的 osgeo/gdal:ubuntu-small-3.9.2
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.9.2

ENV PYTHONUNBUFFERED=1 \
    PIP_BREAK_SYSTEM_PACKAGES=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
      "fastapi>=0.110" \
      "uvicorn[standard]>=0.29" \
      "python-multipart>=0.0.9"

WORKDIR /app
COPY app/ /app/
COPY samples/ /samples/

ENV DATA_DIR=/data \
    SAMPLES_DIR=/data/samples \
    MAX_PARALLEL=2

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
