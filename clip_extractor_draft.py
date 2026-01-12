import boto3
import os
import re
import subprocess
import configparser
from datetime import datetime, timedelta
from pathlib import Path

# ===================== CONFIG FILE =====================
CONFIG_PATH = "config.conf"

cfg = configparser.ConfigParser()
cfg.read(CONFIG_PATH)

AWS_REGION = cfg.get("AWS", "REGION")
BUCKET = cfg.get("AWS", "S3_BUCKET")
PREFIX = cfg.get("AWS", "S3_PREFIX")

BEFORE_SEC = cfg.getint("CLIP", "BEFORE_MINUTES") * 60
AFTER_SEC = cfg.getint("CLIP", "AFTER_MINUTES") * 60
LOCAL_WORKDIR = cfg.get("CLIP", "OUTPUT_DIR")

CHUNK_DURATION_SEC = 300
# ======================================================

s3 = boto3.client("s3", region_name=AWS_REGION)

FILENAME_RE = re.compile(
    r"gcam_(\d{2})(\d{2})(\d{4})_(\d{2})(\d{2})(\d{2})\.mp4"
)

def parse_end_time(name: str) -> datetime | None:
    m = FILENAME_RE.match(name)
    if not m:
        return None
    d, mo, y, h, mi, s = map(int, m.groups())
    return datetime(y, mo, d, h, mi, s)

def list_chunks():
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    chunks = []

    for obj in resp.get("Contents", []):
        name = os.path.basename(obj["Key"])
        E = parse_end_time(name)
        if not E:
            continue
        S = E - timedelta(seconds=CHUNK_DURATION_SEC)
        chunks.append({
            "key": obj["Key"],
            "name": name,
            "S": S,
            "E": E
        })

    return sorted(chunks, key=lambda x: x["S"])

def intersects(chunk, W_start, W_end):
    return not (chunk["E"] <= W_start or chunk["S"] >= W_end)

def ff(cmd):
    subprocess.run(cmd, check=True)

def extract_alert_clip(alert_time: datetime):
    W_start = alert_time - timedelta(seconds=BEFORE_SEC)
    W_end = alert_time + timedelta(seconds=AFTER_SEC)

    Path(LOCAL_WORKDIR).mkdir(exist_ok=True)

    chunks = list_chunks()
    selected = [c for c in chunks if intersects(c, W_start, W_end)]

    if not selected:
        print(f"⚠️ WARN: no chunk intersects window {W_start} → {W_end}")
        return None

    parts = []

    for idx, c in enumerate(selected):
        local_mp4 = Path(LOCAL_WORKDIR) / c["name"]
        part_mp4 = Path(LOCAL_WORKDIR) / f"part_{idx}.mp4"

        s3.download_file(BUCKET, c["key"], str(local_mp4))

        start = max(c["S"], W_start)
        end = min(c["E"], W_end)

        ss = (start - c["S"]).total_seconds()
        dur = (end - start).total_seconds()

        ff([
            "ffmpeg", "-y",
            "-ss", f"{ss}",
            "-i", str(local_mp4),
            "-t", f"{dur}",
            "-c", "copy",
            str(part_mp4)
        ])

        parts.append(part_mp4)

    concat_file = Path(LOCAL_WORKDIR) / "concat.txt"
    with open(concat_file, "w") as f:
        for p in parts:
            f.write(f"file '{p.absolute()}'\n")

    output = Path(LOCAL_WORKDIR) / f"alert_{alert_time:%Y%m%d_%H%M%S}.mp4"

    ff([
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_file),
        "-c", "copy",
        str(output)
    ])

    print(f"✅ Final clip: {output}")
    return output

# ===================== USAGE =====================
if __name__ == "__main__":
    alert = datetime(2025, 12, 22, 7, 50, 30)
    extract_alert_clip(alert)
