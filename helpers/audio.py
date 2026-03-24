import subprocess
import tempfile
import json
import os


def is_vbr_mp3(file_path: str) -> bool:
    """Check if an MP3 file is VBR using ffprobe to sample frame bitrates."""
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-select_streams",
                "a:0",
                "-show_entries",
                "packet=duration_time,size",
                "-read_intervals",
                "%+10",  # sample first 10 seconds
                "-print_format",
                "json",
                file_path,
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
        if result.returncode != 0:
            return False

        data = json.loads(result.stdout)
        packets = data.get("packets", [])
        if len(packets) < 2:
            return False

        # calc bitrate per packet
        bitrates = []
        for pkt in packets:
            duration = float(pkt.get("duration_time", 0))
            size = int(pkt.get("size", 0))
            if duration > 0:
                bitrates.append(size * 8 / duration)

        if not bitrates:
            return False

        # allow small tolerance
        min_br = min(bitrates)
        max_br = max(bitrates)
        if min_br <= 0:
            return False
        return (max_br - min_br) / min_br > 0.05
    except (
        subprocess.TimeoutExpired,
        FileNotFoundError,
        json.JSONDecodeError,
        ValueError,
    ):
        return False


def ensure_cbr_mp3(audio_bytes: bytes) -> bytes:
    tmp_in_path = None
    tmp_out_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp_in:
            tmp_in.write(audio_bytes)
            tmp_in_path = tmp_in.name

        if not is_vbr_mp3(tmp_in_path):
            return audio_bytes

        probe = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_format",
                tmp_in_path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        cbr_bitrate = 192000
        if probe.returncode == 0:
            probe_data = json.loads(probe.stdout)
            avg_bitrate = int(probe_data.get("format", {}).get("bit_rate", 192000))
            standard_bitrates = [128000, 160000, 192000, 224000, 256000, 320000]
            cbr_bitrate = min(standard_bitrates, key=lambda x: abs(x - avg_bitrate))

        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp_out:
            tmp_out_path = tmp_out.name

        result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                tmp_in_path,
                "-codec:a",
                "libmp3lame",
                "-b:a",
                str(cbr_bitrate),
                tmp_out_path,
            ],
            capture_output=True,
            timeout=60,
        )

        if result.returncode != 0:
            return audio_bytes  # failed, return original

        with open(tmp_out_path, "rb") as f:
            return f.read()
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError):
        return audio_bytes  # failed, return original
    finally:
        if tmp_in_path and os.path.exists(tmp_in_path):
            os.unlink(tmp_in_path)
        if tmp_out_path and os.path.exists(tmp_out_path):
            os.unlink(tmp_out_path)
