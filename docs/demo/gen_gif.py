#!/usr/bin/env python3
"""Generate docs/assets/demo.gif from a scripted REAL amem session.

Runs the repo's debug binary in a temp HOME (seed -> ingest -> search ->
sync), captures actual stdout, normalizes volatile bits (device ids, ports,
fingerprints, tmp paths), then renders a typing-animation GIF with PIL and
encodes it with ffmpeg (two-pass palette).

Usage: docs/demo/gen_gif.py [path-to-amem]  (default: target/debug/amem)
Output: docs/assets/demo.gif
"""
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

from PIL import Image, ImageDraw, ImageFont

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
AMEM = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "target", "debug", "amem")
OUT = os.path.join(ROOT, "docs", "assets", "demo.gif")

FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
FS = 15  # font size
PAD = 18
COLS = 88
FPS = 10
BG = (12, 11, 11)
FG = (237, 232, 232)
GREEN = (127, 201, 127)
AMBER = (232, 179, 75)
MUTED = (168, 162, 158)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def run(env, *args, timeout=60):
    r = subprocess.run(
        [AMEM, *args], env=env, capture_output=True, text=True, timeout=timeout
    )
    return (r.stdout + r.stderr).strip()


def norm(text, tmp):
    text = text.replace(tmp, "~")
    text = re.sub(r"@[0-9a-f]{8}", "@a1b2c3d4", text)
    text = re.sub(r'"fingerprint":"[^"]+"', '"fingerprint":"SHA256:…"', text)
    text = re.sub(r":\d{4,5}", ":8443", text)
    text = re.sub(r"\d{4}-\d{2}-\d{2}", "2026-10-30", text)
    return text


def capture():
    """Run the scripted session; return [(cmd, [out lines])]."""
    tmp = tempfile.mkdtemp(prefix="amem-demo-")
    store = os.path.join(tmp, "store")
    env = dict(os.environ, AMEM_STORE_DIR=store)
    steps = []

    def wrap(lines):
        wrapped = []
        for ln in lines:
            while len(ln) > COLS:
                wrapped.append(ln[:COLS])
                ln = "  " + ln[COLS:]
            wrapped.append(ln)
        return wrapped[:10]

    def cmd(c, *args):
        out = run(env, *args)
        steps.append((c, wrap(norm(out, tmp).splitlines()[:8])))
        return out

    # seed two commands as the hooks would
    sid = run(env, "history", "start", "--shell", "bash",
              "--cmd", "cargo test --workspace", "--session", "demo").strip()
    run(env, "history", "end", "--id", sid, "--exit", "0")
    sid = run(env, "history", "start", "--shell", "bash",
              "--cmd", "kubectl rollout restart deploy/api", "--session", "demo").strip()
    run(env, "history", "end", "--id", sid, "--exit", "0")

    cmd("$ amem ingest --once", "ingest", "--once", "--no-transcripts")
    cmd('$ amem search "cargo test"', "search", "cargo test")

    srv = os.path.join(tmp, "srv")
    port = free_port()
    ep = f"https://127.0.0.1:{port}/"
    run(env, "sync-server", "--dir", srv, "--init-cert")
    server = subprocess.Popen(
        [AMEM, "sync-server", "--dir", srv, "--listen", f"127.0.0.1:{port}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(1.0)
        out = run(env, "sync", "--endpoint", ep, "--trust-new")
        steps.append((
            f"$ amem sync --endpoint {norm(ep, tmp)} --trust-new",
            wrap(norm(out, tmp).splitlines()[:8]),
        ))
    finally:
        server.terminate()
        server.wait(timeout=10)
    shutil.rmtree(tmp, ignore_errors=True)
    return steps


def build_timeline(steps):
    """Expand steps into frames: each frame = (visible lines, cursor row|None)."""
    frames = []
    screen = []

    def hold(n, cursor=None):
        for _ in range(n):
            frames.append((list(screen), cursor))

    for ci, (cmd, out) in enumerate(steps):
        # blank line between steps (not before first)
        if ci > 0:
            screen.append(("$", ""))
            hold(2)
        # type the command
        typed = ""
        for ch in cmd:
            typed += ch
            frames.append((screen + [("$", typed)], len(screen)))
        screen.append(("$", cmd))
        hold(3)  # pause on full command
        # print output lines, two per frame
        for i in range(0, len(out), 2):
            for ln in out[i:i + 2]:
                screen.append(("", ln[:COLS]))
            hold(1)
        hold(6)  # read pause
    hold(14)  # end hold
    return frames


def colorize(line):
    """Split a rendered line into (text, color) runs."""
    if line.startswith("$ "):
        return [("$ ", GREEN), (line[2:], FG)]
    if line.startswith("[shell ") or line.startswith("[notes "):
        m = re.match(r"(\[[^\]]+\] )(.*?) (@[0-9a-f]{8})$", line)
        if m:
            return [(m.group(1), AMBER), (m.group(2), FG), (" " + m.group(3), MUTED)]
        return [(line, FG)]
    if line.startswith("{"):
        try:
            d = json.loads(line)
            parts, first = [], True
            for k, v in d.items():
                if not first:
                    parts.append((", ", FG))
                parts.append((f'"{k}":', MUTED))
                parts.append((str(v), AMBER if isinstance(v, int) and v else FG))
                first = False
            return [("{", FG)] + parts + [("}", FG)]
        except (ValueError, TypeError):
            return [(line, FG)]
    return [(line, MUTED)]


def render(frames, workdir):
    font = ImageFont.truetype(FONT_PATH, FS)
    tmp_img = Image.new("RGB", (8, 8))
    d0 = ImageDraw.Draw(tmp_img)
    cw = int(d0.textlength("M" * 10, font=font) / 10) + 1
    lh = FS + 8
    width = PAD * 2 + cw * COLS
    # tallest screen decides the canvas
    max_lines = max(len(lines) for lines, _ in frames)
    height = PAD * 2 + lh * max_lines
    paths = []
    for i, (lines, cursor) in enumerate(frames):
        img = Image.new("RGB", (width, height), BG)
        d = ImageDraw.Draw(img)
        y = PAD
        for r, (kind, text) in enumerate(lines):
            x = PAD
            if kind == "$":
                for seg, col in colorize(text):
                    d.text((x, y), seg, font=font, fill=col)
                    x += d.textlength(seg, font=font)
                if cursor == r:
                    d.rectangle([x + 2, y + 2, x + cw - 2, y + lh - 4], fill=FG)
            else:
                for seg, col in colorize(text):
                    d.text((x, y), seg, font=font, fill=col)
                    x += d.textlength(seg, font=font)
            y += lh
        p = os.path.join(workdir, f"f{i:04d}.png")
        img.save(p)
        paths.append(p)
    return width, height


def encode(workdir, n_frames):
    pal = os.path.join(workdir, "pal.png")
    subprocess.run(
        ["ffmpeg", "-y", "-v", "error", "-framerate", str(FPS),
         "-i", os.path.join(workdir, "f%04d.png"),
         "-vf", "palettegen=max_colors=64", pal],
        check=True,
    )
    subprocess.run(
        ["ffmpeg", "-y", "-v", "error", "-framerate", str(FPS),
         "-i", os.path.join(workdir, "f%04d.png"), "-i", pal,
         "-lavfi", "paletteuse=dither=bayer:bayer_scale=4",
         "-gifflags", "-transdiff", OUT],
        check=True,
    )


def main():
    print(f"capturing with {AMEM} ...", flush=True)
    steps = capture()
    for c, o in steps:
        print(f"  {c}  ->  {len(o)} line(s)")
    frames = build_timeline(steps)
    print(f"rendering {len(frames)} frames ...", flush=True)
    workdir = tempfile.mkdtemp(prefix="amem-gif-")
    try:
        w, h = render(frames, workdir)
        print(f"encoding {w}x{h} gif ...", flush=True)
        encode(workdir, len(frames))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    size = os.path.getsize(OUT)
    print(f"wrote {OUT} ({size / 1024:.0f} KiB)")


if __name__ == "__main__":
    main()
