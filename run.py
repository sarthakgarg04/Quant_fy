# run.py
from __future__ import annotations
import os, socket, subprocess, platform, time


def _port_in_use(p: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", p)) == 0


def _kill_port(p: int) -> bool:
    try:
        if platform.system() == "Darwin":
            result = subprocess.run(["lsof", "-ti", f"tcp:{p}"],
                                    capture_output=True, text=True)
            pids = result.stdout.strip().split()
            for pid in pids:
                subprocess.run(["kill", "-9", pid], capture_output=True)
            return bool(pids)
        else:
            result = subprocess.run(["fuser", "-k", f"{p}/tcp"],
                                    capture_output=True, text=True)
            return result.returncode == 0
    except Exception as e:
        print(f"  ⚠  Could not auto-kill port {p}: {e}")
        return False


def start(app, port: int = None):
    from core.config import PORT
    port = port or PORT

    if _port_in_use(port):
        print(f"  Port {port} is in use — killing stale process…")
        if _kill_port(port):
            time.sleep(0.8)
        if _port_in_use(port):
            port += 1

    print(f"""
╔══════════════════════════════════════════════════╗
║  ⚡  QuantScanner  v3.1                          ║
╠══════════════════════════════════════════════════╣
║  Dashboard  →  http://localhost:{port}              ║
║  API health →  http://localhost:{port}/api/health   ║
╚══════════════════════════════════════════════════╝
""")
    app.run(debug=False, port=port, host="0.0.0.0",
            use_reloader=False, threaded=True)