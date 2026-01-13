#!/usr/bin/env python3
import argparse
import subprocess
import shlex
import time
import os
from datetime import datetime


def get_rss_kb(pid: int) -> int:
    """Lee /proc/<pid>/status y devuelve VmRSS en kB. Si no existe, lanza FileNotFoundError."""
    status_path = f"/proc/{pid}/status"
    with open(status_path, "r") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                parts = line.split()
                # formato: VmRSS:\t  123456 kB
                try:
                    return int(parts[1])
                except Exception:
                    return 0
    return 0


def main():
    parser = argparse.ArgumentParser(description="Ejecuta un comando y registra uso de memoria (VmRSS).")
    parser.add_argument("--cmd", default="python run.py", help="Comando a ejecutar (entre comillas si tiene espacios)")
    parser.add_argument("--interval", type=float, default=1.0, help="Intervalo de muestreo en segundos")
    parser.add_argument("--out", default="memory_log.txt", help="Fichero de salida con muestras")
    args = parser.parse_args()

    cmd = args.cmd
    interval = max(0.1, args.interval)
    out_path = args.out

    print(f"Ejecutando: {cmd}")

    # Lanzar proceso
    p = subprocess.Popen(shlex.split(cmd))
    pid = p.pid
    print(f"PID: {pid}")

    max_rss = 0
    start = time.time()

    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("timestamp,elapsed_s,rss_kb\n")
        try:
            while True:
                if p.poll() is not None:
                    # proceso terminado
                    try:
                        rss = get_rss_kb(pid)
                    except FileNotFoundError:
                        rss = 0
                    elapsed = time.time() - start
                    fh.write(f"{datetime.utcnow().isoformat()},{elapsed:.1f},{rss}\n")
                    if rss > max_rss:
                        max_rss = rss
                    break

                try:
                    rss = get_rss_kb(pid)
                except FileNotFoundError:
                    rss = 0

                elapsed = time.time() - start
                fh.write(f"{datetime.utcnow().isoformat()},{elapsed:.1f},{rss}\n")
                if rss > max_rss:
                    max_rss = rss

                fh.flush()
                time.sleep(interval)

        except KeyboardInterrupt:
            print("Interrumpido por usuario. Terminando proceso hijo...")
            try:
                p.terminate()
            except Exception:
                pass

    rc = p.poll()
    print(f"Proceso finalizado (rc={rc}). Max RSS = {max_rss} kB (~{max_rss/1024:.1f} MiB)")
    print(f"Muestras guardadas en: {out_path}")


if __name__ == '__main__':
    main()
