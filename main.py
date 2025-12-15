import os, sys, time, subprocess
from pathlib import Path
from dotenv import load_dotenv
import random
import signal  

SCRIPT_FIND_URLS  = "find_urls.py"
SCRIPT_FETCH_POST = "fetch_post.py"
SCRIPT_POST_GAST  = "post_to_gast.py"

BATCH_SIZE            = 10
BATCH_FETCH_RUN       = 2         
SLEEP_BETWEEN_BATCHES = 10 * 60    
SLEEP_BEFORE_GAST     = 0

STOP_USER             = "STOP_USER"   
PY   = sys.executable
ROOT = Path(__file__).parent.resolve()
URL_DIR = ROOT / "data" / "urls"
URL_DIR.mkdir(parents=True, exist_ok=True)

LOCK_FILE   = ROOT / ".run.lock"
LATEST_NAME = "latest_urls.json"

load_dotenv()


def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ensure_singleton():
    """Hindari 2 instance pipeline jalan bareng."""
    if LOCK_FILE.exists():
        try:
            old_pid = int(LOCK_FILE.read_text().strip())
            os.kill(old_pid, 0)
            log(f" Pipeline lain sedang berjalan (PID {old_pid}). Keluar.")
            sys.exit(9)
        except (ValueError, ProcessLookupError):
            log(" Lock ditemukan tapi proses mati. Membersihkan lock.")
            LOCK_FILE.unlink(missing_ok=True)

    LOCK_FILE.write_text(str(os.getpid()))


def release_singleton():
    if LOCK_FILE.exists():
        LOCK_FILE.unlink(missing_ok=True)

def run_capture(cmd: list[str]):
    log(" " + " ".join(cmd))
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        capture_output=True
    )
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="")

    return proc.returncode, proc.stdout.lower(), proc.stderr.lower()

def run_find_phase():
    p = ROOT / SCRIPT_FIND_URLS
    if not p.exists():
        log(f" Script {SCRIPT_FIND_URLS} tidak ditemukan.")
        return False

    log(" Menjalankan fase scraping URL (find_urls.py)...")
    rc, out, err = run_capture([PY, SCRIPT_FIND_URLS])

    if rc != 0:
        log(" find_urls exit dengan error.")
        return False
    log(" Scraping URL selesai.")
    return True

def run_fetch_loop(fast_mode: bool = False):
    """
    fast_mode = True jika:
      - --no-scrape
      - atau --fetch-only
    → BATCH_FETCH_RUN = 1, tanpa jeda 10 menit.
    """
    url_file = URL_DIR / LATEST_NAME
    if not url_file.exists():
        log(" latest_urls.json tidak ditemukan, lewati fetch.")
        return
    if fast_mode:
        max_batch = 3
        sleep_between = SLEEP_BETWEEN_BATCHES
        log("Jalankan main.py tanpa find_urls.py")
    else:
        max_batch = BATCH_FETCH_RUN
        sleep_between = SLEEP_BETWEEN_BATCHES

    batch = 1
    while True:
        if os.path.exists(ROOT / STOP_USER):
            log(" STOP_USER terdeteksi, hentikan loop batch.")
            break
        log(f"\n --- Batch fetch ke-{batch} (max {BATCH_SIZE}) ---")

        rc, out, err = run_capture([
            PY, SCRIPT_FETCH_POST,
            "--urls-file", str(url_file),
            "--max", str(BATCH_SIZE)
        ])

        if rc != 0:
            log(f" ERROR pada batch {batch}, menghentikan loop.")
            break

        if "tidak ada url baru" in out or "0 url" in out:
            log(" Semua URL telah diproses.")
            break

        if batch >= max_batch:
            log(" Batas batch tercapai, stop loop.")
            break

        if sleep_between > 0:
            log(f" Menunggu {sleep_between} detik sebelum batch selanjutnya...")
            time.sleep(sleep_between)
        batch += 1

def run_post_gast():
    p = ROOT / SCRIPT_POST_GAST
    if not p.exists():
        log(" Script post_to_gast.py tidak ditemukan. Lewati.")
        return

    log(" Menjalankan fase POST TO GAST...")
    rc, _, _ = run_capture([PY, SCRIPT_POST_GAST])

    if rc == 0:
        log(" GAST: OK.")
    else:
        log(" GAST exit error.")

def main():
    args = sys.argv[1:]
    no_scrape  = "--no-scrape" in args
    fetch_only = "--fetch-only" in args

    if no_scrape and fetch_only:
        log(" Tidak boleh pakai --no-scrape dan --fetch-only sekaligus.")
        sys.exit(1)

    fast_mode = no_scrape or fetch_only

    ensure_singleton()
    try:
        # Mode default → scrape + fetch + GAS
        # Mode no-scrape → fetch + GAS (tanpa scrape, MODE CEPAT)
        # Mode fetch-only → fetch saja (MODE CEPAT)
        if not no_scrape and not fetch_only:
            ok = run_find_phase()
            if not ok:
                log(" find_urls gagal atau kosong. Stop.")
                return
        else:
            lf = URL_DIR / LATEST_NAME
            if not lf.exists():
                log(" Mode tanpa scrape: latest_urls.json tidak ditemukan.")
                return
        # Delay kecil saja, tidak perlu 2 detik kalau fast_mode
        if not fast_mode:
            time.sleep(2)
        run_fetch_loop(fast_mode=fast_mode)

        # POST TO GAS
        if not fetch_only:
            if SLEEP_BEFORE_GAST > 0:
                log(f" Menunggu {SLEEP_BEFORE_GAST} detik sebelum ke GAS...")
                time.sleep(SLEEP_BEFORE_GAST)
            run_post_gast()

        log("\n === SEMUA PROSES SELESAI ===")

    except KeyboardInterrupt:
        log(" [STOP USER] Dihentikan oleh user (Ctrl+C).")
    finally:
        release_singleton()

if __name__ == "__main__":
    main()
