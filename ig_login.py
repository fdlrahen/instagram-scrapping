import os
import time
import random
from pathlib import Path

from dotenv import load_dotenv
from instagrapi import Client

load_dotenv()

ROOT = Path(__file__).parent.resolve()

# Folder khusus untuk semua session akun
SESSION_DIR = ROOT / "sessions"
SESSION_DIR.mkdir(parents=True, exist_ok=True)

# File untuk menyimpan daftar akun yang pernah gagal login
BAD_ACCOUNTS_FILE = SESSION_DIR / "bad_accounts.txt"


# =========================
#   Load Akun Dari .env
# =========================

IG_USER = (os.getenv("IG_USER") or "").strip()
IG_PASS = (os.getenv("IG_PASS") or "").strip()

RAW_ACCOUNTS = (os.getenv("IG_ACCOUNTS") or "").strip()

ACCOUNTS: list[tuple[str, str]] = []

if RAW_ACCOUNTS:
    for pair in RAW_ACCOUNTS.split(","):
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        u, p = pair.split(":", 1)
        u, p = u.strip(), p.strip()
        if u and p:
            ACCOUNTS.append((u, p))

# fallback
if not ACCOUNTS and IG_USER and IG_PASS:
    ACCOUNTS.append((IG_USER, IG_PASS))

IG_PROXY = (os.getenv("IG_PROXY") or "").strip()
ROTATE_HOURS = int(os.getenv("IG_ROTATE_HOURS", "4") or 4)
RANDOM_LOGIN = (os.getenv("IG_RANDOM_LOGIN", "0").strip() == "1")


# =========================
#   Helper: load akun gagal
# =========================

def load_bad_accounts() -> set:
    if not BAD_ACCOUNTS_FILE.exists():
        return set()
    data = BAD_ACCOUNTS_FILE.read_text(encoding="utf-8").splitlines()
    return {x.strip() for x in data if x.strip()}


def save_bad_account(username: str):
    bads = load_bad_accounts()
    bads.add(username)
    BAD_ACCOUNTS_FILE.write_text("\n".join(sorted(bads)), encoding="utf-8")


# =========================
#   Pemilihan akun aman
# =========================

def _pick_account() -> tuple[str, str]:
    """
    Pilih satu akun SAJA untuk 1 run sesuai aturan:
    - Skip akun yang pernah gagal login
    - RANDOM_LOGIN=1 → acak
    - Default: rotasi deterministik tiap N jam
    """
    if not ACCOUNTS:
        raise RuntimeError(
            "Tidak ada akun IG yang dikonfigurasi. Isi IG_USER/IG_PASS atau IG_ACCOUNTS."
        )

    bads = load_bad_accounts()
    valid_accounts = [acc for acc in ACCOUNTS if acc[0] not in bads]

    if not valid_accounts:
        raise RuntimeError(
            "Semua akun IG pernah gagal login. Perbaiki akun atau hapus bad_accounts.txt"
        )

    # random mode
    if RANDOM_LOGIN:
        return random.choice(valid_accounts)

    # rotasi per jam
    slot = int(time.time() // (ROTATE_HOURS * 3600))
    idx = slot % len(valid_accounts)
    return valid_accounts[idx]


def _session_path_for(username: str) -> Path:
    safe = username.replace("@", "_at_").replace(":", "_")
    return SESSION_DIR / f"session_{safe}.json"


def login_instagram() -> Client:
    cl = Client()

    if IG_PROXY:
        print(f"[LOGIN] Pakai proxy: {IG_PROXY}")
        cl.set_proxy(IG_PROXY)

    username, password = _pick_account()
    sess_path = _session_path_for(username)

    print(f"[LOGIN] Akun dipilih: {username}")

    # 1) Gunakan session kalau ada
    if sess_path.exists():
        try:
            print(f"[LOGIN] Load session {sess_path}")
            cl.load_settings(str(sess_path))
            cl.login(username, password)
            cl.get_timeline_feed()
            print(f"[LOGIN] Session OK untuk {username}")
            return cl
        except Exception as e:
            print(f"[WARN] Session rusak untuk {username}: {e}")

    # 2) Login fresh
    print(f"[LOGIN] Login baru sebagai {username}...")
    try:
        cl.login(username, password)
        cl.get_timeline_feed()
    except Exception as e:
        print(f"[ERROR] Login gagal untuk {username}: {e}")
        save_bad_account(username)
        raise

    # 3) Simpan session
    try:
        cl.dump_settings(str(sess_path))
        print(f"[LOGIN] Session disimpan ke {sess_path}")
    except Exception as e:
        print(f"[WARN] Gagal dump session: {e}")

    return cl

if __name__ == "__main__":
    c = login_instagram()
    print("[LOGIN] Selesai. Username:", c.username)
