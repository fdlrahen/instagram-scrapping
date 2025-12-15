# ====================== fetch_post.py (patched) ======================
"""Ambil detail postingan IG dari daftar URL, simpan ke JSON, lalu kirim ke DB
lewat send_sql.upsert_posts.

- Login IG via ig_login.login_instagram()
- Tanggal event diambil lewat utils_date.extract_event_date
- Hanya post dengan event_date valid yang disimpan & di-upsert
- Gemini hanya dipakai untuk title (ai_event_name)
"""

# Patch kecil: beberapa respons IG balikin broadcast_channel None → bikin Pydantic rewel.
try:
    import instagrapi.extractors as _ex

    def _safe_broadcast_channel(data: dict):
        info = (data or {}).get("pinned_channels_info") or {}
        lst = info.get("pinned_channels_list") or []
        return lst  # harus list, bukan None

    _ex.extract_broadcast_channel = _safe_broadcast_channel
except Exception:
    pass
# ===========================================================

import os
import re
import json
import time
import random
import datetime
import sys
from pathlib import Path

from dotenv import load_dotenv
from instagrapi import Client
import instaloader
from instaloader import Post

import google.generativeai as genai

from utils_time import slug_ts_from_epoch
from ig_login import login_instagram
from send_sql import upsert_posts
from utils_time import extract_event_date

# ---------- ENV & KONFIG DASAR ----------
load_dotenv()

IG_ACCOUNTS_RAW = (os.getenv("IG_ACCOUNTS") or "").strip()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "YOUR_GEMINI_API_KEY").strip()
if GEMINI_API_KEY and GEMINI_API_KEY != "YOUR_GEMINI_API_KEY":
    genai.configure(api_key=GEMINI_API_KEY)

ROOT = Path(__file__).parent
URL_DIR = ROOT / "data" / "urls"
DEFAULT_LATE_JSON = URL_DIR / "latest_urls.json"
POSTS_OUT_DIR = ROOT / "data" / "posts"
POSTS_OUT_DIR.mkdir(parents=True, exist_ok=True)
SEEN_FILE = URL_DIR / "seen_shortcodes.txt"

MAX_POSTS_PER_RUN = 10
SLEEP_BETWEEN_POSTS = (0.5, 1.5)
BACKOFF_ON_ERROR = (2.0, 4.0)
DOWNLOAD_HQ_MEDIA = "--with-media" in sys.argv[1:]

SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _slug(x: str) -> str:
    return SAFE.sub("_", (x or "").strip())


def save_json_smart(record: dict, folder: str = "data"):
    os.makedirs(folder, exist_ok=True)
    ts = slug_ts_from_epoch(record.get("date_utc"))
    user = _slug(record.get("username") or "unknown")
    code = _slug(record.get("shortcode") or "no_code")
    path = os.path.join(folder, f"{ts}{user}{code}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
    print(f"[OK] Simpan: {path}")
    return path


def dump_batch(records: list, folder: str = "data"):
    os.makedirs(folder, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(folder, f"batch{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"[GET] Rekap batch {path}")


def extract_shortcode(url: str) -> str:
    m = re.search(r"instagram\.com/(?:p|reel|tv)/([A-Za-z0-9_-]+)", url)
    if not m:
        raise ValueError(f"URL tidak valid atau tidak mengandung shortcode: {url}")
    return m.group(1)


# ---------- GEMINI TITLE ----------
def analyze_caption_with_gemini(caption: str):
    if not caption or GEMINI_API_KEY == "YOUR_GEMINI_API_KEY":
        return None
    try:
        model = genai.GenerativeModel("gemini-flash-latest")
        prompt = f"""Ambil hanya nama acara (title) dari caption Instagram berikut.
        Jangan sertakan tanggal, lokasi, sponsor, atau teks lain.

        Caption:
        "{caption}"

        Output JSON valid:
        {{
          "event_name": "Nama acara"
        }}
        """
        resp = model.generate_content(prompt)
        txt = (resp.text or "").strip()
        txt = txt.replace("```json", "").replace("```", "").strip()
        data = json.loads(txt)
        if isinstance(data, dict) and data.get("event_name"):
            return data
        return None
    except Exception as e:
        print(f"[Gemini Title Error] {e}")
        return None


# ---------- Helper ambil media ----------

def pk_from_url_local(cl: Client, url: str) -> int:
    sc = extract_shortcode(url)
    return cl.media_pk_from_code(sc)


def private_raw_item(cl: Client, pk: int) -> dict:
    data = cl.private_request(f"media/{pk}/info/")
    item = (data.get("items") or [None])[0]
    if not item:
        raise ValueError("Media info kosong")
    return item


def pick_media_url(d: dict) -> str | None:
    for v in (d.get("video_versions") or []):
        if v.get("url"):
            return v["url"]
    for c in ((d.get("image_versions2") or {}).get("candidates") or []):
        if c.get("url"):
            return c["url"]
    return d.get("thumbnail_url") or d.get("display_url") or d.get("url")


def build_record_from_raw(item: dict) -> dict | None:
    media_type = item.get("media_type")
    code = item.get("code") or item.get("shortcode")

    username = None
    if isinstance(item.get("user"), dict):
        username = item["user"].get("username")
    if not username and isinstance(item.get("owner"), dict):
        username = item["owner"].get("username")

    sidecars = []
    for it in (item.get("carousel_media") or []):
        url = pick_media_url(it) or (it.get("image_versions2") or {}).get(
            "candidates", [{}]
        )[0].get("url")
        if url:
            sidecars.append(url)

    main_url = sidecars[0] if (media_type == 8 and sidecars) else pick_media_url(item)
    cap_obj = item.get("caption")
    caption = cap_obj.get("text") if isinstance(cap_obj, dict) else cap_obj
    taken_at = item.get("taken_at") or item.get("device_timestamp") or item.get(
        "imported_taken_at"
    )

    event_date = extract_event_date(caption or "", taken_at)
    if not event_date:
        return None

    ai_info = analyze_caption_with_gemini(caption or "") or {}

    rec = {
        "shortcode": code,
        "username": username,
        "caption": caption,
        "date_utc": taken_at,
        "event_date": event_date,
        "is_video": True if media_type == 2 else bool(item.get("is_video")),
        "url": main_url,
        "cdn_url": main_url,
        "gdrive_url": None,
        "sidecars": sidecars,
        "permalink": f"https://www.instagram.com/p/{code}/" if code else None,
    }

    if isinstance(ai_info, dict) and ai_info.get("event_name"):
        rec["ai_event_name"] = ai_info["event_name"]

    return rec


def fetch_media_safely(cl: Client, pk: int) -> dict | None:
    for fn in (cl.media_info_gql, cl.media_info_v1, cl.media_info):
        try:
            obj = fn(pk)
            m = (
                obj.model_dump(exclude_none=True)
                if hasattr(obj, "model_dump")
                else obj.dict()
            )
            username = None
            if isinstance(m.get("user"), dict):
                username = m["user"].get("username")
            if not username and isinstance(m.get("owner"), dict):
                username = m["owner"].get("username")
            if not username and isinstance(m.get("user_detail"), dict):
                username = m["user_detail"].get("username")

            as_raw = {
                "media_type": m.get("media_type"),
                "code": m.get("code") or m.get("shortcode"),
                "is_video": True
                if m.get("media_type") == 2
                else bool(m.get("is_video")),
                "image_versions2": m.get("image_versions2"),
                "video_versions": m.get("video_versions"),
                "carousel_media": m.get("resources")
                or m.get("carousel_media")
                or [],
                "caption": m.get("caption"),
                "taken_at": m.get("taken_at") or m.get("timestamp"),
                "user": {"username": username} if username else None,
            }
            return build_record_from_raw(as_raw)
        except Exception:
            pass
    raw = private_raw_item(cl, pk)
    return build_record_from_raw(raw)


# ---------- INPUT URL & SEEN ----------

SHORT_RX = re.compile(r"/(?:p|reel|tv)/([^/?#]+)/?")


def is_post_url(u: str) -> bool:
    if not isinstance(u, str):
        return False
    u = u.strip()
    return ("instagram.com" in u) and bool(SHORT_RX.search(u))


def short_code_from_url(u: str) -> str | None:
    m = SHORT_RX.search(str(u))
    return m.group(1) if m else None


def load_urls_from_json(path: Path) -> list[str]:
    if not path.exists():
        print(f"[ERROR] File URL tidak ditemukan: {path}")
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            raw = [str(x).strip() for x in data if str(x).strip()]
        elif isinstance(data, dict):
            raw = []
            if isinstance(data.get("urls"), list):
                raw += [str(x).strip() for x in data["urls"] if str(x).strip()]
            for v in data.values():
                if isinstance(v, list):
                    raw += [str(x).strip() for x in v if str(x).strip()]
        else:
            print(f"[WARN] Format JSON tidak didukung: {type(data)}")
            raw = []
    except Exception as e:
        print(f"[ERROR] Gagal parse JSON {path}: {e}")
        return []
    out, seen = [], set()
    for u in raw:
        u = u.split("?")[0]
        if is_post_url(u) and u not in seen:
            out.append(u)
            seen.add(u)
    return out


def newest_urls_file() -> Path | None:
    files = sorted(
        URL_DIR.glob("urls_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return files[0] if files else None


def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        return {
            s.strip()
            for s in SEEN_FILE.read_text(encoding="utf-8").splitlines()
            if s.strip()
        }
    return set()


def save_seen(seen: set[str]):
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    SEEN_FILE.write_text("\n".join(sorted(seen)), encoding="utf-8")


def resolve_input_urls(argv: list[str]) -> list[str]:
    if "--path-url" in argv:
        try:
            i = argv.index("--path-url")
            return load_urls_from_json(Path(argv[i + 1]))
        except Exception:
            print("[WARN] Pemakaian: --path-url <path-to-json>")
    if "--urls-file" in argv:
        try:
            i = argv.index("--urls-file")
            return load_urls_from_json(Path(argv[i + 1]))
        except Exception:
            print("[WARN] Pemakaian: --urls-file <path-to-json>")
    if "--urls" in argv:
        try:
            i = argv.index("--urls")
            csv = argv[i + 1]
            out, seen = [], set()
            for u in [x.strip() for x in csv.split(",") if x.strip()]:
                u = u.split("?")[0]
                if is_post_url(u) and u not in seen:
                    out.append(u)
                    seen.add(u)
            return out
        except Exception:
            print('[WARN] Pemakaian: --urls "u1,u2,..."')
    if "--latest-urls" in argv:
        p = newest_urls_file()
        if p:
            return load_urls_from_json(p)
        print("[INFO] Tidak ada urls_*.json terbaru di folder data/urls.")
    if DEFAULT_LATE_JSON.exists():
        return load_urls_from_json(DEFAULT_LATE_JSON)
    return []


def arg_max(argv: list[str], default: int = MAX_POSTS_PER_RUN) -> int:
    if "--max" in argv:
        try:
            i = argv.index("--max")
            return max(1, int(argv[i + 1]))
        except Exception:
            print("[WARN] Pemakaian: --max <int>")
    return default

def login_instagrapi_client() -> Client:
    """
    Wrapper ke ig_login.login_instagram().
    Kalau gagal login, lempar exception supaya run_with_instagrapi()
    bisa mengembalikan None dan memicu fallback Instaloader.
    """
    cl = login_instagram()
    if not isinstance(cl, Client):
        raise RuntimeError("login_instagram tidak mengembalikan instagrapi.Client")
    return cl


def fetch_one_with_instagrapi(cl: Client, url: str) -> dict | None:
    """
    Ambil satu postingan IG via Instagrapi:
    - Konversi URL → media pk
    - Ambil detail via fetch_media_safely
    - Simpan JSON
    - Balikkan record dict (atau None kalau gagal / tidak ada event_date)
    """
    pk = pk_from_url_local(cl, url)          
    rec = fetch_media_safely(cl, pk)          #
    if not rec:
        return None

    # Simpan ke JSON (pakai folder default "data" seperti fungsi lain)
    save_json_smart(rec)

    # Jeda random antar request supaya agak "manusiawi"
    time.sleep(random.uniform(*SLEEP_BETWEEN_POSTS))
    return rec

# ---------- Jalur instagrapi ----------
def run_with_instagrapi(urls):
    try:
        cl = login_instagrapi_client()
    except Exception as e:
        print("[ERROR] Gagal inisialisasi Instagrapi:", e)
        return None  

    records = []
    hard_login_error = False

    for url in urls:
        try:
            rec = fetch_one_with_instagrapi(cl, url)
            if not rec:
                continue

            if not rec.get("event_date"):
                sc = rec.get("shortcode") or short_code_from_url(url)
                print(f"[INFO] Lewati {sc}: record kosong / tanpa event_date.")
                continue

            records.append(rec)

        except Exception as e:
            msg = str(e).lower()
            print(f"[ERROR] Gagal proses {url} (Instagrapi): {e}")

            # Kalau isinya "login_required", anggap ini error keras → paksa fallback
            if "login_required" in msg or "login required" in msg:
                hard_login_error = True
                break

    # Kalau dari semua URL, tidak ada satu pun record dan kita kena error login,
    # kembalikan None supaya run_scrape() tau: ini gagal total → pakai Instaloader.
    if hard_login_error and not records:
        print("[INFO] Instagrapi kena 'login_required' untuk semua URL, pakai fallback Instaloader.")
        return None

    return records


# ---------- Fallback Instaloader ----------

def _pick_instaloader_account():
    raw = IG_ACCOUNTS_RAW
    if raw:
        first = raw.split(",")[0].strip()
        if ":" in first:
            u, p = first.split(":", 1)
            u, p = u.strip(), p.strip()
            if u and p:
                return u, p
    user = (os.getenv("IG_USER") or "").strip()
    pwd = (os.getenv("IG_PASS") or "").strip()
    return user, pwd


def run_with_instaloader(urls):
    L = instaloader.Instaloader(download_comments=False, save_metadata=False)

    username, password = _pick_instaloader_account()
    if username and password:
        try:
            print(f"[IL] Login Instaloader sebagai {username}...")
            L.login(username, password)
            L.save_session_to_file(filename=f"ig_{username}.session")
        except Exception as e:
            print(f"[WARN] Gagal login Instaloader: {e}")
    else:
        try:
            L.load_session_from_file(
                username=os.getenv("IG_SESSION_USER", username or "session_user"),
                filename="ig.session",
            )
        except Exception:
            print("[WARN] Tidak login Instaloader. Beberapa URL bisa 403.")

    batch = []
    for url in urls:
        try:
            sc = extract_shortcode(url)
            print(f"\n [instaloader] {sc}")
            post = Post.from_shortcode(L.context, sc)

            if DOWNLOAD_HQ_MEDIA:
                main_url = post.video_url if post.is_video else post.url
                sidecars = []
                if post.typename == "GraphSidecar":
                    for node in post.get_sidecar_nodes():
                        sidecars.append(
                            node.video_url if node.is_video else node.display_url
                        )
            else:
                main_url = f"https://www.instagram.com/p/{post.shortcode}/"
                sidecars = []

            caption = post.caption
            taken_at = int(post.date_utc.timestamp())
            event_date = extract_event_date(caption or "", taken_at)
            if not event_date:
                print(f"[INFO] Lewati {sc}: event_date kosong (fallback).")
                continue

            ai_info = analyze_caption_with_gemini(caption or "") or {}

            record = {
                "shortcode": post.shortcode,
                "username": getattr(post.owner_profile, "username", None),
                "caption": caption,
                "date_utc": taken_at,
                "event_date": event_date,
                "is_video": post.is_video,
                "url": main_url,
                "cdn_url": main_url,
                "gdrive_url": None,
                "sidecars": sidecars,
                "permalink": f"https://www.instagram.com/p/{post.shortcode}/",
            }

            if isinstance(ai_info, dict) and ai_info.get("event_name"):
                record["ai_event_name"] = ai_info["event_name"]

            save_json_smart(record)
            batch.append(record)
            time.sleep(random.uniform(*SLEEP_BETWEEN_POSTS))
        except Exception as e:
            print(f"[ERROR] Gagal (instaloader) {url}: {e}")

    if batch:
        dump_batch(batch)
    return batch

# ---------- FUNGSI UTAMA SCRAPE ----------
def run_scrape():
    wait_min = 5
    wait_max = 12
    t = random.uniform(wait_min, wait_max)
    print(f"[WARM-UP] Menunggu {t:.1f} detik sebelum mulai scraping...")
    time.sleep(t)

    os.makedirs("data", exist_ok=True)

    argv = sys.argv[1:]
    max_items = arg_max(argv, default=MAX_POSTS_PER_RUN)
    resolved = resolve_input_urls(argv)

    if not resolved:
        print(
            "[INFO] Tidak ada URL untuk diproses. Pastikan find_urls.py sudah membuat latest_urls.json atau beri --path-url/--urls-file."
        )
        return []

    seen_codes = load_seen()
    filtered = []
    for u in resolved:
        sc = short_code_from_url(u)
        if not sc:
            continue
        if sc in seen_codes:
            continue
        filtered.append(u)

    if not filtered:
        print(
            "[INFO] Tidak ada URL baru untuk diproses (semua shortcode sudah pernah diproses)."
        )
        return []

    urls_final = filtered[:max_items]
    print(f"[INFO] Total URL target: {len(urls_final)} (maks {max_items})")
    batch = run_with_instagrapi(urls_final)

    if batch is None:
        print("[INFO] Instagrapi gagal total (login/network), fallback ke Instaloader.")
        batch = run_with_instaloader(urls_final) or []
    elif not batch:
        print("[INFO] tidak ada record yang lolos filter (event_date). Tidak fallback ke Instaloader.")

    # Hanya tandai SEEN untuk shortcode yang berhasil diparse & punya event_date
    for rec in batch:
        sc = rec.get("shortcode")
        if sc:
            seen_codes.add(sc)
    save_seen(seen_codes)

    return batch


def prune_old_json(folder: Path = POSTS_OUT_DIR, max_age_days: int = 90):
    cutoff = time.time() - max_age_days * 86400
    removed = 0
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)

    for p in folder.glob("*.json"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
                removed += 1
        except Exception as e:
            print(f"[WARN] Gagal hapus {p}: {e}")

    print(
        f"[CLEANUP] Bersih-bersih: {removed} file lama (> {max_age_days} hari) di {folder}"
    )


# ---------- ENTRYPOINT ----------

if __name__ == "__main__":
    print("--- Menjalankan Scrapper (Mode Mandiri/Testing) ---")
    data_hasil = run_scrape()
    if data_hasil:
        print("\n=== Data yang dikirim ke DB ===")
        for r in data_hasil:
            title = r.get("ai_event_name") or "(tanpa title AI)"
            print(f"- {r.get('shortcode')} | {r.get('event_date')} | {title}")
        print("=== END ===\n")

        upsert_posts(data_hasil)
        prune_old_json(Path("data"), max_age_days=15)
    print(
        f"\n--- Selesai Scrapping. Total {len(data_hasil or [])} data disimpan di folder 'data' & di-upsert ke DB. ---"
    )
