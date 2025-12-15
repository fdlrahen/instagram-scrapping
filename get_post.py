# ====================== fetch_posts.py ======================
# Patch kecil: beberapa respons IG balikin broadcast_channel None → bikin Pydantic rewel.
try:
    import instagrapi.extractors as _ex
    def _safe_broadcast_channel(data: dict):
        info = (data or {}).get("pinned_channels_info") or {}
        lst  = info.get("pinned_channels_list") or []
        return lst  # harus list, bukan None
    _ex.extract_broadcast_channel = _safe_broadcast_channel
except Exception:
    pass
# ===========================================================

import os, re, json, time, random, datetime, sys
# !!! HAPUS: requests (GAS dipisah file, jadi tidak dibutuhkan)
from dotenv import load_dotenv
from instagrapi import Client
import instaloader
from instaloader import Post
import mysql.connector
from mysql.connector import Error
from pathlib import Path
from set_config import DEVICE, UA
from utils_time import to_mysql_datetime, slug_ts_from_epoch

load_dotenv()
SESSIONID       = os.getenv("IG_SESSIONID", "").strip()
DS_USER_ID      = os.getenv("IG_DS_USER_ID", "").strip()
CSRFTOKEN       = os.getenv("IG_CSRFTOKEN", "").strip()
PROXY           = os.getenv("IG_PROXY", "").strip()
IG_USER         = os.getenv("IG_USER", "").strip()
IG_PASS         = os.getenv("IG_PASS", "").strip()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1").strip()
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root").strip()
DB_PASS = os.getenv("DB_PASSWORD", os.getenv("DB_PASS", "")).strip()
DB_NAME = os.getenv("DB_NAME", "instagram_db").strip()

# ========= PATH DEFAULT =========
ROOT = Path(__file__).parent
URL_DIR = ROOT / "data" / "urls"
DEFAULT_LATE_JSON = URL_DIR / "latest_urls.json"
POSTS_OUT_DIR = ROOT / "data" / "posts"
POSTS_OUT_DIR.mkdir(parents=True, exist_ok=True)
SEEN_FILE = URL_DIR / "seen_shortcodes.txt"

# ========= TUNING =========
MAX_POSTS_PER_RUN = 10
SLEEP_BETWEEN_POSTS = (0.5, 1.5)
BACKOFF_ON_ERROR = (3.0, 6.0)
KEEP_POST_JSON = False

# ---------- UTIL: penamaan file pakai waktu kegiatan (waktu posting) ----------
SAFE = re.compile(r"[^A-Za-z0-9._-]+")

def _slug(x: str) -> str:
    return SAFE.sub("_", (x or "").strip())

def save_json_smart(record: dict, folder: str = "data"):
    """
    Simpan ke data/<YYYYmmdd_HHMMSS><username><shortcode>.json
    """
    os.makedirs(folder, exist_ok=True)
    ts   = slug_ts_from_epoch(record.get("date_utc"))
    user = _slug(record.get("username") or "unknown")
    code = _slug(record.get("shortcode") or "no_code")
    path = os.path.join(folder, f"{ts}{user}{code}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
    print(f"[OK] Simpan: {path}")
    if not KEEP_POST_JSON:
        try:
            os.remove(path)
            print(f"[CLEANUP] Hapus file per path: {path}")
        except OSError as e:
            print(f"[WARN] Gagal hapus {path}:{e}") 
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

# ---------- UTIL: Ekstraksi tanggal dari caption (Event Date) ----------
def extract_event_date(caption: str) -> str | None:
    """
    Ekstrak tanggal event (misal: '29 September – 12 Oktober 2025') dari caption.
    """
    if not caption:
        return None
    
    # Regex 1: Cari pola tanggal range (e.g., dd Bulan - dd Bulan YYYY)
    date_regex = re.compile(
        r"\d{1,2}\s+(?:Januari|Februari|Maret|April|Mei|Juni|Juli|Agustus|September|Oktober|November|Desember|Jan|Feb|Mar|Apr|Jun|Jul|Ags|Sep|Okt|Nov|Des)\s*[–-]\s*\d{1,2}\s+(?:Januari|Februari|Maret|April|Mei|Juni|Juli|Agustus|September|Oktober|November|Desember|Jan|Feb|Mar|Apr|Jun|Jul|Ags|Sep|Okt|Nov|Des)\s+\d{4}",
        re.IGNORECASE | re.MULTILINE
    )
    match = date_regex.search(caption)
    if match:
        return match.group(0).strip() 
    
    # Regex 2: Cari pola tanggal tunggal (e.g., dd Bulan YYYY)
    date_regex_single = re.compile(
        r"\d{1,2}\s+(?:Januari|Februari|Maret|April|Mei|Juni|Juli|Agustus|September|Oktober|November|Desember|Jan|Feb|Mar|Apr|Jun|Jul|Ags|Sep|Okt|Nov|Des)\s+\d{4}",
        re.IGNORECASE | re.MULTILINE
    )
    match_single = date_regex_single.search(caption)
    if match_single:
        return match_single.group(0).strip()

    return None

# ---------- Fingerprint device/UA biar konsisten ----------
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

def build_record_from_raw(item: dict) -> dict:
    media_type = item.get("media_type")
    code = item.get("code") or item.get("shortcode")

    username = None
    if isinstance(item.get("user"), dict):
        username = item["user"].get("username")
    if not username and isinstance(item.get("owner"), dict):
        username = item["owner"].get("username")

    sidecars = []
    for it in (item.get("carousel_media") or []):
        url = pick_media_url(it) or (it.get("image_versions2") or {}).get("candidates", [{}])[0].get("url")
        if url:
            sidecars.append(url)

    main_url = sidecars[0] if (media_type == 8 and sidecars) else pick_media_url(item)
    cap_obj = item.get("caption")
    caption = cap_obj.get("text") if isinstance(cap_obj, dict) else cap_obj
    taken_at = item.get("taken_at") or item.get("device_timestamp") or item.get("imported_taken_at")
    
    # Ekstraksi tanggal event dari caption
    event_date = extract_event_date(caption)

    # return dict lebih rapi di bawah
    return {
        "shortcode": code,
        "username": username,
        "caption": caption,
        "date_utc": taken_at,               # mentah: epoch/ISO; konversi saat upsert
        "event_date": event_date,          # string apa adanya
        "is_video": True if media_type == 2 else bool(item.get("is_video")),
        "url": main_url,
        "sidecars": sidecars,
        "permalink": f"https://www.instagram.com/p/{code}/" if code else None
    }

def fetch_media_safely(cl: Client, pk: int) -> dict:
    for fn in (cl.media_info_gql, cl.media_info_v1, cl.media_info):
        try:
            obj = fn(pk)
            m = obj.model_dump(exclude_none=True) if hasattr(obj, "model_dump") else obj.dict()
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
                "is_video": True if m.get("media_type") == 2 else bool(m.get("is_video")),
                "image_versions2": m.get("image_versions2"),
                "video_versions": m.get("video_versions"),
                "carousel_media": m.get("resources") or m.get("carousel_media") or [],
                "caption": m.get("caption"),
                "taken_at": m.get("taken_at") or m.get("timestamp"),
                "user": {"username": username} if username else None
            }
            return build_record_from_raw(as_raw)
        except Exception:
            pass
    raw = private_raw_item(cl, pk)
    return build_record_from_raw(raw)

# ---------- INPUT URL: TAMBAHAN (tidak mengubah penamaan yang ada) ----------
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
            print(f"[WARN] Format JSON tidak didukung: {type(data)}"); raw = []
    except Exception as e:
        print(f"[ERROR] Gagal parse JSON {path}: {e}")
        return []
    # filter & dedupe
    out, seen = [], set()
    for u in raw:
        u = u.split("?")[0]
        if is_post_url(u) and u not in seen:
            out.append(u); seen.add(u)
    return out

def newest_urls_file() -> Path | None:
    files = sorted(URL_DIR.glob("urls_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        return {s.strip() for s in SEEN_FILE.read_text(encoding="utf-8").splitlines() if s.strip()}
    return set()

def save_seen(seen: set[str]):
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    SEEN_FILE.write_text("\n".join(sorted(seen)), encoding="utf-8")

def resolve_input_urls(argv: list[str]) -> list[str]:
    """
    Tambahan tanpa mengubah struktur lama:
    Prioritas: --path-url | --urls-file | --urls | --latest-urls | default latest_urls.json | fallback URLS
    """
    if "--path-url" in argv:
        try:
            i = argv.index("--path-url")
            return load_urls_from_json(Path(argv[i+1]))
        except Exception:
            print("[WARN] Pemakaian: --path-url <path-to-json>")
    if "--urls-file" in argv:
        try:
            i = argv.index("--urls-file")
            return load_urls_from_json(Path(argv[i+1]))
        except Exception:
            print("[WARN] Pemakaian: --urls-file <path-to-json>")
    if "--urls" in argv:
        try:
            i = argv.index("--urls")
            csv = argv[i+1]
            out, seen = [], set()
            for u in [x.strip() for x in csv.split(",") if x.strip()]:
                u = u.split("?")[0]
                if is_post_url(u) and u not in seen:
                    out.append(u); seen.add(u)
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
    # fallback terakhir: konstanta URLS lama
def arg_max(argv: list[str], default:int=MAX_POSTS_PER_RUN) -> int:
    if "--max" in argv:
        try:
            i = argv.index("--max"); return max(1, int(argv[i+1]))
        except Exception:
            print("[WARN] Pemakaian: --max <int>")
    return default

# ---------- Jalur instagrapi (login by session) ----------
def run_with_instagrapi(urls):
    settings = {
        "authorization_data": {"ds_user_id": DS_USER_ID, "sessionid": SESSIONID} if DS_USER_ID else {},
        "cookies": {"csrftoken": CSRFTOKEN} if CSRFTOKEN else {},
        "device_settings": DEVICE,
        "user_agent": UA
    }
    cl = Client(settings=settings)
    if PROXY:
        cl.set_proxy(PROXY)

    cl.delay_range = [0.75, 2]
    cl.request_timeout = 30
    cl.request_timeout_retry = 3

    try:
        cl.login_by_sessionid(SESSIONID)
    except Exception as e:
        print("[WARN] login_by_sessionid gagal:", e, "→ backoff 45 dtk, coba lagi.")
        time.sleep(random.uniform(*SLEEP_BETWEEN_POSTS))
        cl.login_by_sessionid(SESSIONID)

    cl.dump_settings("session.json")

    batch = []
    for url in urls:
        try:
            sc = extract_shortcode(url)
            print(f"\n [instagrapi] {sc}")
            pk = pk_from_url_local(cl, url)
            record = fetch_media_safely(cl, pk)
            if not record.get("event_date"):
                print(f"[INFO] Lewati {sc}: tidak ada event_date di caption, tidak disimpan.")
                continue
            save_json_smart(record)
            batch.append(record)
            time.sleep(random.uniform(1.5, 3.0))
        except Exception as e:
            print(f"[ERROR] Gagal (instagrapi) {url}: {e}")
            
    if batch:
        dump_batch(batch)
    return batch

# ---------- Fallback Instaloader (publik) ----------
def run_with_instaloader(urls):
    L = instaloader.Instaloader(download_comments=False, save_metadata=False)
    if IG_USER and IG_PASS:
        L.login(IG_USER, IG_PASS)
        L.save_session_to_file(filename="ig.session")
    else:
        try:
            L.load_session_from_file(username=os.getenv("IG_SESSION_USER", IG_USER or "session_user"),
                                     filename="ig.session")
        except Exception:
            print("[WARN] Tidak login Instaloader. Beberapa URL bisa 403.")

    batch = []
    for url in urls:
        try:
            sc = extract_shortcode(url)
            print(f"\n [instaloader] {sc}")
            post = Post.from_shortcode(L.context, sc)
            main_url = post.video_url if post.is_video else post.url
            sidecars = []
            if post.typename == "GraphSidecar":
                for node in post.get_sidecar_nodes():
                    sidecars.append(node.video_url if node.is_video else node.display_url)

            record = {
                "shortcode": post.shortcode,
                "username": getattr(post.owner_profile, "username", None),
                "caption": post.caption,
                "date_utc": int(post.date_utc.timestamp()),          # mentah, konversi saat upsert
                "event_date": extract_event_date(post.caption),
                "is_video": post.is_video,
                "url": main_url,
                "sidecars": sidecars
            }
            if not record.get("event_date"):
                print(f"[INFO] Lewati {sc}: tidak ada event_date di caption, tidak disimpan.")
                continue
            save_json_smart(record)
            batch.append(record)
            time.sleep(random.uniform(*SLEEP_BETWEEN_POSTS))
        except Exception as e:
            print(f"[ERROR] Gagal (instaloader) {url}: {e}")
            
    if batch:
        dump_batch(batch)
    return batch

def run_scrape():
    """
    Fungsi utama untuk menjalankan scraper.
    """
    os.makedirs("data", exist_ok=True)

    # Tambahan: resolve sumber URL (tanpa menghapus fallback lama)
    argv = sys.argv[1:]
    max_items = arg_max(argv, default=MAX_POSTS_PER_RUN)
    resolved = resolve_input_urls(argv)

    # Cache shortcode untuk menghindari duplikasi proses
    seen = load_seen()
    filtered = []
    for u in resolved:
        sc = short_code_from_url(u)
        if not sc:
            continue
        if sc in seen:
            continue
        filtered.append(u)

    # Hormati limit
    urls_final = (filtered if filtered else resolved)[:max_items]

    if not urls_final:
        print("[INFO] Tidak ada URL untuk diproses. Pastikan find_urls.py sudah membuat latest_urls.json atau beri --path-url/--urls-file.")
        return []

    print(f"[INFO] Total URL target: {len(urls_final)} (maks {max_items})")

    if SESSIONID:
        batch = run_with_instagrapi(urls_final)
    else:
        print("[INFO] IG_SESSIONID tidak ada. Fallback ke Instaloader.")
        batch = run_with_instaloader(urls_final)

    # Update cache seen setelah sukses
    if batch:
        for r in batch:
            sc = (r.get("shortcode") or "").strip()
            if sc:
                seen.add(sc)
        save_seen(seen)

    return batch

# ---------- MySQL ----------
def connect_mysql():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            charset="utf8mb4",
            use_unicode=True
        )
        return conn
    except Error as e:
        raise RuntimeError(f"Gagal konek MySQL: {e}")

def upsert_posts(records: list):
    """
    Simpan list record hasil scrape ke tabel MySQL dengan UPSERT berdasarkan shortcode.
    Tabel yang diharapkan: ig_posts (lihat DDL di bawah).
    """
    if not records:
        print(" Tidak ada record untuk disimpan ke MySQL.")
        return {"inserted": 0, "updated": 0, "skipped": 0}

    rows = []
    skipped_no_shortcode = 0
    skipped_no_eventdate = 0
    for r in records:
        shortcode  = (r.get("shortcode") or "").strip()
        if not shortcode:
            skipped_no_shortcode +=1
            continue
        
        raw_event = r.get("event_date")
        event_date = (raw_event or "").strip()
        # anggap "null" (string) juga sampah
        if not event_date or event_date.lower() == "null":
            print(f" Lewati {shortcode}: event_date kosong / NULL, tidak di-upsert ke DB.")
            skipped_no_eventdate += 1
            continue
        
        username   = r.get("username")
        caption    = r.get("caption")
        date_mysql = to_mysql_datetime(r.get("date_utc"))
        event_date = r.get("event_date")  # string hasil regex
        is_video   = 1 if r.get("is_video") else 0
        url        = r.get("url")
        sidecars   = json.dumps(r.get("sidecars") or [], ensure_ascii=False)
        permalink  = r.get("permalink") or f"https://www.instagram.com/p/{shortcode}/"

        rows.append((
            shortcode, username, caption, date_mysql, event_date,
            is_video, url, sidecars, permalink
        ))

    if not rows:
        print(f" Semua record tidak valid/kehilangan shortcode."
              f"Skip shortcode kosong: {skipped_no_shortcode}, "
              f"Skip event date kosong: {skipped_no_eventdate}. "
              )
        return {"inserted": 0, "updated": 0, "skipped": len(records)}

    sql = """
    INSERT INTO ig_posts
        (shortcode, username, caption, date_utc, event_date, is_video, url, sidecars, permalink, created_at, updated_at)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
    ON DUPLICATE KEY UPDATE
        username  = VALUES(username),
        caption   = VALUES(caption),
        date_utc  = VALUES(date_utc),
        event_date= VALUES(event_date),
        is_video  = VALUES(is_video),
        url       = VALUES(url),
        sidecars  = VALUES(sidecars),
        permalink = VALUES(permalink),
        updated_at= NOW();
    """

    conn = None
    cur = None
    inserted = updated = 0
    try:
        conn = connect_mysql()
        cur = conn.cursor()
        cur.executemany(sql, rows)
        affected = cur.rowcount
        conn.commit()

        total_rows = len(rows)
        updated = max(0, affected - total_rows)  # UPDATE biasanya dihitung 2
        inserted = total_rows - updated

        print(f"[OK] MySQL upsert selesai. Inserted: {inserted}, Updated: {updated}, Total input: {total_rows}")
        return {"inserted": inserted, "updated": updated, "skipped": len(records) - total_rows}
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[WARN] Gagal simpan ke MySQL: {e}")
        return {"inserted": 0, "updated": 0, "skipped": len(records)}
    finally:
        try:
            if cur: cur.close()
            if conn: conn.close()
        except Exception:
            pass
def prune_old_json(folder: Path = POSTS_OUT_DIR, max_age_days: int = 90):
    """
    Hapus file JSON lama di folder posts yang lebih tua dari max_age_days.
    """
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

    print(f"[CLEANUP] Bersih-bersih: {removed} file lama (> {max_age_days} hari) di {folder}")

if __name__ == "__main__":
    # Mode mandiri: scrape + upsert DB (tanpa kirim GAS)
    print("--- Menjalankan Scrapper (Mode Mandiri/Testing) ---")
    data_hasil = run_scrape()
    if data_hasil:
        upsert_posts(data_hasil)
        prune_old_json(POSTS_OUT_DIR, max_age_days = 90)
    print(f"\n--- Selesai Scrapping. Total {len(data_hasil or [])} data disimpan di folder 'data' & di-upsert ke DB. ---")
