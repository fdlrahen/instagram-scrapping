# send_sql.py
import os, json, datetime, re
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

from utils_time import to_mysql_datetime

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1").strip()
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root").strip()
DB_PASS = os.getenv("DB_PASSWORD", os.getenv("DB_PASS", "")).strip()
DB_NAME = os.getenv("DB_NAME", os.getenv("db_name", "instagram_db")).strip()

# Berapa bulan ke depan yang masih diterima untuk event
MAX_EVENT_FUTURE_MONTHS = int(os.getenv("MAX_EVENT_FUTURE_MONTHS", "1") or 1)


def get_conn():
    try:
        return mysql.connector.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASS,
            database=DB_NAME,
            charset="utf8mb4", use_unicode=True
        )
    except Error as e:
        raise RuntimeError(f"Gagal konek MySQL: {e}")


def _normalize_row(row: dict) -> dict:
    """
    Bikin aman untuk json.dumps:
    - datetime -> 'YYYY-mm-dd HH:MM:SS'
    - sidecars JSON string -> list
    """
    out = dict(row)
    for k, v in list(out.items()):
        if isinstance(v, (datetime.datetime, datetime.date)):
            out[k] = v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime.datetime) else v.isoformat()
    # sidecars bisa JSON atau TEXT; kirim sebagai list biar rapi
    sc = out.get("sidecars")
    if isinstance(sc, str):
        try:
            out["sidecars"] = json.loads(sc)
        except Exception:
            # kalau bukan JSON valid, tetap kirim string apa adanya
            pass
    return out


def fetch_unsent_posts(limit=100):
    """
    Ambil baris yang PERLU dikirim ke GAS:
    - Event baru        : gcal_event_id IS NULL
    - Event lama, di-edit: updated_at > gas_sent_at
    """
    sql = """
      SELECT
        id,
        shortcode,
        username,
        caption,
        event_date,
        UNIX_TIMESTAMP(date_utc) AS date_utc,  -- epoch detik untuk GAS
        is_video,
        url AS gallery_url,
        sidecars,
        permalink,
        gcal_event_id,
        gas_sent_at,
        updated_at
      FROM ig_posts
      WHERE
        gcal_event_id IS NULL
        OR updated_at > IFNULL(gas_sent_at, '1970-01-01 00:00:00')
      ORDER BY updated_at ASC
      LIMIT %s
    """
    conn = cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, (limit,))
        rows = cur.fetchall() or []
        return [_normalize_row(r) for r in rows]
    finally:
        try:
            if cur: cur.close()
            if conn: conn.close()
        except Exception:
            pass


def mark_sent(success_rows, status_text: str = ""):
    """
    Tandai baris 'sudah terkirim' dengan cap waktu & status respons singkat.
    """
    if not success_rows:
        return

    conn = cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        now_str = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for r in success_rows:
            row_id = r["id"]
            ev_id  = r["gcal_event_id"]
            action = (r.get("action", "") or "").strip()
            status = f"{status_text} {action}".strip()
            cur.execute(
                """
                UPDATE ig_posts
                SET
                    gcal_event_id = %s,
                    gas_sent_at   = %s,
                    gas_status    = %s
                WHERE id = %s
                """,
                (ev_id, now_str, status, row_id)
            )
        conn.commit()
        print(f"Terkirim: {len(success_rows)} baris.")
    except Exception as e:
        if conn: conn.rollback()
        print(f"Gagal menandai terkirim: {e}")
    finally:
        try:
            if cur: cur.close()
            if conn: conn.close()
        except Exception:
            pass


# =============================
#  Tambahan: helper tanggal & UPSERT dari fetch_post
# =============================

# Map nama bulan Indonesia → angka
MONTH_MAP = {
    "januari": 1, "jan": 1,
    "februari": 2, "feb": 2,
    "maret": 3, "mar": 3,
    "april": 4, "apr": 4,
    "mei": 5,
    "juni": 6, "jun": 6,
    "juli": 7, "jul": 7,
    "agustus": 8, "ags": 8,
    "september": 9, "sep": 9,
    "oktober": 10, "okt": 10,
    "november": 11, "nov": 11,
    "desember": 12, "des": 12,
}

# Pola teks tanggal, misal:
# "5 Oktober - 7 Oktober 2025" atau "19 Januari 2025"
EVENT_RANGE_RX = re.compile(
    r"(\d{1,2})\s+([A-Za-z]+)\s*[–-]\s*\d{1,2}\s+([A-Za-z]+)\s+(\d{4})",
    re.IGNORECASE
)
EVENT_SINGLE_RX = re.compile(
    r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})",
    re.IGNORECASE
)


def _add_months(year: int, month: int, delta: int):
    total = year * 12 + (month - 1) + delta
    ny = total // 12
    nm = total % 12 + 1
    return ny, nm


def _ym_ge(a, b):
    return (a[0] > b[0]) or (a[0] == b[0] and a[1] >= b[1])


def _ym_le(a, b):
    return (a[0] < b[0]) or (a[0] == b[0] and a[1] <= b[1])


def parse_event_start_ym(event_date: str):
    """
    Ambil (tahun, bulan_awal) dari event_date.

    Support:
      - "DD/MM/YYYY"
      - "DD/MM/YYYY - DD/MM/YYYY"
      - "19 Januari 2025"
      - "5 Oktober - 7 Oktober 2025"
    """
    if not event_date:
        return None
    s = event_date.strip()

    # Numeric range: "DD/MM/YYYY - DD/MM/YYYY"
    m = re.search(
        r"(\d{1,2})/(\d{1,2})/(\d{4})\s*[–-]\s*(\d{1,2})/(\d{1,2})/(\d{4})",
        s
    )
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        return (int(y1), int(m1))

    # Numeric single: "DD/MM/YYYY"
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        d, m1, y1 = m.groups()
        return (int(y1), int(m1))

    # Textual range: "5 Oktober - 7 Oktober 2025"
    m = EVENT_RANGE_RX.search(s)
    if m:
        d1, mname1, d2, mname2, y = m.groups()
        mname1 = mname1.lower()
        mname2 = mname2.lower()
        mm = MONTH_MAP.get(mname1) or MONTH_MAP.get(mname2)
        if not mm:
            return None
        return (int(y), mm)

    # Textual single: "19 Januari 2025"
    m = EVENT_SINGLE_RX.search(s)
    if m:
        d, mname, y = m.groups()
        mm = MONTH_MAP.get(mname.lower())
        if not mm:
            return None
        return (int(y), mm)

    return None


def upsert_posts(records: list):
    """
    Simpan list record hasil scrape ke tabel MySQL dengan UPSERT berdasarkan shortcode.
    Hanya memasukkan event yang:
      - punya event_date valid,
      - bulan >= bulan ini,
      - bulan <= bulan depan (MAX_EVENT_FUTURE_MONTHS).
    """
    if not records:
        print(" Tidak ada record untuk disimpan ke MySQL.")
        return {"inserted": 0, "updated": 0, "skipped": 0}

    today = datetime.date.today()
    cur_y, cur_m = today.year, today.month
    max_y, max_m = _add_months(cur_y, cur_m, MAX_EVENT_FUTURE_MONTHS)

    rows = []
    skipped_no_shortcode = 0
    skipped_no_eventdate = 0
    skipped_out_of_range = 0

    for r in records:
        shortcode  = (r.get("shortcode") or "").strip()
        if not shortcode:
            skipped_no_shortcode += 1
            continue

        raw_event = r.get("event_date")
        event_date_str = (raw_event or "").strip()
        if not event_date_str or event_date_str.lower() == "null":
            print(f" Lewati {shortcode}: event_date kosong / NULL, tidak di-upsert ke DB.")
            skipped_no_eventdate += 1
            continue

        ym = parse_event_start_ym(event_date_str)
        if not ym:
            print(f" Lewati {shortcode}: event_date tidak bisa diparse: {event_date_str!r}")
            skipped_no_eventdate += 1
            continue

        if not _ym_ge(ym, (cur_y, cur_m)) or not _ym_le(ym, (max_y, max_m)):
            print(f" Lewati {shortcode}: event_date {event_date_str!r} di luar rentang bulan yang diizinkan.")
            skipped_out_of_range += 1
            continue

        username   = r.get("username")
        caption    = r.get("caption")
        date_mysql = to_mysql_datetime(r.get("date_utc"))
        event_date = event_date_str  # simpan string aslinya
        is_video   = 1 if r.get("is_video") else 0
        url        = r.get("url")
        sidecars   = json.dumps(r.get("sidecars") or [], ensure_ascii=False)
        permalink  = r.get("permalink") or f"https://www.instagram.com/p/{shortcode}/"
        cdn_url    = r.get("cdn_url") or r.get("url")
        gdrive_url = r.get("gdrive_url")
        ai_event_name = r.get("ai_event_name")

        rows.append((
            shortcode, username, caption, date_mysql, event_date,
            is_video, url, sidecars, permalink,
            cdn_url, gdrive_url, ai_event_name
        ))

    if not rows:
        print(
            f" Semua record tidak valid/kehilangan shortcode/di luar rentang tanggal."
            f" Skip shortcode kosong: {skipped_no_shortcode}, "
            f" Skip event date kosong/invalid: {skipped_no_eventdate}, "
            f" Skip event_date di luar rentang: {skipped_out_of_range}."
        )
        return {"inserted": 0, "updated": 0, "skipped": len(records)}

    sql = """
INSERT INTO ig_posts
    (shortcode, username, caption, date_utc, event_date,
     is_video, url, sidecars, permalink,
     cdn_url, gdrive_url, ai_event_name,
     created_at, updated_at)
VALUES
    (%s, %s, %s, %s, %s,
     %s, %s, %s, %s,
     %s, %s, %s,
     NOW(), NOW())
ON DUPLICATE KEY UPDATE
    username      = VALUES(username),
    caption       = VALUES(caption),
    date_utc      = VALUES(date_utc),
    event_date    = VALUES(event_date),
    is_video      = VALUES(is_video),
    url           = VALUES(url),
    sidecars      = VALUES(sidecars),
    permalink     = VALUES(permalink),
    cdn_url       = VALUES(cdn_url),
    gdrive_url    = VALUES(gdrive_url),
    ai_event_name = VALUES(ai_event_name),
    updated_at    = NOW();
"""

    conn = cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.executemany(sql, rows)
        affected = cur.rowcount
        conn.commit()

        total_rows = len(rows)
        updated = max(0, affected - total_rows)  # aproksimasi
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
