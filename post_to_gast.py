# post_to_gast.py
import os, json, sys, time
import requests
from dotenv import load_dotenv

from send_sql import fetch_unsent_posts, mark_sent

load_dotenv()
GAS_URL     = os.getenv("GAS_WEBAPP_URL", "").strip()
GAS_SECRET  = os.getenv("GAS_SHARED_SECRET", "").strip()
BATCH_LIMIT = int(os.getenv("GAS_BATCH_LIMIT", "100"))
CHUNK_SIZE  = int(os.getenv("GAS_CHUNK_SIZE", "50"))  


def _post_chunk(chunk):
    """
    Kirim satu chunk ke GAS, lalu tandai HANYA yang sukses.
    Sekarang: deteksi juga error duplikasi dari sisi GAS.
    Sekaligus log baris mana yang dikirim.
    """
    if not chunk:
        return 0, 0

    # DEBUG: tampilkan baris yang akan dikirim dalam chunk ini
    print(f"[INFO] Chunk berisi {len(chunk)} record. Detail yang dikirim:")
    for row in chunk:
        print(
            "  [POST] id=",
            row.get("id"),
            "| shortcode=",
            row.get("shortcode"),
            "| event_date=",
            row.get("event_date"),
            "| title=",
            (row.get("ai_event_name") or "(tanpa title AI)")
        )

    headers = {"Content-Type": "application/json; charset=utf-8"}
    payload = {"secret": GAS_SECRET, "records": chunk}

    resp = requests.post(GAS_URL, json=payload, headers=headers, timeout=30)
    text = (resp.text or "")[:500]
    print(f"HTTP {resp.status_code} | Preview: {text.replace(os.linesep, ' ')[:200]}")
    resp.raise_for_status()

    # Parsing hasil
    try:
        data = resp.json()
    except Exception:
        print("[ERROR] Respons bukan JSON valid. Tidak menandai apa pun.")
        return 0, len(chunk)

    results = data.get("results") or []
    if not isinstance(results, list) or not results:
        print("[WARN] Respons tidak berisi 'results' yang valid. Tidak menandai apa pun.")
        return 0, len(chunk)

    success_rows = []
    failed_items = []

    for idx, res in enumerate(results):
        row = chunk[idx] if idx < len(chunk) else {}
        row_id = row.get("id")
        sc     = row.get("shortcode")

        if res and res.get("ok") and res.get("created"):
            created = res["created"] or {}
            ev_id   = created.get("id")
            mode    = created.get("mode")
            action  = created.get("action", "create")

            if row_id is not None and ev_id:
                success_rows.append({
                    "id": row_id,
                    "gcal_event_id": ev_id,
                    "mode": mode,
                    "action": action,
                })
        else:
            err_msg = (res or {}).get("error") or "Unknown error"
            # Deteksi pesan error yang sifatnya duplikat di sisi GAS
            lower = err_msg.lower()
            if "duplicate" in lower or "already exists" in lower:
                print(f"[WARN] Duplikat di GAS untuk id={row_id} sc={sc}: {err_msg}")
            else:
                print(f"[ERROR] Gagal id={row_id} sc={sc} err={err_msg}")
            failed_items.append({
                "id": row_id,
                "shortcode": sc,
                "error": err_msg
            })

    # Tandai hanya yang sukses
    if success_rows:
        mark_sent(success_rows, status_text="OK")

    # (Log detail gagal sudah di-print di atas)

    return len(success_rows), len(failed_items)


def send_batch_to_gas(limit=BATCH_LIMIT):
    if not GAS_URL:
        print("[ERROR] GAS_WEBAPP_URL kosong.")
        return

    rows = fetch_unsent_posts(limit=limit)
    if not rows:
        print("[INFO] Tidak ada data baru untuk dikirim.")
        return

    unique_rows = []
    seen_shortcodes = set()
    dup_count = 0

    for row in rows:
        sc = (row.get("shortcode") or "").strip()
        if sc:
            if sc in seen_shortcodes:
                dup_count += 1
                print(f"[WARN] Duplikat dalam batch: shortcode={sc} id={row.get('id')} dilewati, sudah ada entri lain di batch ini.")
                continue
            seen_shortcodes.add(sc)
        unique_rows.append(row)

    if dup_count:
        print(f"[INFO] Ditemukan {dup_count} baris duplikat dalam batch (berdasarkan shortcode). Hanya baris pertama per shortcode yang dikirim.")

    # Kalau setelah dibersihkan duplikat ternyata kosong, ya sudah.
    if not unique_rows:
        print("[INFO] Semua kandidat di batch ini terdeteksi duplikat. Tidak ada yang dikirim ke GAS.")
        return

    rows = unique_rows
    # ===========================================================

    total_ok = total_fail = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i+CHUNK_SIZE]
        try:
            ok, fail = _post_chunk(chunk)
            total_ok   += ok
            total_fail += fail
        except requests.RequestException as e:
            print(f"[ERROR] Chunk idx {i} gagal total: {e}")
            total_fail += len(chunk)

    print(f"[OK] Selesai. Sukses: {total_ok} | Gagal: {total_fail}")


if __name__ == "__main__":
    send_batch_to_gas(limit=BATCH_LIMIT)
