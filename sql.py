#kalau gasalah ini ga kepake
# upload_to_db.py
import os, json, glob, argparse
from get_post import upsert_posts  # re-use fungsi dari filemu

def load_batch(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Format batch bukan list: {path}")
    return data

def pick_latest_batch(data_dir="data"):
    files = sorted(glob.glob(os.path.join(data_dir, "batch*.json")))
    if not files:
        raise FileNotFoundError(f"Tidak ditemukan file batch di {data_dir}/")
    return files[-1]

def main():
    ap = argparse.ArgumentParser(description="Upload hasil scrape IG ke DB dari file batch JSON.")
    ap.add_argument("--batch", help="Path ke batch JSON (mis. data/batch20251031_062852.json). Kalau tidak diisi, ambil yang terbaru.")
    args = ap.parse_args()

    batch_path = args.batch or pick_latest_batch()
    print(f"📂 Memuat batch: {batch_path}")
    records = load_batch(batch_path)
    if not records:
        print("ℹ Batch kosong. Tidak ada yang diinsert.")
        return

    res = upsert_posts(records)
    print(f"🎯 Selesai. Inserted={res['inserted']}, Updated={res['updated']}")

if __name__ == "__main__":
    main()
