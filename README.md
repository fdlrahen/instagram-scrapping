# Instagram Scraping & Event Pipeline

Proyek ini adalah pipeline scraping Instagram yang digunakan untuk:
- Mengambil URL postingan dari hashtag atau akun tertentu.
- Mengunduh detail postingan (caption, media, tanggal, dll).
- Menyimpan hasilnya ke database MySQL.
- (Opsional) Meneruskan data terpilih ke Google Apps Script / Google Calendar sebagai event.

Proyek ini dirancang agar bisa dijalankan otomatis (via Task Scheduler / cron) dan digunakan untuk monitoring event/konten dari akun atau hashtag tertentu.

---

## Fitur Utama

- Scraping URL postingan Instagram dari:
  - Hashtag tertentu (mis. `#eventsemarang`, `#eventsalatiga`, dll).
  - Akun tertentu (username list).
- Pengambilan detail posting:
  - Caption
  - Tanggal posting
  - URL media (foto/video)
  - Shortcode / ID posting
- Penyimpanan ke MySQL:
  - Tabel utama (mis. `ig_posts`) untuk menyimpan hasil scraping.
- Integrasi lanjutan (opsional):
  - Mengirim data ke Google Apps Script (GAS) untuk diolah lagi (misalnya ke Google Calendar).
- Mekanisme pencegahan duplikasi berdasarkan `shortcode` / URL.

---

## Arsitektur Singkat

Struktur logika (bisa disesuaikan dengan isi repo):

- `find_urls.py`  
  Mengambil daftar URL Instagram dari hashtag / akun (via Selenium / undetected-chromedriver, atau API library yang digunakan).

- `fetch_post.py`  
  Mengambil detail posting dari list URL (caption, tanggal, media URL, dll).

- `send_sql.py`  
  Menyimpan hasil scraping ke database MySQL (insert / update ke tabel, misalnya `ig_posts`).

- `post_to_gast.py` (opsional)  
  Mengirim data promosi / event yang sudah dipilih ke endpoint Google Apps Script.

- `main.py`  
  Orkestrator pipeline. Mengatur urutan:
  1. Ambil URL
  2. Ambil detail posting
  3. Simpan ke MySQL
  4. (Opsional) Push ke GAS

---

## Prasyarat

- Python 3.13+ (disarankan)
- MySQL server
- Virtual environment (opsional, tapi disarankan)
- Library Python (lihat bagian Installation)

Jika memakai Selenium / undetected-chromedriver:
- Google Chrome / Chromium
- ChromeDriver yang sesuai versi browser

---

## Instalasi

1. **Clone repository**

## Etika & Batasan

Gunakan proyek ini hanya untuk keperluan yang mematuhi Terms of Service Instagram.

Jangan menyalahgunakan scraping untuk spam, pelanggaran privasi, atau aktivitas ilegal.

Pertimbangkan rate limit dan frekuensi scraping agar tidak membebani layanan.



