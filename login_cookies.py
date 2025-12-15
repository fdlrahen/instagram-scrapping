# login_cookies_to_env.py
import os, sys, time, random
from pathlib import Path
from dotenv import dotenv_values
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options

ENV_PATH = Path(".env")

def write_env(update: dict):
    existing = {}
    if ENV_PATH.exists():
        # muat .env lama
        for k, v in dotenv_values(ENV_PATH).items():
            if v is not None:
                existing[k] = v
    existing.update(update)
    # tulis ulang
    lines = []
    for k, v in existing.items():
        lines.append(f'{k}={v}')
    ENV_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f".env diperbarui → {ENV_PATH.resolve()}")

def main():
    opts = Options()
    # profil sementara bersih
    opts.add_argument("--user-data-dir=" + str(Path(".cache/chrome_tmp").resolve()))
    opts.add_argument("--profile-directory=Default")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-sync")
    opts.add_argument("--lang=id-ID")

    driver = uc.Chrome(options=opts)
    driver.set_page_load_timeout(90)

    print("▶ Membuka instagram.com. Silakan LOGIN manual (username/password, OTP, challenge, dll.).")
    driver.get("https://www.instagram.com/")
    # tunggu kamu login
    # tips: setelah login dan feed tampil, tekan ENTER di terminal
    try:
        input("Tekan ENTER di terminal ini setelah kamu selesai login dan feed sudah tampil...")
    except KeyboardInterrupt:
        driver.quit(); sys.exit(1)

    # ambil cookies penting
    cookies = {}
    for name in ["sessionid", "ds_user_id", "csrftoken"]:
        try:
            c = driver.get_cookie(name)
            if c and c.get("value"):
                cookies[name] = c["value"]
            else:
                cookies[name] = ""
        except Exception:
            cookies[name] = ""

    print("[cek] sessionid:", (cookies.get("sessionid") or "")[:18] + "…" if cookies.get("sessionid") else "(kosong)")
    print("[cek] ds_user_id:", cookies.get("ds_user_id") or "(kosong)")
    print("[cek] csrftoken :", (cookies.get("csrftoken") or "")[:18] + "…" if cookies.get("csrftoken") else "(kosong)")

    driver.quit()

    if not cookies.get("sessionid"):
        print("sessionid kosong. Login gagal atau kamu belum tekan ENTER setelah feed tampil.")
        sys.exit(2)

    # tulis ke .env
    write_env({
        "IG_SESSIONID": cookies["sessionid"],
        "IG_DS_USER_ID": cookies.get("ds_user_id", ""),
        "IG_CSRFTOKEN": cookies.get("csrftoken", ""),
        # opsional: UA default Chrome
        "UA": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari/537.36"
    })

if __name__ == "__main__":
    Path(".cache").mkdir(exist_ok=True)
    main()
