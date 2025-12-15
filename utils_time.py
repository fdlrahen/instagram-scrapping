# utils_time.py
import datetime as _dt
import re

MONTHS = {
    "jan": 1, "januari": 1,
    "feb": 2, "februari": 2,
    "mar": 3, "maret": 3,
    "apr": 4, "april": 4,
    "mei": 5,
    "jun": 6, "juni": 6,
    "jul": 7, "juli": 7,
    "ags": 8, "agustus": 8,
    "sep": 9, "september": 9,
    "okt": 10, "oktober": 10,
    "nov": 11, "november": 11,
    "des": 12, "desember": 12,
}


def extract_event_date(caption: str, taken_at) -> str | None:
    """Parse tanggal event dari caption.

    Mendukung, antara lain:
    - "14 Juli 2025"
    - "14 Juli"
    - "14-16 Juli 2025"
    - "14 Juli - 16 Juli 2025"
    - "30 November - 2 Desember 2025"
    - "30 November s/d 2 Desember 2025"
    - "30/11/2025"
    - "30/11/2025 - 2/12/2025"
    - "30-11-2025 s/d 2-12-2025"
    """

    if not caption:
        return None

    MONTHS_CANON = {
        1: "Januari",
        2: "Februari",
        3: "Maret",
        4: "April",
        5: "Mei",
        6: "Juni",
        7: "Juli",
        8: "Agustus",
        9: "September",
        10: "Oktober",
        11: "November",
        12: "Desember",
    }

    def _norm_year(y_str, default_year: int) -> int:
        """
        Normalisasi tahun:
        - None  → pakai default_year
        - 2025  → 2025
        - 25    → 2025 (anggap 00–50 = 2000–2050)
        """
        if not y_str:
            return default_year
        y_str = str(y_str).strip()
        y = int(y_str)
        if y < 100:
            return 2000 + y if y <= 50 else 1900 + y
        return y

    # Tentukan tahun default dari taken_at (epoch)
    if taken_at:
        try:
            ts = int(taken_at)
            year_default = _dt.datetime.utcfromtimestamp(ts).year
        except Exception:
            year_default = _dt.datetime.utcnow().year
    else:
        year_default = _dt.datetime.utcnow().year

    text = str(caption)

    # Normalisasi kata penghubung range tanggal jadi '-'
    # s/d, s.d., sd, sampai (dengan), hingga → "-"
    connector_rx = re.compile(
        r"\b(s/d|s\.d\.|sd|sampai(?: dengan)?|hingga)\b",
        flags=re.IGNORECASE,
    )
    norm = connector_rx.sub("-", text)

    # 1) Range dua bulan, contoh:
    #    "30 November - 2 Desember 2025"
    m = re.search(
        r"(\d{1,2})\s+([A-Za-z]+)\s*[–-]\s*(\d{1,2})\s+([A-Za-z]+)(?:\s+(\d{2,4}))?",
        norm,
        flags=re.IGNORECASE,
    )
    if m:
        d1, m1, d2, m2, y = m.groups()
        m1_low, m2_low = m1.lower(), m2.lower()
        if m1_low in MONTHS and m2_low in MONTHS:
            year = _norm_year(y, year_default)
            if m1_low == m2_low:
                # Bulan sama → pakai bulan yang sama
                return f"{int(d1)} {m1} - {int(d2)} {m1} {year}"
            else:
                # Bulan beda → "30 November - 2 Desember 2025"
                return f"{int(d1)} {m1} - {int(d2)} {m2} {year}"

    # 2) Range satu bulan, contoh:
    #    "14-16 Juli 2025" atau "14 - 16 Juli"
    m = re.search(
        r"(\d{1,2})\s*[–-]\s*(\d{1,2})\s+([A-Za-z]+)(?:\s+(\d{2,4}))?",
        norm,
        flags=re.IGNORECASE,
    )
    if m:
        d1, d2, mname, y = m.groups()
        m_low = mname.lower()
        if m_low in MONTHS:
            year = _norm_year(y, year_default)
            return f"{int(d1)} {mname} - {int(d2)} {mname} {year}"

    # 3) Range full numeric, contoh:
    #    "30/11/2025 - 2/12/2025"
    #    "30-11-2025 s/d 2-12-2025"
    m = re.search(
        r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\s*[–-]\s*"
        r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})",
        norm,
        flags=re.IGNORECASE,
    )
    if m:
        d1, mo1, y1, d2, mo2, y2 = m.groups()
        d1, mo1, d2, mo2 = int(d1), int(mo1), int(d2), int(mo2)
        if 1 <= mo1 <= 12 and 1 <= mo2 <= 12:
            year1 = _norm_year(y1, year_default)
            year2 = _norm_year(y2, year1)
            name1 = MONTHS_CANON.get(mo1, str(mo1))
            name2 = MONTHS_CANON.get(mo2, str(mo2))
            if mo1 == mo2 and year1 == year2:
                # "30 November - 2 November 2025"
                return f"{d1} {name1} - {d2} {name2} {year1}"
            else:
                # "30 November 2025 - 2 Desember 2025"
                return f"{d1} {name1} {year1} - {d2} {name2} {year2}"

    # 4) Single date dengan nama bulan:
    #    "14 Juli 2025" atau "14 Juli"
    m2 = re.search(
        r"(\d{1,2})\s+([A-Za-z]+)(?:\s+(\d{2,4}))?",
        text,
        flags=re.IGNORECASE,
    )
    if m2:
        d, mname, y = m2.groups()
        m_low = mname.lower()
        if m_low in MONTHS:
            year = _norm_year(y, year_default)
            return f"{int(d)} {mname} {year}"

    # 5) Single date numeric:
    #    "30/11/2025", "30-11-2025"
    m3 = re.search(
        r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})",
        norm,
        flags=re.IGNORECASE,
    )
    if m3:
        d, mo, y = m3.groups()
        d, mo = int(d), int(mo)
        if 1 <= mo <= 12:
            year = _norm_year(y, year_default)
            name = MONTHS_CANON.get(mo, str(mo))
            return f"{d} {name} {year}"

    return None


def to_mysql_datetime(val):
    """
    Terima epoch/int/float/str(ISO) → 'YYYY-mm-dd HH:MM:SS' (UTC).
    Return None kalau gagal.
    """
    try:
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)) or (isinstance(val, str) and val.isdigit()):
            ts = int(val)
            dt = _dt.datetime.fromtimestamp(ts, _dt.timezone.utc)
        elif isinstance(val, str):
            s = val.replace("Z", "")
            dt = _dt.datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_dt.timezone.utc)
            else:
                dt = dt.astimezone(_dt.timezone.utc)
        else:
            return None
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def slug_ts_from_epoch(val):
    """
    Untuk penamaan file JSON: YYYYmmdd_HHMMSS — fallback UTC now.
    """
    try:
        if val is None or val == "":
            raise ValueError
        if isinstance(val, (int, float)) or (isinstance(val, str) and val.isdigit()):
            ts = int(val)
            return _dt.datetime.utcfromtimestamp(ts).strftime("%Y%m%d_%H%M%S")
        # parse ISO-ish string
        return _dt.datetime.fromisoformat(str(val).replace("Z", "")).strftime("%Y%m%d_%H%M%S")
    except Exception:
        return _dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
